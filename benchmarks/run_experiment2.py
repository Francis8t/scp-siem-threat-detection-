"""
Experiment 2 driver — speed-layer latency under load (M4).

Ramps the producer through a series of target rates, holds each at steady
state, then pulls the CloudWatch metrics and Lambda log fields for each tier
into a CSV for the latency-vs-rate plots.

Method: for every rate it starts producer.py --loop, lets it run for --hold
seconds, stops it, then waits --cooldown so the stream drains and one tier's
metrics do not bleed into the next. All metric queries happen at the END, once,
after a settling delay — CloudWatch publishes on a delay and querying per tier
would mean waiting repeatedly for no benefit.

WHAT THE NUMBERS MEAN

  iterator_age_max_ms   how far the CONSUMER trails the head of the shard.
                        Rises when the Lambda cannot keep up.
  batch_lag_*_ms        how OLD each record was when handled, from producer_ts
                        (the D26 instrumentation). Per batch, not per record.
  incoming_records      what Kinesis actually accepted. Below the target rate
                        at the high tiers, because one shard caps at ~1,000
                        rec/s — that gap IS the saturation knee.
  throttles             Lambda throttles. Expected to stay 0: one shard allows
                        little concurrency, so the consumer is serialised long
                        before Lambda's own limits are reached.

Context from D26: before the batching optimisation this layer saturated at
~90 rec/s with 138 s of iterator age. After it, 1,000 rec/s ran at ~90 ms per
batch with zero lag. This sweep finds where the SHARD, rather than the
consumer, becomes the constraint.

Usage:
    python3 benchmarks/run_experiment2.py --dry-run
    python3 benchmarks/run_experiment2.py
    python3 benchmarks/run_experiment2.py --rates 100,500,1000 --hold 120
"""

import argparse
import csv
import signal
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone

import boto3

REGION = "us-east-1"
STREAM = "scp-siem-stream"
FUNCTION = "scp-siem-speed"
LOG_GROUP = f"/aws/lambda/{FUNCTION}"


def cw_stat(cw, namespace, metric, dimensions, start, end, stat):
    """One CloudWatch statistic over a window, or None if nothing published."""
    resp = cw.get_metric_statistics(
        Namespace=namespace, MetricName=metric,
        Dimensions=[{"Name": k, "Value": v} for k, v in dimensions.items()],
        StartTime=start, EndTime=end, Period=60, Statistics=[stat])
    points = resp.get("Datapoints", [])
    if not points:
        return None
    values = [p[stat] for p in points]
    # Sum accumulates across the window; the rest describe it, so take the peak.
    return round(sum(values), 1) if stat == "Sum" else round(max(values), 1)


def logs_lag(logs, start, end, timeout=120):
    """Batch-latency percentiles from the handler's own summary lines."""
    query = ("fields lag_ms_p50, lag_ms_p95, lag_ms_max "
             "| filter ispresent(lag_ms_p50) "
             "| stats pct(lag_ms_p50, 50) as p50, pct(lag_ms_p95, 95) as p95, "
             "max(lag_ms_max) as maxlag")
    try:
        qid = logs.start_query(logGroupName=LOG_GROUP,
                               startTime=int(start.timestamp()),
                               endTime=int(end.timestamp()),
                               queryString=query)["queryId"]
    except logs.exceptions.ResourceNotFoundException:
        return {}
    deadline = time.time() + timeout
    while time.time() < deadline:
        r = logs.get_query_results(queryId=qid)
        if r["status"] == "Complete":
            if not r["results"]:
                return {}
            return {f["field"]: round(float(f["value"]), 1)
                    for f in r["results"][0] if f["field"] in ("p50", "p95", "maxlag")}
        if r["status"] in ("Failed", "Cancelled"):
            return {}
        time.sleep(3)
    return {}


def run_tier(rate, hold, log_file, producer_args):
    """Run the producer at `rate` for `hold` seconds. Returns (start, end)."""
    cmd = [sys.executable, "producer/producer.py", "--rate", str(rate), "--loop"] + producer_args
    print(f"  starting producer at {rate} rec/s for {hold}s ...", flush=True)
    started = datetime.now(timezone.utc)
    with open(log_file, "a") as out:
        proc = subprocess.Popen(cmd, stdout=out, stderr=subprocess.STDOUT)
        try:
            time.sleep(hold)
        finally:
            # SIGINT rather than terminate(): the producer traps KeyboardInterrupt
            # and flushes its final partial batch before exiting.
            proc.send_signal(signal.SIGINT)
            try:
                proc.wait(timeout=30)
            except subprocess.TimeoutExpired:
                proc.kill()
    ended = datetime.now(timezone.utc)
    print(f"  stopped after {(ended - started).total_seconds():.0f}s", flush=True)
    return started, ended


def collect(cw, logs, rate, start, end, hold):
    kin = {"StreamName": STREAM}
    fn = {"FunctionName": FUNCTION}
    incoming = cw_stat(cw, "AWS/Kinesis", "IncomingRecords", kin, start, end, "Sum")
    row = {
        "target_rate": rate,
        "hold_seconds": hold,
        "incoming_records": incoming,
        "achieved_rate": round(incoming / hold, 1) if incoming else None,
        "iterator_age_max_ms": cw_stat(cw, "AWS/Kinesis",
                                       "GetRecords.IteratorAgeMilliseconds",
                                       kin, start, end, "Maximum"),
        "lambda_duration_avg_ms": cw_stat(cw, "AWS/Lambda", "Duration", fn,
                                          start, end, "Average"),
        "lambda_duration_max_ms": cw_stat(cw, "AWS/Lambda", "Duration", fn,
                                          start, end, "Maximum"),
        "concurrent_max": cw_stat(cw, "AWS/Lambda", "ConcurrentExecutions", fn,
                                  start, end, "Maximum"),
        "invocations": cw_stat(cw, "AWS/Lambda", "Invocations", fn, start, end, "Sum"),
        "throttles": cw_stat(cw, "AWS/Lambda", "Throttles", fn, start, end, "Sum"),
        "errors": cw_stat(cw, "AWS/Lambda", "Errors", fn, start, end, "Sum"),
    }
    lag = logs_lag(logs, start, end)
    row["batch_lag_p50_ms"] = lag.get("p50")
    row["batch_lag_p95_ms"] = lag.get("p95")
    row["batch_lag_max_ms"] = lag.get("maxlag")
    return row


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rates", default="100,500,1000,2500,5000")
    ap.add_argument("--hold", type=int, default=180,
                    help="seconds at steady state per rate (default 180)")
    ap.add_argument("--cooldown", type=int, default=60,
                    help="seconds between tiers so metrics separate cleanly")
    ap.add_argument("--settle", type=int, default=180,
                    help="seconds to wait after the last tier before querying "
                         "CloudWatch (it publishes on a delay)")
    ap.add_argument("--file", default="SSH.log", help="producer input file")
    ap.add_argument("--batch-size", type=int, default=500,
                    help="producer put_records batch size; 500 is the Kinesis cap "
                         "and is needed to reach the high tiers")
    ap.add_argument("--out", default="benchmarks/experiment2_raw.csv")
    ap.add_argument("--producer-log", default="benchmarks/experiment2_producer.log")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    rates = [int(r) for r in args.rates.split(",")]
    total = len(rates) * (args.hold + args.cooldown) + args.settle
    print(f"Experiment 2 — {len(rates)} rates x {args.hold}s hold "
          f"(+{args.cooldown}s cooldown), then {args.settle}s settle")
    print(f"  rates      : {rates}")
    print(f"  estimated  : ~{total / 60:.0f} minutes")
    if args.dry_run:
        print("\n--dry-run: producer not started.")
        return

    cw = boto3.client("cloudwatch", region_name=REGION)
    logs = boto3.client("logs", region_name=REGION)
    producer_args = ["--file", args.file, "--batch-size", str(args.batch_size)]

    windows = []
    for rate in rates:
        print(f"\n=== {rate} rec/s ===", flush=True)
        start, end = run_tier(rate, args.hold, args.producer_log, producer_args)
        windows.append((rate, start, end))
        if rate != rates[-1]:
            print(f"  cooldown {args.cooldown}s ...", flush=True)
            time.sleep(args.cooldown)

    print(f"\nwaiting {args.settle}s for CloudWatch to publish ...", flush=True)
    time.sleep(args.settle)

    rows = []
    for rate, start, end in windows:
        print(f"  collecting {rate} rec/s ...", flush=True)
        # Widen slightly: CloudWatch buckets to minute boundaries.
        rows.append(collect(cw, logs, rate,
                            start - timedelta(seconds=30),
                            end + timedelta(seconds=90), args.hold))

    with open(args.out, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)

    print(f"\nraw -> {args.out}\n")
    print(f"{'target':>7} {'achieved':>9} {'iter age':>10} {'dur avg':>9} "
          f"{'conc':>5} {'throt':>6} {'lag p50':>9} {'lag max':>9}")
    print("-" * 72)
    for r in rows:
        def s(v, suffix=""):
            return f"{v}{suffix}" if v is not None else "-"
        print(f"{r['target_rate']:>7} {s(r['achieved_rate']):>9} "
              f"{s(r['iterator_age_max_ms']):>10} {s(r['lambda_duration_avg_ms']):>9} "
              f"{s(r['concurrent_max']):>5} {s(r['throttles']):>6} "
              f"{s(r['batch_lag_p50_ms']):>9} {s(r['batch_lag_max_ms']):>9}")
    print("\nachieved_rate falling below target = the single shard saturating "
          "(~1,000 rec/s).")


if __name__ == "__main__":
    main()
