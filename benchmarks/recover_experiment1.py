"""
Experiment 1 recovery / verification (M4).

Rebuilds the Experiment 1 dataset from ground truth in AWS rather than trusting
the runner's local CSV, and — critically — verifies the node count each step
ACTUALLY ran at instead of believing the label in its name.

Why this exists: three copies of run_experiment1.py were accidentally launched
against the same cluster. EMR runs steps one at a time (Concurrent steps = 1),
so no two jobs competed for resources and every measurement is individually
valid. But the runners resized the core group independently, so a step named
"exp1-n1-run2" may have executed while the cluster was actually at 3 nodes, and
each process overwrote the same CSV with only its own rows.

Reconstruction:
  1. list every exp1 step and pull ELAPSED_SECONDS from its stdout in S3
  2. rebuild the core group's size over time from EC2 instance lifetimes
     (ReadyDateTime .. EndDateTime), giving the true node count at any instant
  3. discard any step whose node count changed mid-run, or whose label
     disagrees with reality
  4. discard each sweep's warm-up run (the "-run0" step), per the documented
     Experiment 1 method
  5. group the survivors by ACTUAL node count and report medians

Net effect: 52 steps recovered instead of 16, and roughly 8-9 measured samples
per size instead of 3, with contaminated and warm-up runs excluded on evidence
rather than assumption.

Warm-ups matter here: the n=1 series drifts from ~168 s down to ~142 s across
repeated runs as JVMs start warm and S3 metadata caches fill. Including a cold
first run inflates the median for that tier. Both figures are printed so the
effect is visible rather than hidden.

Usage:
    python3 benchmarks/recover_experiment1.py --cluster-id j-XXXXXXXX
    python3 benchmarks/recover_experiment1.py --cluster-id j-XXXXXXXX --include-warmup
"""

import argparse
import csv
import gzip
import io
import re
import statistics

import boto3

REGION = "us-east-1"
RE_ELAPSED = re.compile(r"ELAPSED_SECONDS:\s*([\d.]+)")
RE_RECORDS = re.compile(r"records aggregated\s*:\s*(\d+)")
EXPECTED_RECORDS = 31_376_600          # 100 replicas x 313,766 (D27)


def core_instance_lifetimes(emr, cluster_id):
    """(ready, end) for every core instance ever provisioned."""
    spans = []
    for page in emr.get_paginator("list_instances").paginate(
            ClusterId=cluster_id, InstanceGroupTypes=["CORE"]):
        for inst in page["Instances"]:
            tl = inst["Status"].get("Timeline", {})
            ready = tl.get("ReadyDateTime") or tl.get("CreationDateTime")
            spans.append((ready, tl.get("EndDateTime")))
    return spans


def nodes_at(spans, when):
    """How many core nodes were actually serving at `when`."""
    return sum(1 for ready, end in spans
               if ready and ready <= when and (end is None or end > when))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cluster-id", required=True)
    ap.add_argument("--raw-out", default="benchmarks/experiment1_raw.csv")
    ap.add_argument("--summary-out", default="benchmarks/experiment1_summary.csv")
    ap.add_argument("--include-warmup", action="store_true",
                    help="keep each sweep's run0 instead of discarding it "
                         "(default: discard, per the documented method)")
    args = ap.parse_args()

    emr = boto3.client("emr", region_name=REGION)
    s3 = boto3.client("s3", region_name=REGION)

    log_uri = emr.describe_cluster(ClusterId=args.cluster_id)["Cluster"]["LogUri"]
    prefix = log_uri.replace("s3n://", "").replace("s3://", "").rstrip("/")
    bucket, _, base = prefix.partition("/")

    spans = core_instance_lifetimes(emr, args.cluster_id)

    steps = []
    for page in emr.get_paginator("list_steps").paginate(ClusterId=args.cluster_id):
        steps.extend(page["Steps"])

    rows = []
    for st in steps:
        m = re.match(r"exp1-n(\d+)-run(\d+)", st["Name"])
        if not m:
            continue
        tl = st["Status"].get("Timeline", {})
        start, end = tl.get("StartDateTime"), tl.get("EndDateTime")
        if not (start and end):
            continue
        try:
            body = s3.get_object(
                Bucket=bucket,
                Key=f"{base}/{args.cluster_id}/steps/{st['Id']}/stdout.gz"
            )["Body"].read()
            out = gzip.GzipFile(fileobj=io.BytesIO(body)).read().decode("utf-8", "replace")
        except Exception:
            continue
        me, mr = RE_ELAPSED.search(out), RE_RECORDS.search(out)
        if not me:
            continue

        at_start, at_end = nodes_at(spans, start), nodes_at(spans, end)
        rows.append({
            "labelled_nodes": int(m.group(1)),
            "run_index": int(m.group(2)),
            # run0 is each sweep's warm-up: cold JVMs and an unpopulated S3
            # metadata cache, so it is systematically slower.
            "warmup": int(m.group(2)) == 0,
            "actual_nodes_start": at_start,
            "actual_nodes_end": at_end,
            "stable": at_start == at_end,
            "label_correct": int(m.group(1)) == at_start,
            "elapsed_seconds": float(me.group(1)),
            "records": int(mr.group(1)) if mr else None,
            "step_id": st["Id"],
            "started": start.isoformat(),
        })

    rows.sort(key=lambda r: r["started"])
    with open(args.raw_out, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)

    bad_records = [r for r in rows if r["records"] != EXPECTED_RECORDS]
    contaminated = [r for r in rows if not r["stable"] or not r["label_correct"]]
    valid = [r for r in rows if r["stable"] and r["label_correct"]]
    warmups = [r for r in valid if r["warmup"]]
    usable = valid if args.include_warmup else [r for r in valid if not r["warmup"]]

    print(f"steps recovered       : {len(rows)}")
    print(f"record-count anomalies: {len(bad_records)} (expect 0)")
    print(f"excluded, contaminated: {len(contaminated)} (resized mid-run or mislabelled)")
    print(f"excluded, warm-up     : {0 if args.include_warmup else len(warmups)}"
          f"{' (KEPT via --include-warmup)' if args.include_warmup else ''}")
    print(f"usable measurements   : {len(usable)}\n")

    def summarise(records):
        groups = {}
        for r in records:
            groups.setdefault(r["actual_nodes_start"], []).append(r["elapsed_seconds"])
        out, baseline = [], None
        for n in sorted(groups):
            vals = sorted(groups[n])
            med = statistics.median(vals)
            if baseline is None:
                baseline = med
            out.append((n, len(vals), med, vals[0], vals[-1],
                        baseline / med, baseline / med / n))
        return out

    summary = summarise(usable)
    print(f"{'nodes':>6} {'runs':>5} {'median':>9} {'min':>8} {'max':>8} "
          f"{'speedup':>8} {'efficiency':>11}")
    print("-" * 60)
    for n, cnt, med, lo, hi, sp, eff in summary:
        print(f"{n:>6} {cnt:>5} {med:>8.1f}s {lo:>7.1f}s {hi:>7.1f}s "
              f"{sp:>7.2f}x {eff:>10.1%}")

    # Show what the warm-ups were doing, so the exclusion is transparent
    # rather than a number the reader has to take on trust.
    if not args.include_warmup and warmups:
        with_warm = {n: med for n, _, med, *_ in summarise(valid)}
        print("\n  effect of discarding warm-ups (median seconds):")
        for n, _, med, *_ in summary:
            delta = with_warm[n] - med
            print(f"    n={n}: {with_warm[n]:.1f}s including -> {med:.1f}s excluding "
                  f"({delta:+.1f}s)")

    with open(args.summary_out, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["nodes", "runs", "median_seconds", "min_seconds",
                    "max_seconds", "speedup", "efficiency"])
        for n, cnt, med, lo, hi, sp, eff in summary:
            w.writerow([n, cnt, round(med, 2), round(lo, 2), round(hi, 2),
                        round(sp, 3), round(eff, 3)])


if __name__ == "__main__":
    main()
