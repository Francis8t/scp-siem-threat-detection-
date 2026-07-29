"""
Batch-layer reference implementation — M2.

A single-process, plain-Python computation of exactly the aggregates the PySpark
job produces. It exists for two reasons, and it is not throwaway code:

  1. CORRECTNESS ORACLE. It reconciles to the ground truth in CONTRACTS.md §3
     (and PROJECT_PLAN.md §5.4) on a machine with no Spark, so "is the logic
     right?" is settled before EMR config is ever in play. The CSV it writes is
     the expected-output file the Spark batch view gets diffed against.

  2. EXPERIMENT 1 SEQUENTIAL BASELINE. This is the T(1) single-process number
     the speedup curve S(N) = T(1)/T(N) is measured against (PROJECT_PLAN.md §7).
     That is why it reports its own wall-clock time and why the aggregation is
     deliberately straightforward — it should represent honest single-threaded
     work, not a tuned strawman.

Reuses producer.parse_line() rather than reimplementing it (D12). A divergent
parser would let the batch and speed layers silently disagree about the same
event, which is the worst class of bug on this project.

Two input formats, because the reference has to be comparable to both sides:

  --format log   raw OpenSSH lines (e.g. SSH.log), parsed with parse_line()
  --format json  newline-delimited JSON records as Firehose lands them in
                 s3://<bucket>/raw/ — i.e. a local copy of what Spark reads

Directories are walked recursively, which is the local equivalent of Spark's
recursiveFileLookup=true (D15) over raw/yyyy/MM/dd/HH/.

Usage:
    # reconcile against the full-log ground truth
    python3 batch/reference_counts.py --input SSH.log --out batch/expected_full.csv

    # same aggregates over a local copy of the Firehose landing zone
    python3 batch/reference_counts.py --input ./raw_download/ --format json \
        --out batch/expected_raw.csv

    # timed baseline run for Experiment 1 (no CSV written)
    python3 batch/reference_counts.py --input SSH.log --no-out
"""

import argparse
import csv
import gzip
import json
import os
import sys
import time
from collections import defaultdict

# D12: import the ONE canonical parser rather than rewriting it.
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "producer"))
import producer  # noqa: E402

STATUSES = ("failed", "accepted", "invalid_user", "other")


def open_maybe_gzip(path):
    """Firehose can be configured to gzip; handle both without caring which."""
    if path.endswith(".gz"):
        return gzip.open(path, "rt", encoding="utf-8", errors="replace")
    return open(path, "r", encoding="utf-8", errors="replace")


def iter_files(path):
    """Yield every file under path. A directory is walked recursively.

    This is the local stand-in for Spark's recursiveFileLookup=true (D15):
    Firehose nests objects under raw/yyyy/MM/dd/HH/, so a non-recursive walk
    finds nothing at the top level.
    """
    if os.path.isfile(path):
        yield path
        return
    for root, _dirs, files in os.walk(path):
        for name in sorted(files):
            # Skip the directory-marker objects the S3 console can create.
            if name.startswith(".") or name.endswith("_$folder$"):
                continue
            yield os.path.join(root, name)


def iter_records(path, fmt):
    """Yield parsed records from a file or directory tree, in either format."""
    for file_path in iter_files(path):
        with open_maybe_gzip(file_path) as fh:
            for lineno, line in enumerate(fh, start=1):
                if fmt == "log":
                    rec = producer.parse_line(line)
                    if rec is not None:
                        yield rec
                else:
                    line = line.strip()
                    if not line:
                        continue  # trailing newline from the \n delimiter (D13)
                    try:
                        yield json.loads(line)
                    except json.JSONDecodeError as e:
                        # Bare "Expecting value: line 1 column 1" is useless when
                        # the input is thousands of objects across nested dirs.
                        raise SystemExit(
                            f"{file_path}:{lineno}: not valid JSON ({e}).\n"
                            f"  line: {line[:120]!r}\n"
                            f"  If this is a producer --dry-run capture, drop its "
                            f"trailing '... records' summary line.\n"
                            f"  If it is real Firehose data, check the \\n "
                            f"delimiter (D13) survived."
                        ) from None


def aggregate(path, fmt):
    """Per-IP status counts plus first/last seen. Returns (per_ip, totals, n)."""
    per_ip = defaultdict(lambda: {s: 0 for s in STATUSES})
    first_seen = {}
    last_seen = {}
    totals = {s: 0 for s in STATUSES}
    n = 0

    for rec in iter_records(path, fmt):
        ip = rec["source_ip"]
        status = rec["status"]
        n += 1

        # An unknown status would silently vanish from the per-IP columns while
        # still inflating the total, so fail loudly instead (CONTRACTS.md §1).
        if status not in totals:
            raise ValueError(f"record has status {status!r}, not in {STATUSES}")

        per_ip[ip][status] += 1
        totals[status] += 1

        ts = rec["producer_ts"]
        if ip not in first_seen or ts < first_seen[ip]:
            first_seen[ip] = ts
        if ip not in last_seen or ts > last_seen[ip]:
            last_seen[ip] = ts

    return per_ip, totals, first_seen, last_seen, n


def write_csv(out_path, per_ip, first_seen, last_seen):
    """Write the batch view. Row order is deterministic so diffs are clean."""
    os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
    rows = sorted(
        per_ip.items(),
        key=lambda kv: (-kv[1]["failed"], kv[0]),  # failed desc, then IP asc
    )
    with open(out_path, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["source_ip", "failed", "accepted", "invalid_user", "other",
                    "total", "first_seen", "last_seen"])
        for ip, counts in rows:
            w.writerow([
                ip,
                counts["failed"], counts["accepted"],
                counts["invalid_user"], counts["other"],
                sum(counts.values()),
                first_seen[ip], last_seen[ip],
            ])
    return len(rows)


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--input", required=True,
                    help="raw log file, or a file/directory of JSON records")
    ap.add_argument("--format", choices=("log", "json"), default="log",
                    help="'log' = raw OpenSSH lines (default); 'json' = Firehose records")
    ap.add_argument("--out", default="batch/expected_counts.csv",
                    help="CSV batch view to write (default batch/expected_counts.csv)")
    ap.add_argument("--no-out", action="store_true",
                    help="skip the CSV write; time the aggregation only")
    ap.add_argument("--top", type=int, default=10,
                    help="how many top offenders to print (default 10)")
    args = ap.parse_args()

    started = time.perf_counter()
    per_ip, totals, first_seen, last_seen, n = aggregate(args.input, args.format)
    elapsed = time.perf_counter() - started

    print(f"input            : {args.input}  (format={args.format})")
    print(f"records parsed   : {n}")
    print(f"distinct IPs     : {len(per_ip)}")
    print("\nstatus distribution:")
    for s in STATUSES:
        print(f"  {s:<13}: {totals[s]}")

    print(f"\ntop {args.top} offenders by failed-auth count:")
    top = sorted(per_ip.items(), key=lambda kv: (-kv[1]["failed"], kv[0]))[:args.top]
    for ip, counts in top:
        print(f"  {counts['failed']:7d}   {ip}")

    if not args.no_out:
        written = write_csv(args.out, per_ip, first_seen, last_seen)
        print(f"\nbatch view -> {args.out}  ({written} rows)")

    # Reported separately from any I/O so the Experiment 1 baseline measures the
    # aggregation itself, comparable to Spark timed driver-side.
    print(f"\nELAPSED_SECONDS  : {elapsed:.3f}")

    if args.format == "log":
        print("\nNOTE: parse_line() stamps producer_ts at parse time, so on --format log "
              "the\n      first_seen/last_seen columns describe THIS run, not the data. "
              "They are\n      only meaningful on --format json, where producer_ts came from the "
              "actual\n      replay. Exclude those two columns when diffing a log-format run.")


if __name__ == "__main__":
    main()
