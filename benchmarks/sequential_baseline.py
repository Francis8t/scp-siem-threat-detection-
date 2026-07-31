"""
Experiment 1 sequential baseline (M4).

Single process, single thread, no framework: the honest "what if we hadn't
distributed this at all?" number that the Spark speedup curve is measured
against.

Computes exactly the aggregates batch/spark_batch.py produces, over exactly the
same input, and prints its timing in the same format so the same tooling parses
both.

FAIRNESS — why this reads from S3 rather than local disk
--------------------------------------------------------
Spark's ELAPSED_SECONDS covers reading from S3, shuffling and aggregating. If
this baseline read from local disk it would be comparing network-bound work
against disk-bound work and the comparison would be meaningless. So it streams
the same S3 objects, one at a time, in one thread. Expect a good share of the
runtime to be transfer rather than CPU — that is a real property of the
sequential approach, not a measurement artefact.

Run it as an EMR step so it executes on the primary node: same instance type
and same network path to S3 as the Spark executors. Running it on a laptop
would measure your broadband, not the algorithm.

No parsing needed: records in benchmark-input/ were already parsed by the
producer (D12), so this reads the locked JSON schema directly — the same thing
Spark does.

Usage (EMR step, command-runner.jar):
    bash -c "aws s3 cp s3://<bucket>/scripts/sequential_baseline.py . && \
             python3 sequential_baseline.py --input s3://<bucket>/benchmark-input/"

Local smoke test against a few objects:
    python3 sequential_baseline.py --input s3://<bucket>/benchmark-input/ --max-objects 3
"""

import argparse
import json
import time
from collections import defaultdict

import boto3

STATUSES = ("failed", "accepted", "invalid_user", "other")


def iter_objects(s3, bucket, prefix, max_objects=0):
    """Yield object keys under prefix, skipping directory markers."""
    n = 0
    for page in s3.get_paginator("list_objects_v2").paginate(
            Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith("/") or key.rsplit("/", 1)[-1].startswith("_"):
                continue
            yield key
            n += 1
            if max_objects and n >= max_objects:
                return


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="s3://bucket/prefix/")
    ap.add_argument("--region", default="us-east-1")
    ap.add_argument("--max-objects", type=int, default=0,
                    help="cap objects read (smoke testing only; 0 = all)")
    ap.add_argument("--top", type=int, default=10)
    args = ap.parse_args()

    path = args.input.replace("s3://", "").rstrip("/") + "/"
    bucket, _, prefix = path.partition("/")
    s3 = boto3.client("s3", region_name=args.region)

    per_ip = defaultdict(lambda: {s: 0 for s in STATUSES})
    totals = {s: 0 for s in STATUSES}
    records = 0
    objects = 0

    started = time.perf_counter()
    for key in iter_objects(s3, bucket, prefix, args.max_objects):
        objects += 1
        body = s3.get_object(Bucket=bucket, Key=key)["Body"]
        # iter_lines streams, so a 44 MB object never lands in memory whole.
        for raw in body.iter_lines():
            if not raw:
                continue                       # the \n delimiter (D13)
            rec = json.loads(raw)
            status = rec["status"]
            if status not in totals:
                raise ValueError(f"unexpected status {status!r}")
            per_ip[rec["source_ip"]][status] += 1
            totals[status] += 1
            records += 1
    elapsed = time.perf_counter() - started

    # Same output shape as spark_batch.py so one parser reads both.
    print(f"=== objects read        : {objects} ===")
    print(f"=== distinct source IPs : {len(per_ip)} ===")
    print(f"=== records aggregated  : {records} ===")
    print("=== STATUS DISTRIBUTION ===")
    for s in STATUSES:
        print(f"  {s:<13}: {totals[s]}")

    print(f"=== TOP {args.top} OFFENDERS BY FAILED-AUTH COUNT ===")
    top = sorted(per_ip.items(), key=lambda kv: (-kv[1]["failed"], kv[0]))[:args.top]
    for ip, counts in top:
        print(f"  {counts['failed']:9d}   {ip}")

    print(f"=== ELAPSED_SECONDS: {elapsed:.3f} ===")


if __name__ == "__main__":
    main()
