#!/usr/bin/env python3
"""
Hadoop Streaming mapper — batch layer MapReduce variant (M2).

Reads the Firehose-landed JSON records from raw/ on stdin and emits one
tab-separated (source_ip, status) pair per record. All the aggregation happens
in the reducer; the mapper's only job is to project the two fields the shuffle
needs and let Hadoop sort by key.

This is the MapReduce implementation of the same aggregation as
batch/spark_batch.py, and it exists for the Experiment 1 sequential-vs-parallel
comparison (PROJECT_PLAN.md §7): Hadoop Streaming on a single node is the
non-Spark baseline the parallel Spark runs are measured against.

No parsing happens here. The producer already parsed these records (D12); the
mapper just reads the locked schema.

Local test without Hadoop (this is the point of the design — you can validate
the whole job on a laptop):

    cat records.ndjson | ./mapper.py | sort | ./reducer.py

On EMR:

    hadoop-streaming -files mapper.py,reducer.py \
        -mapper mapper.py -reducer reducer.py \
        -input  s3://<bucket>/raw/2026/07/29/ \
        -output s3://<bucket>/batch-views/mr/
"""

import json
import sys


def main():
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue  # the \n delimiter the producer appends (D13)
        try:
            rec = json.loads(line)
        except json.JSONDecodeError:
            # A malformed line must not kill the task — Hadoop would retry the
            # split and eventually fail the whole job over one bad record.
            # Surface it as a job counter instead so it is visible but survivable.
            print("reporter:counter:scp-siem,skipped_bad_json,1", file=sys.stderr)
            continue

        source_ip = rec.get("source_ip")
        status = rec.get("status")
        if not source_ip or not status:
            print("reporter:counter:scp-siem,skipped_missing_field,1", file=sys.stderr)
            continue

        # Key = source_ip so Hadoop's sort groups every event for one IP into a
        # single contiguous run at the reducer.
        print(f"{source_ip}\t{status}")


if __name__ == "__main__":
    main()
