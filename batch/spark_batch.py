"""
Batch layer — PySpark job (M2).

Computes the authoritative full-history view the serving layer merges against:
per-source-IP counts by status, plus first/last seen. Writes parquet (D20) to
s3://<bucket>/batch-views/.

This must agree EXACTLY with batch/reference_counts.py. That script is the
oracle; this one is the distributed implementation of the same aggregation.
Diffing the two is what proves the Spark logic is right (PROJECT_PLAN.md M2).

It does NOT parse anything. Records in raw/ were already parsed by the producer,
which is the single parsing boundary (D12) — Spark just reads the locked JSON
schema. Nothing to ship with --py-files.

INPUT PATH MATTERS. raw/ also holds ~2,300 records from the M1/M2 smoke tests
(23 and 28 Jul). Pointing at raw/ therefore yields 316,066 records, not the
313,766 of the 29 Jul full replay, and the per-IP counts are contaminated
because the 2k fixture reuses the same offender IPs. For a clean reconciliation
pass the replay partition:

    s3://scp-siem-data-009910375264/raw/2026/07/29/

Usage (EMR Step, Deploy mode = Client, per D16):
    spark-submit s3://<bucket>/scripts/spark_batch.py \
        s3://<bucket>/raw/2026/07/29/ \
        s3://<bucket>/batch-views/full/ \
        --csv-out s3://<bucket>/batch-views/full-csv/
"""

import argparse
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# The locked record schema (CONTRACTS.md §1). Declared explicitly rather than
# inferred: inference costs an extra full pass over the input, which would both
# slow the job and pollute the Experiment 1 timings. It also pins `port` to a
# long instead of letting Spark guess per-file, which is how a null-heavy column
# ends up typed differently across partitions.
SCHEMA = T.StructType([
    T.StructField("producer_ts", T.StringType(), True),
    T.StructField("source_ip", T.StringType(), True),
    T.StructField("status", T.StringType(), True),
    T.StructField("user", T.StringType(), True),
    T.StructField("port", T.LongType(), True),
    T.StructField("host", T.StringType(), True),
])

STATUSES = ("failed", "accepted", "invalid_user", "other")


def status_count(status):
    """Conditional sum for one status value.

    Preferred over .pivot("status") because it guarantees all four columns
    exist even when a status is absent from the input — a pivot would silently
    emit a narrower table and break the diff against the reference CSV.
    """
    return F.sum(F.when(F.col("status") == status, 1).otherwise(0)).alias(status)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("input", help="s3://<bucket>/raw/<partition>/ (see module docstring)")
    ap.add_argument("output", help="s3://<bucket>/batch-views/<name>/ — parquet destination")
    ap.add_argument("--csv-out", default=None,
                    help="optional CSV copy for diffing against reference_counts.py")
    ap.add_argument("--top", type=int, default=10)
    args = ap.parse_args()

    spark = SparkSession.builder.appName("scp-siem-batch-view").getOrCreate()

    # recursiveFileLookup: Firehose nests objects under yyyy/MM/dd/HH/ and Spark
    # will not descend by default -> UNABLE_TO_INFER_SCHEMA (D15).
    df = (spark.read
          .option("recursiveFileLookup", "true")
          .schema(SCHEMA)
          .json(args.input))

    agg = (df.groupBy("source_ip")
             .agg(*[status_count(s) for s in STATUSES],
                  F.count("*").alias("total"),
                  F.min("producer_ts").alias("first_seen"),
                  F.max("producer_ts").alias("last_seen"))
             # Tie-break on source_ip: 139.219.191.138 and 183.62.140.253 are
             # tied on failed count, so ordering by count alone is arbitrary and
             # a row-by-row diff reports a false mismatch (CONTRACTS.md §3).
             .orderBy(F.desc("failed"), F.asc("source_ip")))

    agg = agg.cache()

    # Timed driver-side, excluding cluster bootstrap, per the Experiment 1
    # method. collect() forces the read + shuffle + aggregation; the writes
    # below reuse the cache so they are not counted twice.
    started = time.perf_counter()
    rows = agg.collect()
    elapsed = time.perf_counter() - started

    total_records = sum(r["total"] for r in rows)
    print(f"=== distinct source IPs : {len(rows)} ===")
    print(f"=== records aggregated  : {total_records} ===")

    print("=== STATUS DISTRIBUTION ===")
    for s in STATUSES:
        print(f"  {s:<13}: {sum(r[s] for r in rows)}")

    print(f"=== TOP {args.top} OFFENDERS BY FAILED-AUTH COUNT ===")
    for r in rows[:args.top]:
        print(f"  {r['failed']:7d}   {r['source_ip']}")
        
    agg.coalesce(1).write.mode("overwrite").parquet(args.output)
    print(f"=== batch view (parquet) -> {args.output} ===")

    if args.csv_out:
        # coalesce(1) so the result is a single readable file rather than one
        # part-* per partition. Safe here: the view is ~1,238 rows.
        (agg.coalesce(1).write.mode("overwrite")
            .option("header", "true").csv(args.csv_out))
        print(f"=== batch view (csv copy) -> {args.csv_out} ===")


    # agg.write.mode("overwrite").parquet(args.output)
    # print(f"=== batch view (parquet) -> {args.output} ===")

    # if args.csv_out:
    #     # coalesce(1) so the result is a single readable file rather than one
    #     # part-* per partition. Safe here: the view is ~1,238 rows.
    #     # (agg.coalesce(1).write.mode("overwrite")
    #     agg.coalesce(1).write.mode("overwrite").parquet(args.output)
    #     print(f"=== batch view (csv copy) -x    > {args.csv_out} ===")

    print(f"=== ELAPSED_SECONDS: {elapsed:.3f} ===")
    spark.stop()


if __name__ == "__main__":
    main()
