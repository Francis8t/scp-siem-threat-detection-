"""
EMR smoke test — Step 10 (M1 de-risk).

Purpose: prove the cluster bootstraps, spark-submit works, EMRFS can read the
Firehose-written JSON in s3://<bucket>/raw/, and a shuffle completes. This is a
de-risk job, not the real batch layer — that arrives in M2.

Usage (as an EMR Step):
    spark-submit s3://<bucket>/scripts/smoke_test.py s3://<bucket>/raw/

Usage (local test):
    spark-submit smoke_test.py ./local_raw/
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    if len(sys.argv) < 2:
        print("ERROR: pass the input path, e.g. s3://my-bucket/raw/")
        sys.exit(1)

    input_path = sys.argv[1]

    spark = (SparkSession.builder
             .appName("scp-siem-smoke-test")
             .getOrCreate())

    print(f"=== reading {input_path} ===")
    # Firehose writes into nested date dirs (raw/yyyy/MM/dd/HH/). Spark does NOT
    # recurse into subdirectories by default -> UNABLE_TO_INFER_SCHEMA. This flag
    # makes it walk the whole tree.
    df = spark.read.option("recursiveFileLookup", "true").json(input_path)

    total = df.count()
    print(f"=== TOTAL RECORDS: {total} ===")

    print("=== SCHEMA (confirms the locked contract survived the round trip) ===")
    df.printSchema()

    # A groupBy forces a shuffle — the operation that actually matters for the
    # M4 speedup benchmark. If this works, the cluster is genuinely usable.
    print("=== TOP FAILED-AUTH SOURCE IPs (exercises a shuffle) ===")
    (df.filter(F.col("status") == "failed")
       .groupBy("source_ip")
       .agg(F.count("*").alias("failed_count"))
       .orderBy(F.desc("failed_count"))
       .show(10, truncate=False))

    print("=== STATUS DISTRIBUTION ===")
    df.groupBy("status").count().orderBy(F.desc("count")).show(truncate=False)

    print("=== SMOKE TEST PASSED ===")
    spark.stop()


if __name__ == "__main__":
    main()