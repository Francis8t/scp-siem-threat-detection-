-- Serving layer — Athena DDL over the Spark batch view (M3).
--
-- The batch view is parquet written by batch/spark_batch.py to
-- s3://scp-siem-data-009910375264/batch-views/full/ (D20).
-- Athena reads the whole prefix; the _SUCCESS marker is ignored because
-- Hive-style readers skip files whose names begin with _ or .
--
-- Per D25 this table does the batch-side heavy lifting: rank offenders and
-- threshold-filter down to a small result set. The dashboard then joins that
-- against live DynamoDB alert rows on source_ip.

CREATE DATABASE IF NOT EXISTS scp_siem;

CREATE EXTERNAL TABLE IF NOT EXISTS scp_siem.batch_view (
  `source_ip`    string,
  `failed`       bigint,
  `accepted`     bigint,
  `invalid_user` bigint,
  `other`        bigint,
  `total`        bigint,
  `first_seen`   string,
  `last_seen`    string
)
STORED AS PARQUET
LOCATION 's3://scp-siem-data-009910375264/batch-views/full/';
