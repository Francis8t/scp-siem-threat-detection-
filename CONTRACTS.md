# Shared Contracts (LOCKED at M0)

Everything downstream builds against these. Change only by team agreement.

## 1. Record schema (Kinesis payload)

The producer parses each OpenSSH log line into this JSON and puts it to Kinesis
(indented block = the record):

    {
      "producer_ts": "2026-07-24T14:03:22.481Z",
      "source_ip":   "52.80.34.196",
      "status":      "failed",
      "user":        "invalid user test",
      "port":        36034,
      "host":        "LabSZ"
    }

- `producer_ts` — ISO-8601 UTC, millisecond precision, set at EMIT time. Drives the
  end-to-end latency benchmark AND is the clock we window on.
- `status` — enum: `failed` | `accepted` | `invalid_user` | `other`.
- Kinesis partition key = `source_ip` (locked).

### Two locked decisions worth understanding
- **Partition key = source_ip.** All of one IP's events land on the same shard, so
  per-IP window counting never needs cross-shard coordination.
- **Window on processing time (`producer_ts`), NOT the log's own timestamp.** The raw
  lines carry Dec-2018 timestamps with no year; replay/arrival time keeps the
  sliding-window logic clean. Stated explicitly in the report.

## 2. S3 key layout

Bucket: scp-siem-data-<ACCOUNT_ID>  (region us-east-1)

    raw/yyyy/mm/dd/hh/          <- Firehose landing, immutable master dataset (UTC)
    batch-views/                <- Spark batch aggregates (parquet/csv), partitioned
    athena-results/             <- Athena query output location

`raw/` is the append-only source of truth for the batch layer. Batch jobs read `raw/`,
never mutate it, and write only under `batch-views/`.

## 3. DynamoDB speed-view state table

Table: scp-siem-speed-state  (billing: PAY_PER_REQUEST / on-demand)

- Partition key: `source_ip` (String)
- Sort key:      `bucket` (Number) — 30-second bucket start, epoch seconds floored to 30s
- Attributes:    `count` (Number) — failed-auth count in that bucket
                 `ttl`   (Number) — epoch seconds = bucket + 360; item auto-expires ~6 min later
- TTL: enabled on the `ttl` attribute.

Windowing scheme (bucketed, recommended): ten 30-second buckets = 5-minute window.
Speed layer on each failed event: floor `producer_ts` to 30s, atomically ADD to the
(ip, bucket) count, then query the trailing 10 buckets for that IP and sum = current
5-min count. If sum >= threshold, emit an alert.

Alerts table / exact threshold: finalised at M2.
