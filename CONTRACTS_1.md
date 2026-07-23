# Shared Contracts (LOCKED at M0)

Everything downstream builds against these. Producer, speed layer, batch layer, and serving all
agree on these exact field names. Change only by team agreement — and when one changes, update
`report/architecture.svg` and `TEAMMATE_ONBOARDING.md` in the same commit.

**Region: `us-east-1`** · **Last updated:** 23 Jul 2026 (post-M1)

---

## 1. Record schema (Kinesis payload)

The producer parses each OpenSSH log line into this JSON and puts it to Kinesis
(indented block = the record):

    {
      "producer_ts": "2026-07-24T14:03:22.481Z",
      "source_ip":   "52.80.34.196",
      "status":      "failed",
      "user":        "test",
      "port":        36034,
      "host":        "LabSZ"
    }

### Field semantics

- `producer_ts` — ISO-8601 UTC, millisecond precision, set at **emit** time (not parsed from the
  log line). Drives the end-to-end latency benchmark AND is the clock we window on.
- `source_ip` — dotted-quad IPv4 extracted from the line. **Never null**: a line with no
  extractable IP is dropped by the producer rather than emitted.
- `status` — enum, exactly one of: `failed` | `accepted` | `invalid_user` | `other`.
- `user` — the **username only**, with no `"invalid user"` prefix. Invalid-ness is carried by
  `status`, not duplicated into this field. One fact per field.
- `port` — integer, or null.
- `host` — the syslog host field (e.g. `LabSZ`).

### Null handling (important — read before writing a consumer)

`user` and `port` are **null** whenever the line carries no username/port. This is the common
case, not an edge case: roughly **1,100 of the 1,999** fixture lines parse as `status: "other"`
with both fields null (disconnects, connection-closed, reverse-mapping lines). Any consumer that
assumes those fields are present will throw on the majority of records. **Tolerate nulls on
`user` and `port` everywhere.**

### Kinesis partition key

**Partition key = `source_ip`** (locked). All of one IP's events land on the same shard, so per-IP
window counting never needs cross-shard coordination. Natural fit for the access pattern.

---

## 2. Two decisions worth understanding before you touch the code

**Window on processing time (`producer_ts`), NOT the log's own timestamp.** The raw lines carry
Dec-2018 timestamps with no year field. Windowing on replay/arrival time keeps the sliding-window
logic clean and avoids year-inference hacks. Stated explicitly in the report.

**The fixture uses CRLF line endings.** `.gitattributes` sets `*.log -text` so git preserves the
exact bytes. Parsers must `rstrip("\r\n")` — missing the `\r` silently corrupts the last field of
every line.

---

## 3. Parsing contract

The canonical parser is `parse_line()` in `producer/producer.py`. **Reuse it — do not rewrite it**
in the Spark job or the Lambda. Divergent parsers mean the batch and speed layers silently
disagree about what happened, which is the worst class of bug on this project.

### Known data irregularity (already handled)

One fixture line reads `Failed password for invalid user  0101 from ...` — with a **double
space**. A regex assuming single spaces misclassifies it and the failed-count comes out 519
instead of 520. All patterns therefore use `\s+` rather than a literal space. Expect more of this
in the full 655k log; validate counts against the shell ground truth rather than trusting the
parser.

### Validated ground truth — `fixtures/OpenSSH_2k.log`

The parser reproduces these exactly. **Use them as the regression anchor** for the Spark batch job.

| Metric | Value |
|---|---|
| Total lines | 1,999 |
| `status: failed` | **520** |
| `status: invalid_user` | 113 |
| `status: accepted` | 1 |
| `status: other` | 1,100 |
| Lines dropped (no IP) | 265 |

Top failed-auth source IPs:

| IP | Failed count |
|---|---|
| 183.62.140.253 | 286 |
| 187.141.143.180 | 80 |
| 103.99.0.122 | 46 |

Cross-check command:

    grep -c "Failed password" fixtures/OpenSSH_2k.log

---

## 4. S3 key layout

Bucket: `scp-siem-data-<ACCOUNT_ID>` (region `us-east-1`)

    raw/yyyy/mm/dd/hh/          <- Firehose landing, immutable master dataset (UTC-partitioned)
    batch-views/                <- Spark batch aggregates (parquet or csv), partitioned
    athena-results/             <- Athena query output location

Bucket in use: `scp-siem-data-009910375264`

`raw/` is the append-only source of truth for the batch layer. Batch jobs **read** `raw/`, never
mutate it, and write only under `batch-views/`.

### Two properties of `raw/` that every reader must handle

**1. Records are newline-delimited JSON.** Firehose concatenates record payloads with no
delimiter, so the producer appends `\n` to each Kinesis payload. Without it, S3 objects are one
unparseable run-on blob. Consumers can rely on one JSON object per line.

**2. Spark must be told to recurse.** Firehose writes into nested `raw/yyyy/MM/dd/HH/`
directories, and `spark.read.json()` does **not** descend into subdirectories by default —
pointing it at `raw/` yields `UNABLE_TO_INFER_SCHEMA`. Every job that reads `raw/` needs:

    df = spark.read.option("recursiveFileLookup", "true").json("s3://<bucket>/raw/")

---

## 5. DynamoDB speed-view state table

Table: `scp-siem-speed-state` · billing: **PAY_PER_REQUEST** (on-demand) · **LIVE**

- Partition key: `source_ip` (String)
- Sort key: `bucket` (Number) — 30-second bucket start, epoch seconds floored to 30s
- Attributes: `count` (Number) — failed-auth count in that bucket
- `ttl` (Number) — epoch seconds = `bucket + 360`; item auto-expires ~6 min after its bucket
- TTL: **enabled** on the `ttl` attribute

### Windowing scheme (bucketed)

Ten 30-second buckets = a 5-minute window. On each failed event the speed layer:

1. floors `producer_ts` to a 30s boundary → `bucket`
2. atomically `ADD`s 1 to the `(source_ip, bucket)` item's `count`
3. queries the trailing 10 buckets for that IP and sums → current 5-minute count
4. emits an alert if the sum ≥ threshold

Chosen over the exact approach (storing a timestamp list and evicting >5 min) because it holds far
less state and the atomic `ADD` is naturally idempotent-friendly under Kinesis at-least-once
delivery.

**TTL is housekeeping, not correctness.** DynamoDB deletes expired items within ~48h of the `ttl`
timestamp, not on the dot. Harmless: the speed layer only ever sums the trailing 10 buckets, so
stale items never affect a count. TTL exists to stop the table growing forever.

---

## 6. Still open (to be settled at M2)

- **Alert threshold** — how many failed auths in 5 min constitutes an alert. Set it off the back
  of the full-log ground-truth offender counts, not guesswork.
- **Alerts storage** — separate DynamoDB table vs an attribute on the state table.
- **Batch-view format** — parquet vs csv (leaning parquet).
- **Merge implementation** — dashboard-side join (simplest) vs Athena over an exported DynamoDB
  snapshot.
- **EMR managed scaling trigger** — which metric + cooldown. Must be *stated* in the report, not
  merely enabled.
