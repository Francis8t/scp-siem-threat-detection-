# Shared Contracts (LOCKED at M0)

Everything downstream builds against these. Producer, speed layer, batch layer, and serving all
agree on these exact field names. Change only by team agreement — and when one changes, update
`report/architecture.svg` and `TEAMMATE_ONBOARDING.md` in the same commit.

**Region: `us-east-1`** · **Last updated:** 29 Jul 2026 (M2 — full-log ground truth verified locally)

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

### Option-A skip (LOCKED M2 — D17)

`parse_line()` **explicitly drops** two line classes before any IP fallback, via:

    RE_SKIP = re.compile(r"message repeated \d+ times|authentication failure")

- **`message repeated N times: [...]`** (~37k in the full log) — syslog compression; each stands
  for N real failures nested in brackets. Skipped rather than expanded (Option A).
- **PAM `authentication failure` lines** (~231k) — a *second* log line for a failure already
  captured by its `Failed password` line. Emitting them would **double-count** every failure, and
  their `rhost=` is sometimes a hostname, which violates the dotted-quad `source_ip` contract.

Net effect: a **deliberate, stated undercount** of true failed-auth volume. This is acceptable
because the project is graded on the scalable pipeline, not log-parsing completeness. Every
downstream consumer sees a consistent view because all reuse this one parser. **Stated as a known
limitation in the report.**

### Known data irregularity (already handled)

One fixture line reads `Failed password for invalid user  0101 from ...` — with a **double
space**. A regex assuming single spaces misclassifies it and the failed-count comes out 519
instead of 520. All patterns therefore use `\s+` rather than a literal space. Expect more of this
in the full 655k log; validate counts against the shell ground truth rather than trusting the
parser.

### Validated ground truth — `fixtures/OpenSSH_2k.log`

**Use these as the regression anchor** for the Spark batch job. Two columns: the original M1
numbers, and the M2 numbers **with the Option-A skip applied** (D17). The Spark job must reconcile
to the **skip-applied** column, because it reuses the same skipping parser.

| Metric | M1 (raw) | M2 (skip applied) |
|---|---|---|
| Total lines | 1,999 | 1,999 |
| `status: failed` | 520 | **518** |
| `status: invalid_user` | 113 | 113 |
| `status: accepted` | 1 | 1 |
| `status: other` | 1,100 | **601** |
| Lines dropped (skip + no-IP) | 265 | **767** |

The `failed` delta (520 → 518) is the two embedded-repeat failures now skipped. The large `other`
drop (1,100 → 601) is the PAM + repeat lines that used to fall through to `other` and are now
dropped outright.

Top failed-auth source IPs (unchanged by the skip): `183.62.140.253` (286) ·
`187.141.143.180` (80) · `103.99.0.122` (46). **These three are exactly the IPs that fired
alerts in the live speed-layer test** — a useful end-to-end sanity check.

Cross-check command (note: `grep -c` counts the repeat lines the parser skips, so it returns a
*higher* number than the parser — that gap is the D17 undercount, not a bug):

    grep -c "Failed password" fixtures/OpenSSH_2k.log

### Full-log ground truth (skip applied)

Re-verified locally on 29 Jul 2026 against `SSH.log` (MD5 `fc38b9b464eae746aaed8ceb044bd743`).
Every previously-documented figure reproduced exactly; the status breakdown below is new.

| Metric | Value |
|---|---|
| Total lines | 655,146 |
| `status: failed` (skip applied) | **160,616** |
| `status: invalid_user` | 14,581 |
| `status: accepted` | 182 |
| `status: other` | 138,387 |
| Lines dropped (skip + no-IP) | 341,381 |
| `grep -c "Failed password"` (raw) | 197,587 |
| Worst single 5-min window | **157** (`183.63.110.206`) |
| Attacker peak-window band | **126–157** |

> **Line-count note.** `wc -l` reports 655,146 because the file has no trailing newline; a
> Python `for line in fh` loop therefore iterates **655,147** times. Same off-by-one on the
> fixture (1,999 vs 2,000). Not a parser bug — don't "fix" it.

#### Top failed-auth IPs, full log — use THIS list, not `grep`

| Rank | IP | `parse_line()` (skip applied) | Raw `grep` count |
|---|---|---|---|
| 1 | `183.63.110.206` | **17,340** | 17,340 |
| 2 | `183.238.178.195` | **14,519** | 14,519 |
| 3 | `59.63.188.30` | **14,384** | 28,766 |
| 4 | `183.62.140.253` | **10,852** | 10,852 |
| 5 | `139.219.191.138` | **10,852** | 10,852 |
| 6 | `183.192.189.131` | **8,755** | 8,755 |
| 7 | `183.129.154.138` | **8,670** | 8,670 |
| 8 | `183.63.172.52` | **7,506** | 7,506 |
| 9 | `58.242.83.25` | **7,192** | 14,383 |
| 10 | `14.116.171.251` | **4,462** | — |

> **Validation trap — read before reconciling the Spark job.** The shell command in
> `PROJECT_PLAN.md` §5.1 (`grep "Failed password" | ... | sort | uniq -c | sort -rn`) produces a
> **different ranking** from the parser. `message repeated N times: [ Failed password for root
> from ... ]` lines contain the string "Failed password" *inside the brackets*, so `grep` counts
> them while `parse_line()` skips them (D17). The effect is concentrated on the heavy-repeat IPs:
> `59.63.188.30` and `58.242.83.25` are each roughly **doubled** by grep, which lifts
> `59.63.188.30` to a spurious #1 at 28,766.
>
> **The batch layer must reconcile to the parser column**, because it reuses the same skipping
> parser (D12). Reassuringly, the parser's #1 (`183.63.110.206`) is the same IP that owns the
> worst 5-minute burst in the threshold analysis — the parser view is the internally consistent
> one. A Spark job matching the grep ranking would be the actual bug.

---

## 4. S3 key layout

Bucket: `scp-siem-data-<ACCOUNT_ID>` (region `us-east-1`)

    raw/yyyy/mm/dd/hh/          <- Firehose landing, immutable master dataset (UTC-partitioned)
    batch-views/                <- Spark batch aggregates, PARQUET (D20), partitioned
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

### Alert rows (LOCKED M2 — D19)

Alerts live in the **same table**, not a separate one. An alert row for `(source_ip, window)`:

| Attribute | Value |
|---|---|
| `source_ip` (PK) | the offending IP |
| `bucket` (SK) | **`10_000_000_000 + window_start`** — a synthetic marker far above any real 30s bucket, so alert rows sort clear of state rows and never fall inside a `[lo,hi]` window query |
| `kind` | `"alert"` (state rows have no `kind`) |
| `window_start` | epoch seconds — start of the trailing 10-bucket window |
| `window_count` | the window sum that tripped the alert (≥ threshold) |
| `first_seen_ts` | `producer_ts` of the tripping record |
| `ttl` | `window_start + 3600` (alerts kept ~1h for the dashboard) |

Written with an **idempotent conditional put** (`ConditionExpression="attribute_not_exists(source_ip)"`):
the first event to cross the threshold writes the row; every later event in the same window — and
every Kinesis redelivery — is a silent no-op. **Exactly one alert per IP per window.**

Caveat (D6 tradeoff): the *count* rows still double-count on redelivery because atomic `ADD`
re-adds. The *alert* does not. So `window_count` on an alert row can read slightly high under
heavy redelivery; the alert's existence is still correct. Noted in the report.

---

## 6. Still open

**Settled at M2 (28 Jul):**
- ✅ **Alert threshold = 50** / IP / 5-min window (D18) — off the full-log 126–157 attacker band.
- ✅ **Alerts storage** — same table, `kind="alert"` + synthetic `bucket` (D19).

**Settled at M2 (29 Jul):**
- ✅ **Batch-view format = parquet** (D20). Athena reads it natively, so the dashboard is
  unaffected — it queries Athena via boto3 rather than reading the files directly.
- ✅ **Local Spark prototyping dropped** (D21) — replaced by a single-process Python reference
  implementation that doubles as the Experiment 1 sequential baseline.

**Still open (batch/serving):**
- **Merge implementation** — dashboard-side join (simplest) vs Athena over an exported DynamoDB
  snapshot.
- **EMR managed scaling trigger** — which metric + cooldown. Must be *stated* in the report, not
  merely enabled.

---

## 7. Canonical code (M2, as deployed)

Both files below are the M2 versions verified live on 28 Jul. The producer's `parse_line()` is
the single canonical parser (D12) — the Spark job and Lambda must reuse it, not re-implement it.

### 7.1 `producer/producer.py`

```python
"""
Replay producer — M2.

Reads the OpenSSH log, parses each line into the locked record schema
(see CONTRACTS.md), and puts it to Kinesis at a controlled rate.

M2 additions over the M1 skeleton:
  --rate        target records/sec (paced). Omit for max throughput.
  --loop        repeat the file forever (sustain a rate for long benchmark runs)
  --batch-size  records per put_records call (needed to actually reach the
                high benchmark tiers; one put_record per record can't keep up)
  --limit 0     no limit (M1 default of 10 was a benchmarking footgun)

Explicit-skip: `message repeated N times` and PAM `authentication failure`
lines are dropped (Option A, report-accurate). They carry an IP and would
otherwise emit as status:"other" noise. The speed layer only counts
status=="failed", so this is a labelling/accuracy choice, not a correctness
one for detection. Documented as a known undercount limitation in the report.
"""

import argparse
import json
import re
import sys
import time
from datetime import datetime, timezone

import boto3

STREAM_NAME = "scp-siem-stream"
REGION = "us-east-1"

# --- Line patterns -----------------------------------------------------------
# Ordered most-specific first; the first match wins.
IP = r"(\d{1,3}(?:\.\d{1,3}){3})"

RE_FAILED = re.compile(
    rf"Failed password for (?:(invalid user)\s+)?(\S+)\s+from\s+{IP}\s+port\s+(\d+)"
)
RE_ACCEPTED = re.compile(
    rf"Accepted password for (?:invalid user\s+)?(\S+)\s+from\s+{IP}\s+port\s+(\d+)"
)
RE_INVALID_USER = re.compile(rf"Invalid user\s+(\S+)\s+from\s+{IP}")
RE_ANY_IP = re.compile(IP)
RE_HEADER = re.compile(r"^\w{3}\s+\d+\s[\d:]+\s(\S+)\ssshd\[\d+\]:\s(.*)$")

# Option A explicit-skip: drop these before any IP fallback.
RE_SKIP = re.compile(r"message repeated \d+ times|authentication failure")


def parse_line(line):
    """OpenSSH log line -> record dict, or None if skipped / no usable IP."""
    line = line.rstrip("\r\n")  # fixture is CRLF; strip both
    if not line:
        return None

    header = RE_HEADER.match(line)
    if not header:
        return None
    host, message = header.group(1), header.group(2)

    # Explicit-skip (Option A): message-repeated + PAM auth-failure lines.
    if RE_SKIP.search(message):
        return None

    user, port, status = None, None, "other"

    m = RE_FAILED.search(message)
    if m:
        status = "failed"
        user, source_ip, port = m.group(2), m.group(3), int(m.group(4))
    else:
        m = RE_ACCEPTED.search(message)
        if m:
            status = "accepted"
            user, source_ip, port = m.group(1), m.group(2), int(m.group(3))
        else:
            m = RE_INVALID_USER.search(message)
            if m:
                status = "invalid_user"
                user, source_ip = m.group(1), m.group(2)
            else:
                m = RE_ANY_IP.search(message)
                if not m:
                    return None  # no IP -> not useful to us
                source_ip = m.group(1)

    now = datetime.now(timezone.utc)
    return {
        "producer_ts": now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond // 1000:03d}Z",
        "source_ip": source_ip,
        "status": status,
        "user": user,
        "port": port,
        "host": host,
    }


def iter_records(path, loop):
    """Yield parsed records from the file, optionally forever."""
    while True:
        with open(path, "r", encoding="utf-8", errors="replace") as fh:
            for line in fh:
                rec = parse_line(line)
                if rec is not None:
                    yield rec
        if not loop:
            return


def flush(client, batch):
    """Send one put_records batch, retrying only the failed records once."""
    if not batch:
        return 0
    entries = [
        {"Data": (json.dumps(r) + "\n").encode("utf-8"), "PartitionKey": r["source_ip"]}
        for r in batch
    ]
    resp = client.put_records(StreamName=STREAM_NAME, Records=entries)
    failed = resp.get("FailedRecordCount", 0)
    if failed:
        # Retry just the failures once (throughput exceeded / internal error).
        retry = [entries[i] for i, r in enumerate(resp["Records"]) if r.get("ErrorCode")]
        if retry:
            time.sleep(0.2)
            client.put_records(StreamName=STREAM_NAME, Records=retry)
    return len(entries)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", default="fixtures/OpenSSH_2k.log")
    ap.add_argument("--limit", type=int, default=0,
                    help="max records to send; 0 = no limit (default 0)")
    ap.add_argument("--rate", type=float, default=0,
                    help="target records/sec; 0 = as fast as possible (default 0)")
    ap.add_argument("--loop", action="store_true",
                    help="repeat the file forever (for sustained benchmark runs)")
    ap.add_argument("--batch-size", type=int, default=200,
                    help="records per put_records call (max 500; default 200)")
    ap.add_argument("--dry-run", action="store_true",
                    help="parse and print only; do not touch AWS")
    args = ap.parse_args()

    batch_size = max(1, min(args.batch_size, 500))  # Kinesis hard cap is 500
    client = None if args.dry_run else boto3.client("kinesis", region_name=REGION)

    sent = 0
    batch = []
    window_start = time.perf_counter()
    window_count = 0

    def pace():
        """Sleep so we don't exceed --rate, measured per 1s window."""
        nonlocal window_start, window_count
        if args.rate <= 0:
            return
        window_count += 1
        if window_count >= args.rate:
            elapsed = time.perf_counter() - window_start
            if elapsed < 1.0:
                time.sleep(1.0 - elapsed)
            window_start = time.perf_counter()
            window_count = 0

    try:
        for rec in iter_records(args.file, args.loop):
            if args.dry_run:
                print(json.dumps(rec))
            else:
                batch.append(rec)
                if len(batch) >= batch_size:
                    flush(client, batch)
                    batch = []
            sent += 1
            pace()
            if args.limit and sent >= args.limit:
                break
        if not args.dry_run:
            flush(client, batch)  # final partial batch
    except KeyboardInterrupt:
        if not args.dry_run:
            flush(client, batch)
        print(f"\ninterrupted after {sent} records", file=sys.stderr)

    print(f"\n{'parsed' if args.dry_run else 'sent'} {sent} records")


if __name__ == "__main__":
    main()
```

### 7.2 `speed/lambda_function.py`

```python
"""
Speed-layer Lambda — M2 (sliding window + alerting).

Triggered by the Kinesis event source mapping. For each `failed` auth record:
  1. floor producer_ts into a 30-second bucket
  2. atomically ADD 1 to (source_ip, bucket).count in DynamoDB, set TTL
  3. sum the trailing ten buckets for that IP  -> current 5-minute window count
  4. if the window count >= ALERT_THRESHOLD, write ONE alert item for that IP
     and window (idempotent: a conditional put that no-ops if it already exists)

Windowing scheme, table design, and the at-least-once tradeoff are all per
CONTRACTS.md §5. The M1 count logic is unchanged; everything from the window
query down is new.

Alert storage (settles a CONTRACTS §6 open item): alerts live in the SAME
state table, distinguished by a synthetic numeric sort key
10_000_000_000 + window_start (the table's sort key is a Number, so alerts
cannot use a string marker). That value sorts clear of every real 30s bucket,
so alert rows never fall inside a [lo,hi] window query. No second table, no
extra IAM. The serving layer already queries this table by source_ip, so
merged reads stay single-table.
"""

import base64
import json
import os
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

TABLE = os.environ.get("STATE_TABLE", "scp-siem-speed-state")
BUCKET_SECONDS = 30           # ten of these = the 5-minute window (CONTRACTS.md §5)
WINDOW_BUCKETS = 10           # trailing buckets summed = 5 min
TTL_SECONDS = 360             # bucket + 6 min
ALERT_THRESHOLD = int(os.environ.get("ALERT_THRESHOLD", "50"))
ALERT_TTL_SECONDS = 3600      # keep alert rows ~1h for the dashboard

ddb = boto3.client("dynamodb")


def to_epoch(ts):
    """'2026-07-24T14:03:22.481Z' -> epoch seconds (float)."""
    return (datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S.%fZ")
            .replace(tzinfo=timezone.utc)
            .timestamp())


def floor_bucket(epoch_seconds):
    """Floor to the containing 30-second boundary."""
    return int(epoch_seconds // BUCKET_SECONDS) * BUCKET_SECONDS


def window_sum(source_ip, newest_bucket):
    """Sum `count` across the trailing WINDOW_BUCKETS buckets for this IP.

    Query is bounded to the 10-bucket window [oldest, newest] so it never
    scans the whole partition. Alert rows are excluded automatically: their
    synthetic bucket (10_000_000_000 + window_start) sits far above :hi.
    """
    oldest = newest_bucket - (WINDOW_BUCKETS - 1) * BUCKET_SECONDS
    resp = ddb.query(
        TableName=TABLE,
        KeyConditionExpression="source_ip = :ip AND #b BETWEEN :lo AND :hi",
        ExpressionAttributeNames={"#b": "bucket", "#c": "count"},
        ExpressionAttributeValues={
            ":ip": {"S": source_ip},
            ":lo": {"N": str(oldest)},
            ":hi": {"N": str(newest_bucket)},
        },
        ProjectionExpression="#c",
    )
    total = 0
    for item in resp.get("Items", []):
        total += int(item.get("count", {}).get("N", "0"))
    return total


def try_emit_alert(source_ip, window_start, window_count, producer_ts):
    """Write ONE alert row for (ip, window_start). Idempotent.

    The conditional expression makes the put a no-op if the row already
    exists, so repeated events in the same window — and Kinesis redeliveries —
    produce exactly one alert. No need to know the previous count.
    """
    try:
        ddb.put_item(
            TableName=TABLE,
            Item={
                "source_ip": {"S": source_ip},
                # 'bucket' is the table's sort key (Number). Alerts need a
                # sortable, non-colliding SK. We keep bucket numeric for state
                # rows and store the alert marker in a separate attribute set,
                # using a very large synthetic bucket so alert rows sort after
                # state rows and never fall inside a [lo,hi] window query.
                "bucket": {"N": str(10_000_000_000 + window_start)},
                "kind": {"S": "alert"},
                "window_start": {"N": str(window_start)},
                "window_count": {"N": str(window_count)},
                "first_seen_ts": {"S": producer_ts},
                "ttl": {"N": str(window_start + ALERT_TTL_SECONDS)},
            },
            ConditionExpression="attribute_not_exists(source_ip)",
        )
        print(json.dumps({
            "alert": True, "source_ip": source_ip,
            "window_start": window_start, "window_count": window_count,
        }))
    except ClientError as e:
        if e.response["Error"]["Code"] != "ConditionalCheckFailedException":
            raise
        # Alert for this IP+window already exists -> intended no-op.


def lambda_handler(event, context):
    processed = 0
    counted = 0
    alerts_checked = 0

    for r in event["Records"]:
        payload = base64.b64decode(r["kinesis"]["data"]).decode("utf-8")
        rec = json.loads(payload)
        processed += 1

        if rec.get("status") != "failed":
            continue

        bucket = floor_bucket(to_epoch(rec["producer_ts"]))

        # --- count (unchanged from M1) --------------------------------------
        ddb.update_item(
            TableName=TABLE,
            Key={
                "source_ip": {"S": rec["source_ip"]},
                "bucket": {"N": str(bucket)},
            },
            UpdateExpression="SET #t = :ttl ADD #c :one",
            ExpressionAttributeNames={"#c": "count", "#t": "ttl"},
            ExpressionAttributeValues={
                ":one": {"N": "1"},
                ":ttl": {"N": str(bucket + TTL_SECONDS)},
            },
        )
        counted += 1

        # --- window + alert (new in M2) -------------------------------------
        total = window_sum(rec["source_ip"], bucket)
        if total >= ALERT_THRESHOLD:
            alerts_checked += 1
            # window_start = start of the trailing 10-bucket window
            window_start = bucket - (WINDOW_BUCKETS - 1) * BUCKET_SECONDS
            try_emit_alert(rec["source_ip"], window_start, total, rec["producer_ts"])

    print(json.dumps({
        "processed": processed,
        "counted_failed": counted,
        "alerts_checked": alerts_checked,
    }))
    return {"processed": processed, "counted_failed": counted}
```

### 7.3 `benchmarks/find_threshold.py` (threshold-derivation helper)

> **Status: specified here, not yet written to `benchmarks/`.** As of 29 Jul 2026 `benchmarks/`
> contains only `.gitkeep`. The listing below is the agreed design, not a transcript of a
> committed file — treat it as the spec to implement when the script is written.
>
> **The D18 numbers it justifies are nonetheless verified.** On 29 Jul this logic was run against
> `SSH.log` and reproduced every documented figure exactly: 160,616 failed events, **7,026**
> distinct (IP, 5-min window) pairs, worst single window **157** (`183.63.110.206`, Jan 03 01:50),
> and a top-15 peak band of exactly **126–157**. So the threshold justification rests on real
> evidence and the report can state it with confidence — but committing the script is what makes
> it *reproducible* by a marker, which is why it stays on the open-items list.

Not part of the runtime pipeline — a one-off analysis script that derives the D18 threshold.
Reuses `parse_line()` so the threshold basis matches exactly what the Lambda receives.

**Verified peak-window bursts (29 Jul).** The tight clustering is the argument for threshold 50 —
every confirmed brute-forcer peaks in a narrow band far above it, and no legitimate user comes
close:

| IP | Peak single 5-min window |
|---|---|
| `183.63.110.206` | 157 |
| `183.63.172.52` | 152 |
| `183.62.140.253` | 149 |
| `183.238.178.195` | 148 |
| `14.116.171.251` | 147 |
| … (top 15 tail) | down to 126 |

```python
"""
Threshold-finder for the speed-layer alert level.

Runs the SAME parse_line() as the producer (import, don't re-implement — D12),
applies the SAME Option-A skip, then buckets each IP's FAILED events into
5-minute windows using the LOG'S OWN timestamp, and reports the busiest windows.

This tells us the natural per-5-min burst size of real brute-force IPs, which
is what the sliding-window Lambda will actually see. Set the alert threshold
off these numbers, not off the 28-day grep totals.

Usage:
    python3 find_threshold.py --file SSH.log
"""

import argparse
import collections
import re
from datetime import datetime

import producer  # reuse parse_line + RE_SKIP (D12: single canonical parser)

# The log line's own leading timestamp, e.g. "Dec 10 06:55:48".
# No year in the log; we only need relative time to bucket, so we pin any year.
RE_TS = re.compile(r"^(\w{3}\s+\d+\s[\d:]+)")
MONTHS = {m: i for i, m in enumerate(
    ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
     "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"], start=1)}


def log_epoch(line):
    """Seconds-since-a-fixed-point from the log's own timestamp, or None."""
    m = RE_TS.match(line)
    if not m:
        return None
    mon_str, day, clock = m.group(1).split(None, 2)
    h, mi, s = clock.split(":")
    # Fixed nominal year; only relative spacing matters for bucketing.
    dt = datetime(2018, MONTHS[mon_str], int(day), int(h), int(mi), int(s))
    return int(dt.timestamp())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", default="SSH.log")
    ap.add_argument("--window", type=int, default=300, help="window seconds (default 300 = 5 min)")
    ap.add_argument("--top", type=int, default=15, help="how many top windows to show")
    args = ap.parse_args()

    # (source_ip, window_bucket) -> failed count
    windows = collections.Counter()
    total_failed = 0

    with open(args.file, "r", encoding="utf-8", errors="replace") as fh:
        for line in fh:
            rec = producer.parse_line(line)          # applies Option-A skip already
            if rec is None or rec["status"] != "failed":
                continue
            ts = log_epoch(line)
            if ts is None:
                continue
            bucket = ts - (ts % args.window)          # floor to window start
            windows[(rec["source_ip"], bucket)] += 1
            total_failed += 1

    print(f"total FAILED events (skip applied): {total_failed}")
    print(f"distinct (IP, {args.window}s-window) pairs: {len(windows)}\n")

    print(f"Top {args.top} busiest 5-min windows (IP, window count):")
    for (ip, bucket), n in windows.most_common(args.top):
        when = datetime.fromtimestamp(bucket).strftime("%b %d %H:%M")
        print(f"  {n:6d}   {ip:<18} starting {when}")

    # Per-IP peak: the worst single window each IP ever hits.
    peak = collections.Counter()
    for (ip, _), n in windows.items():
        if n > peak[ip]:
            peak[ip] = n
    print(f"\nTop {args.top} IPs by their PEAK single-window burst:")
    for ip, n in peak.most_common(args.top):
        print(f"  {n:6d}   {ip}")


if __name__ == "__main__":
    main()
```
