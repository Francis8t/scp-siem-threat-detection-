"""
Speed-layer Lambda — M3 (sliding window + alerting, batch-aggregated).

Triggered by the Kinesis event source mapping. For each `failed` auth record:
  1. floor producer_ts into a 30-second bucket
  2. atomically ADD to (source_ip, bucket).count in DynamoDB, set TTL
  3. sum the trailing ten buckets for that IP  -> current 5-minute window count
  4. if the window count >= ALERT_THRESHOLD, write ONE alert item for that IP
     and window (idempotent: a conditional put that no-ops if it already exists)

Windowing scheme, table design, and the at-least-once tradeoff are all per
CONTRACTS.md §5.

Alert storage: alerts live in the SAME state table, distinguished by a
synthetic numeric sort key 10_000_000_000 + window_start (the table's sort key
is a Number, so alerts cannot use a string marker). That value sorts clear of
every real 30s bucket, so alert rows never fall inside a [lo,hi] window query.
No second table, no extra IAM.

--------------------------------------------------------------------------
M3 OPTIMISATION — why this handler aggregates before it writes
--------------------------------------------------------------------------
The M2 version did one UpdateItem AND one Query per failed record: two
synchronous DynamoDB round trips each, in a serial loop. Measured on a 1,000
rec/s replay that cost ~2,200 ms per 100-record batch (~22 ms/record) and the
consumer fell 138 SECONDS behind the stream (CloudWatch
GetRecords.IteratorAgeMilliseconds). Effective throughput was ~90 rec/s — an
order of magnitude below the single-shard Kinesis limit, so the speed layer,
not the shard, was the bottleneck.

This version does the same work in three phases per batch:

  1. decode every record and aggregate in memory into (source_ip, bucket) -> n
  2. ONE UpdateItem per group, using `ADD :n` instead of n separate `ADD :1`
  3. ONE window Query per DISTINCT source_ip, not per record

DynamoDB calls drop from 2N to len(groups) + len(distinct_ips). The win scales
with key skew, and brute-force traffic is extremely skewed by nature: a batch
dominated by one attacking IP collapses from ~200 calls to ~2.

Phase 3 MUST run after phase 2 completes, or the window sum would be computed
against counts that have not landed yet. That ordering is the reason these are
separate loops rather than one.

Semantics are deliberately unchanged: `ADD` still double-counts on Kinesis
redelivery (the D6 tradeoff), and the alert is still written by an idempotent
conditional put, so it is still exactly one alert per IP per window.

--------------------------------------------------------------------------
M4 INSTRUMENTATION — end-to-end latency for Experiment 2
--------------------------------------------------------------------------
Each invocation reports lag_ms_p50 / p95 / max on its summary line: the time
from the producer stamping producer_ts to this handler processing the record.

This is NOT the same thing as GetRecords.IteratorAgeMilliseconds. Iterator age
measures how far the consumer trails the head of the shard; this measures how
old an individual record was when it was handled. Under saturation the two
diverge, and that divergence is itself the finding.

The percentiles are computed PER BATCH, so aggregating them across invocations
yields the distribution of batch latencies, not of individual records. True
per-record percentiles would need one log line per record, which at 5,000 rec/s
is far too much log volume to be worth it. Describe them as batch-level in the
report rather than implying per-record p99.
"""

import base64
import json
import os
import time
from collections import defaultdict
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

    # --- phase 1: decode and aggregate in memory (no I/O) -------------------
    groups = defaultdict(int)   # (source_ip, bucket) -> count in this batch
    newest = {}                 # source_ip -> (newest_bucket, its producer_ts)
    lags_ms = []                # emit-to-process latency per record (Experiment 2)
    now = time.time()

    for r in event["Records"]:
        payload = base64.b64decode(r["kinesis"]["data"]).decode("utf-8")
        rec = json.loads(payload)
        processed += 1

        if rec.get("status") != "failed":
            continue

        event_epoch = to_epoch(rec["producer_ts"])
        # End-to-end latency: producer emit -> this handler. The plan's
        # Experiment 2 metric. Distinct from IteratorAgeMilliseconds, which
        # measures how far the consumer trails the shard, not per-record age.
        lags_ms.append((now - event_epoch) * 1000.0)

        bucket = floor_bucket(event_epoch)
        groups[(rec["source_ip"], bucket)] += 1
        counted += 1

        # The window query needs the LATEST bucket this IP touched in the
        # batch; anything older is already inside that window's range.
        prev = newest.get(rec["source_ip"])
        if prev is None or bucket > prev[0]:
            newest[rec["source_ip"]] = (bucket, rec["producer_ts"])

    # --- phase 2: one UpdateItem per (ip, bucket) group ---------------------
    for (source_ip, bucket), n in groups.items():
        ddb.update_item(
            TableName=TABLE,
            Key={
                "source_ip": {"S": source_ip},
                "bucket": {"N": str(bucket)},
            },
            UpdateExpression="SET #t = :ttl ADD #c :n",
            ExpressionAttributeNames={"#c": "count", "#t": "ttl"},
            ExpressionAttributeValues={
                ":n": {"N": str(n)},
                ":ttl": {"N": str(bucket + TTL_SECONDS)},
            },
        )

    # --- phase 3: one window query per distinct IP -------------------------
    # Must follow phase 2: the sum has to see this batch's counts.
    alerts_checked = 0
    for source_ip, (bucket, producer_ts) in newest.items():
        total = window_sum(source_ip, bucket)
        if total >= ALERT_THRESHOLD:
            alerts_checked += 1
            window_start = bucket - (WINDOW_BUCKETS - 1) * BUCKET_SECONDS
            try_emit_alert(source_ip, window_start, total, producer_ts)

    summary = {
        "processed": processed,
        "counted_failed": counted,
        "alerts_checked": alerts_checked,
        "ddb_calls": len(groups) + len(newest),
        "ddb_calls_before_optimisation": counted * 2,
    }
    if lags_ms:
        ordered = sorted(lags_ms)
        def pct(q):
            # Nearest-rank; exact enough at these batch sizes and avoids
            # pulling in statistics just for this.
            return round(ordered[min(len(ordered) - 1, int(q * len(ordered)))], 1)
        summary.update({"lag_ms_p50": pct(0.50), "lag_ms_p95": pct(0.95),
                        "lag_ms_max": round(ordered[-1], 1)})
    print(json.dumps(summary))
    return {"processed": processed, "counted_failed": counted}
