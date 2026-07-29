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