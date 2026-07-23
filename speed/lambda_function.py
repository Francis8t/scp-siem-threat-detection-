"""
Speed-layer Lambda — Step 8 (M1 walking skeleton).

Triggered by the Kinesis event source mapping. For each `failed` auth record it
floors producer_ts into a 30-second bucket and atomically increments the count
for (source_ip, bucket) in DynamoDB, setting a TTL.

M1 scope: ingest + count. The trailing-10-bucket window query and alert
threshold arrive in M2 — this handler is the foundation for them, not a throwaway.
"""

import base64
import json
import os
from datetime import datetime, timezone

import boto3

TABLE = os.environ.get("STATE_TABLE", "scp-siem-speed-state")
BUCKET_SECONDS = 30       # ten of these = the 5-minute window (CONTRACTS.md §5)
TTL_SECONDS = 360         # bucket + 6 min

ddb = boto3.client("dynamodb")


def to_epoch(ts):
    """'2026-07-24T14:03:22.481Z' -> epoch seconds (float)."""
    return (datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S.%fZ")
            .replace(tzinfo=timezone.utc)
            .timestamp())


def floor_bucket(epoch_seconds):
    """Floor to the containing 30-second boundary."""
    return int(epoch_seconds // BUCKET_SECONDS) * BUCKET_SECONDS


def lambda_handler(event, context):
    processed = 0
    counted = 0

    for r in event["Records"]:
        payload = base64.b64decode(r["kinesis"]["data"]).decode("utf-8")
        rec = json.loads(payload)
        processed += 1

        # M1: only failed auths drive the window count.
        if rec.get("status") != "failed":
            continue

        bucket = floor_bucket(to_epoch(rec["producer_ts"]))

        # 'count' and 'ttl' are DynamoDB reserved words -> alias both.
        # ADD is atomic, so concurrent shards/retries can't lose an increment.
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

    print(json.dumps({"processed": processed, "counted_failed": counted}))
    return {"processed": processed, "counted_failed": counted}