"""
Speed-layer tests — no AWS, no moto, no dependencies.

Runs speed/lambda_function.py against an in-memory DynamoDB fake that
implements only the four operations the handler uses. Pins the behaviours the
speed layer is actually relied upon for:

  1. counting        — only status=="failed" is counted, into the right
                       30-second bucket
  2. batch equivalence — aggregating a batch gives the same counts as feeding
                       the records one at a time
  3. windowing       — the sum covers exactly the trailing 10 buckets, and
                       anything older is excluded
  4. alerting        — fires at the threshold, once per IP per window
  5. idempotency     — Kinesis at-least-once redelivery does not duplicate the
                       alert (the D19 guarantee)

Run:  python3 speed/test_speed_layer.py
"""

import base64
import importlib.util
import json
import os
import sys
from datetime import datetime, timedelta, timezone

from botocore.exceptions import ClientError

HERE = os.path.dirname(os.path.abspath(__file__))


class FakeDDB:
    """In-memory stand-in for the DynamoDB calls the handler makes."""

    def __init__(self):
        self.items = {}
        self.calls = 0

    def update_item(self, TableName, Key, UpdateExpression,
                    ExpressionAttributeNames, ExpressionAttributeValues):
        self.calls += 1
        key = (Key["source_ip"]["S"], int(Key["bucket"]["N"]))
        inc_key = ":n" if ":n" in ExpressionAttributeValues else ":one"
        inc = int(ExpressionAttributeValues[inc_key]["N"])
        item = self.items.setdefault(key, {})
        item["count"] = {"N": str(int(item.get("count", {"N": "0"})["N"]) + inc)}
        item["ttl"] = ExpressionAttributeValues[":ttl"]

    def query(self, TableName, KeyConditionExpression, ExpressionAttributeNames,
              ExpressionAttributeValues, ProjectionExpression):
        self.calls += 1
        ip = ExpressionAttributeValues[":ip"]["S"]
        lo = int(ExpressionAttributeValues[":lo"]["N"])
        hi = int(ExpressionAttributeValues[":hi"]["N"])
        return {"Items": [v for (k_ip, k_b), v in self.items.items()
                          if k_ip == ip and lo <= k_b <= hi]}

    def put_item(self, TableName, Item, ConditionExpression):
        self.calls += 1
        key = (Item["source_ip"]["S"], int(Item["bucket"]["N"]))
        if key in self.items:              # attribute_not_exists(source_ip)
            raise ClientError({"Error": {"Code": "ConditionalCheckFailedException",
                                         "Message": "exists"}}, "PutItem")
        self.items[key] = dict(Item)

    def counts(self):
        return {k: int(v["count"]["N"])
                for k, v in self.items.items() if "kind" not in v}

    def alerts(self):
        return {k: v for k, v in self.items.items() if "kind" in v}


def load_handler():
    spec = importlib.util.spec_from_file_location(
        "lambda_function", os.path.join(HERE, "lambda_function.py"))
    mod = importlib.util.module_from_spec(spec)
    sys.modules["lambda_function"] = mod
    spec.loader.exec_module(mod)
    return mod


BASE = datetime(2026, 7, 29, 12, 0, 0, tzinfo=timezone.utc)


def rec(ip, offset_seconds, status="failed"):
    ts = BASE + timedelta(seconds=offset_seconds)
    return {"kinesis": {"data": base64.b64encode(json.dumps({
        "producer_ts": ts.strftime("%Y-%m-%dT%H:%M:%S.") + f"{ts.microsecond // 1000:03d}Z",
        "source_ip": ip, "status": status,
        "user": None, "port": None, "host": "LabSZ",
    }).encode()).decode()}}


def run(mod, records):
    fake = FakeDDB()
    mod.ddb = fake
    mod.lambda_handler({"Records": records}, None)
    return fake


def test_only_failed_counted(mod):
    fake = run(mod, [rec("1.1.1.1", 0, s)
                     for s in ("failed", "accepted", "invalid_user", "other", "failed")])
    assert list(fake.counts().values()) == [2], fake.counts()
    print("  ok  only status=='failed' is counted")


def test_bucketing(mod):
    # 0s and 29s share a bucket; 30s starts the next one.
    fake = run(mod, [rec("1.1.1.1", 0), rec("1.1.1.1", 29), rec("1.1.1.1", 30)])
    buckets = sorted(b for (_ip, b) in fake.counts())
    assert buckets[1] - buckets[0] == 30, buckets
    assert sorted(fake.counts().values()) == [1, 2], fake.counts()
    print("  ok  events land in the correct 30-second buckets")


def test_batching_matches_singles(mod):
    records = [rec("1.1.1.1", i % 90) for i in range(120)]
    batched = run(mod, records)
    singles = FakeDDB()
    mod.ddb = singles
    for r in records:
        mod.lambda_handler({"Records": [r]}, None)
    assert batched.counts() == singles.counts(), (batched.counts(), singles.counts())
    print(f"  ok  batch aggregation matches one-at-a-time "
          f"({singles.calls} ddb calls -> {batched.calls})")


def test_window_excludes_old_buckets(mod):
    # 10 buckets * 30s = 300s window. An event 400s earlier must not count.
    records = [rec("1.1.1.1", 0)] * 40 + [rec("1.1.1.1", 400)] * 20
    fake = run(mod, records)
    assert not fake.alerts(), "60 events split across a >5min gap must not alert"
    print("  ok  window excludes buckets older than the trailing 10")


def test_alert_fires_at_threshold(mod):
    fake = run(mod, [rec("2.2.2.2", 0) for _ in range(mod.ALERT_THRESHOLD)])
    alerts = fake.alerts()
    assert len(alerts) == 1, alerts
    (_ip, sk), item = next(iter(alerts.items()))
    assert sk >= 10_000_000_000, "alert must use the synthetic sort key (D19)"
    assert int(item["window_count"]["N"]) >= mod.ALERT_THRESHOLD
    print(f"  ok  alert fires at threshold={mod.ALERT_THRESHOLD} "
          f"with count={item['window_count']['N']}")


def test_no_alert_below_threshold(mod):
    fake = run(mod, [rec("3.3.3.3", 0) for _ in range(mod.ALERT_THRESHOLD - 1)])
    assert not fake.alerts(), "must not alert below the threshold"
    print("  ok  no alert one event below the threshold")


def test_redelivery_idempotent(mod):
    """The D19 guarantee: exactly one alert per IP per window under retries."""
    records = [rec("4.4.4.4", 0) for _ in range(mod.ALERT_THRESHOLD * 2)]
    fake = FakeDDB()
    mod.ddb = fake
    for _ in range(3):                       # same batch delivered three times
        mod.lambda_handler({"Records": records}, None)
    assert len(fake.alerts()) == 1, f"expected 1 alert, got {len(fake.alerts())}"
    # Counts DO inflate on redelivery — the documented D6 tradeoff.
    assert sum(fake.counts().values()) == mod.ALERT_THRESHOLD * 6
    print("  ok  redelivery yields exactly one alert (counts inflate, per D6)")


def test_alert_rows_excluded_from_window(mod):
    """Alert rows live in the same table; they must never be summed as counts."""
    fake = run(mod, [rec("5.5.5.5", 0) for _ in range(mod.ALERT_THRESHOLD)])
    mod.ddb = fake
    total = mod.window_sum("5.5.5.5", mod.floor_bucket(BASE.timestamp()))
    assert total == mod.ALERT_THRESHOLD, f"alert row leaked into the window sum: {total}"
    print("  ok  alert rows are excluded from window sums")


def main():
    mod = load_handler()
    print(f"testing {mod.__file__}")
    for fn in (test_only_failed_counted, test_bucketing, test_batching_matches_singles,
               test_window_excludes_old_buckets, test_alert_fires_at_threshold,
               test_no_alert_below_threshold, test_redelivery_idempotent,
               test_alert_rows_excluded_from_window):
        fn(mod)
    print("\nALL SPEED-LAYER TESTS PASS")


if __name__ == "__main__":
    main()
