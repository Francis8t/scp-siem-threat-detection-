"""
Serving layer — the merge (M3).

Combines the two views the Lambda architecture keeps separate:

  * BATCH  — full-history reputation, Athena over the parquet batch view in
             s3://<bucket>/batch-views/full/  (authoritative, complete, slow)
  * SPEED  — what is happening right now, DynamoDB alert rows written by the
             speed Lambda                    (fresh, partial, fast)

The product is the "confirmed repeat offender AND spiking right now" list,
which is a higher-confidence signal than either view alone — the whole reason
the architecture is split.

Per D25 the work is divided rather than centralised: Athena does the batch-side
ranking and threshold filtering in SQL and returns a small result set,
DynamoDB supplies current alerts, and the final join over those two small sets
happens here in Python. Stated limitation for the report: that last join runs
in the serving/presentation layer, not in a distributed engine.

IMPORTANT — the speed view is ephemeral by design. State rows live ~6 minutes
(ttl = bucket + 360) and alert rows ~1 hour (ttl = window_start + 3600), so a
few hours after a replay the table is empty and every IP reports as batch-only.
That is correct behaviour, not a bug: "spiking right now" is meaningless about
a stream that stopped hours ago. To see live rows, run the producer first:

    python3 producer/producer.py --file SSH.log --rate 1000

Standalone use (prints the merged table; the dashboard imports these functions):

    python3 serving/merge.py --top 20
"""

import argparse
import time

import boto3

REGION = "us-east-1"
BUCKET = "scp-siem-data-009910375264"
DATABASE = "scp_siem"
TABLE = "batch_view"
STATE_TABLE = "scp-siem-speed-state"
ATHENA_OUTPUT = f"s3://{BUCKET}/athena-results/"

# Mirrors the speed layer's ALERT_THRESHOLD (D18) so "repeat offender" and
# "spiking now" are judged on a consistent basis rather than two arbitrary bars.
DEFAULT_MIN_FAILED = 50


def athena_query(sql, database=DATABASE, output=ATHENA_OUTPUT,
                 region=REGION, poll_seconds=0.5, timeout_seconds=60):
    """Run one Athena statement and return a list of dicts.

    Athena is asynchronous: submit, poll for terminal state, then page the
    results. Note Athena permits exactly ONE statement per execution — no
    semicolon-separated batches.
    """
    client = boto3.client("athena", region_name=region)
    execution = client.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": output},
    )
    qid = execution["QueryExecutionId"]

    deadline = time.time() + timeout_seconds
    while True:
        info = client.get_query_execution(QueryExecutionId=qid)["QueryExecution"]
        state = info["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        if time.time() > deadline:
            raise TimeoutError(f"Athena query {qid} still {state} after {timeout_seconds}s")
        time.sleep(poll_seconds)

    if state != "SUCCEEDED":
        reason = info["Status"].get("StateChangeReason", "no reason given")
        raise RuntimeError(f"Athena query {state}: {reason}")

    rows = []
    header = None
    paginator = client.get_paginator("get_query_results")
    for page in paginator.paginate(QueryExecutionId=qid):
        for row in page["ResultSet"]["Rows"]:
            # A missing VarCharValue means SQL NULL, not an empty string.
            values = [c.get("VarCharValue") for c in row["Data"]]
            if header is None:
                header = values  # Athena returns the column names as row 1
                continue
            rows.append(dict(zip(header, values)))
    return rows


def batch_offenders(min_failed=DEFAULT_MIN_FAILED, limit=1000):
    """Full-history reputation: the batch side of the merge.

    The filtering and ranking are pushed into SQL (D25) so Athena returns tens
    of rows rather than all 1,238 for the client to sort.
    """
    sql = f"""
        SELECT source_ip, failed, accepted, invalid_user, other, total,
               first_seen, last_seen
        FROM {TABLE}
        WHERE failed >= {int(min_failed)}
        ORDER BY failed DESC, source_ip ASC
        LIMIT {int(limit)}
    """
    out = []
    for r in athena_query(sql):
        out.append({
            "source_ip": r["source_ip"],
            "failed": int(r["failed"]),
            "accepted": int(r["accepted"]),
            "invalid_user": int(r["invalid_user"]),
            "other": int(r["other"]),
            "total": int(r["total"]),
            "first_seen": r["first_seen"],
            "last_seen": r["last_seen"],
        })
    return out


def live_alerts(region=REGION, table=STATE_TABLE):
    """Current alert rows from the speed view, keyed by source_ip.

    A Scan rather than a Query: alerts are spread across every partition, and
    the table has no GSI on `kind`, so there is no key condition that would
    reach them. Acceptable here because TTL keeps the table small (~1 h of
    alerts), but it would need a GSI at real scale — noted in the report.

    Returns {} when nothing is currently spiking, which is the normal state
    between producer runs.
    """
    ddb = boto3.client("dynamodb", region_name=region)
    alerts = {}
    kwargs = {
        "TableName": table,
        "FilterExpression": "#k = :alert",
        "ExpressionAttributeNames": {"#k": "kind"},
        "ExpressionAttributeValues": {":alert": {"S": "alert"}},
    }
    while True:
        resp = ddb.scan(**kwargs)
        for item in resp.get("Items", []):
            ip = item["source_ip"]["S"]
            window_start = int(item["window_start"]["N"])
            window_count = int(item["window_count"]["N"])
            # One IP can hold several alert rows because the window slides every
            # 30 s. Keep the most recent, and track how many fired as a crude
            # persistence signal.
            existing = alerts.get(ip)
            if existing is None or window_start > existing["window_start"]:
                alerts[ip] = {
                    "source_ip": ip,
                    "window_start": window_start,
                    "window_count": window_count,
                    "first_seen_ts": item.get("first_seen_ts", {}).get("S"),
                    "alert_rows": 1 if existing is None else existing["alert_rows"] + 1,
                }
            else:
                existing["alert_rows"] += 1
        if "LastEvaluatedKey" not in resp:
            return alerts
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]


def alert_history(region=REGION, table=STATE_TABLE):
    """Every current alert row, uncollapsed — one entry per (IP, window).

    live_alerts() keeps only the newest row per IP because the merge cares
    about current state. The dashboard's time-series needs them all, since
    each row marks a distinct 30-second window in which that IP was over the
    threshold. Same TTL horizon applies: roughly the last hour.
    """
    ddb = boto3.client("dynamodb", region_name=region)
    rows = []
    kwargs = {
        "TableName": table,
        "FilterExpression": "#k = :alert",
        "ExpressionAttributeNames": {"#k": "kind"},
        "ExpressionAttributeValues": {":alert": {"S": "alert"}},
    }
    while True:
        resp = ddb.scan(**kwargs)
        for item in resp.get("Items", []):
            rows.append({
                "source_ip": item["source_ip"]["S"],
                "window_start": int(item["window_start"]["N"]),
                "window_count": int(item["window_count"]["N"]),
            })
        if "LastEvaluatedKey" not in resp:
            return rows
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]





def merge_views(min_failed=DEFAULT_MIN_FAILED, limit=1000):
    """Join the two views on source_ip and classify each offender.

    Categories, in descending order of how much they should worry an analyst:

      PRIORITY    in both  — a known repeat offender that is active right now
      SPIKING NEW alerting but absent from history — a source with no
                  reputation yet, which is exactly what a brand-new attacker
                  looks like, so it must not be dropped just because the batch
                  view has never seen it
      HISTORIC    in the batch view only — bad reputation, currently quiet
    """
    batch = batch_offenders(min_failed=min_failed, limit=limit)
    alerts = live_alerts()
    by_ip = {b["source_ip"]: b for b in batch}

    merged = []
    for b in batch:
        alert = alerts.get(b["source_ip"])
        merged.append({
            **b,
            "spiking_now": alert is not None,
            "window_count": alert["window_count"] if alert else None,
            "window_start": alert["window_start"] if alert else None,
            "category": "PRIORITY" if alert else "HISTORIC",
        })

    for ip, alert in alerts.items():
        if ip in by_ip:
            continue
        merged.append({
            "source_ip": ip,
            "failed": 0, "accepted": 0, "invalid_user": 0, "other": 0, "total": 0,
            "first_seen": None, "last_seen": None,
            "spiking_now": True,
            "window_count": alert["window_count"],
            "window_start": alert["window_start"],
            "category": "SPIKING NEW",
        })

    rank = {"PRIORITY": 0, "SPIKING NEW": 1, "HISTORIC": 2}
    merged.sort(key=lambda r: (rank[r["category"]],
                               -(r["window_count"] or 0),
                               -r["failed"],
                               r["source_ip"]))
    return merged


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--min-failed", type=int, default=DEFAULT_MIN_FAILED)
    ap.add_argument("--limit", type=int, default=100)
    ap.add_argument("--top", type=int, default=20, help="rows to print")
    args = ap.parse_args()

    started = time.perf_counter()
    merged = merge_views(min_failed=args.min_failed, limit=args.limit)
    elapsed = time.perf_counter() - started

    spiking = sum(1 for r in merged if r["spiking_now"])
    print(f"merged rows      : {len(merged)}")
    print(f"spiking right now: {spiking}")
    print(f"merge latency    : {elapsed:.2f}s\n")

    print(f"{'category':<12} {'source_ip':<18} {'failed':>8} {'window':>8}")
    print("-" * 50)
    for r in merged[:args.top]:
        window = r["window_count"] if r["window_count"] is not None else "-"
        print(f"{r['category']:<12} {r['source_ip']:<18} {r['failed']:>8} {window:>8}")

    if spiking == 0:
        print("\nNothing is spiking. The speed view is empty, which is expected "
              "between\nproducer runs — alert rows expire ~1 h after their window "
              "(ttl = window_start\n+ 3600). Start the producer to populate it.")


if __name__ == "__main__":
    main()
