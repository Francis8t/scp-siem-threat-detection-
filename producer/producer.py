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