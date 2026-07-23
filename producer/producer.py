"""
Minimal replay producer — Step 7 (M1 walking skeleton).

Reads the OpenSSH fixture, parses each line into the locked record schema
(see CONTRACTS.md), and puts it to Kinesis. Deliberately minimal: no rate
control, no batching. Those arrive in M2.
"""

import argparse
import json
import re
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


def parse_line(line):
    """OpenSSH log line -> record dict, or None if there's no usable IP."""
    line = line.rstrip("\r\n")          # fixture is CRLF; strip both
    if not line:
        return None

    header = RE_HEADER.match(line)
    if not header:
        return None
    host, message = header.group(1), header.group(2)

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
                    return None          # no IP -> not useful to us
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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", default="fixtures/OpenSSH_2k.log")
    ap.add_argument("--limit", type=int, default=10,
                    help="how many records to send (default 10)")
    ap.add_argument("--dry-run", action="store_true",
                    help="parse and print only; do not touch AWS")
    args = ap.parse_args()

    client = None if args.dry_run else boto3.client("kinesis", region_name=REGION)

    sent = 0
    with open(args.file, "r", encoding="utf-8", errors="replace") as fh:
        for line in fh:
            rec = parse_line(line)
            if rec is None:
                continue

            if args.dry_run:
                print(json.dumps(rec))
            else:
                client.put_record(
                    StreamName=STREAM_NAME,
                    Data=(json.dumps(rec) + "\n").encode("utf-8"),
                    PartitionKey=rec["source_ip"],      # locked: PK = source_ip
                )
                print(f"sent {rec['status']:<13} {rec['source_ip']}")

            sent += 1
            if sent >= args.limit:
                break

    print(f"\n{'parsed' if args.dry_run else 'sent'} {sent} records")


if __name__ == "__main__":
    main()