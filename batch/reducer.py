#!/usr/bin/env python3
"""
Hadoop Streaming reducer — batch layer MapReduce variant (M2).

Consumes the mapper's (source_ip, status) pairs and emits one row per IP with
the same columns as the Spark batch view and the reference oracle:

    source_ip <TAB> failed  accepted  invalid_user  other  total

Relies on the Streaming contract that input arrives sorted by key, so every
record for one IP is contiguous and the counts can be accumulated in constant
memory and flushed on key change. That is the whole reason MapReduce scales:
the reducer never holds more than one IP's counters at a time, no matter how
many IPs there are.

first_seen/last_seen are deliberately omitted. Carrying producer_ts through the
shuffle would double the intermediate data for two columns that the merge in M3
does not use, and the Spark view already provides them.

Local test without Hadoop:

    cat records.ndjson | ./mapper.py | sort | ./reducer.py
"""

import sys

STATUSES = ("failed", "accepted", "invalid_user", "other")


def emit(source_ip, counts):
    total = sum(counts.values())
    cols = "\t".join(str(counts[s]) for s in STATUSES)
    print(f"{source_ip}\t{cols}\t{total}")


def main():
    current_ip = None
    counts = {s: 0 for s in STATUSES}

    for line in sys.stdin:
        line = line.rstrip("\n")
        if not line:
            continue
        try:
            source_ip, status = line.split("\t", 1)
        except ValueError:
            print("reporter:counter:scp-siem,malformed_reducer_input,1", file=sys.stderr)
            continue

        if source_ip != current_ip:
            # Key change: the previous IP is complete, so flush it. Guarded on
            # `is not None` rather than truthiness so an empty key would still
            # be reported rather than silently swallowed.
            if current_ip is not None:
                emit(current_ip, counts)
            current_ip = source_ip
            counts = {s: 0 for s in STATUSES}

        if status in counts:
            counts[status] += 1
        else:
            # An unknown status would vanish from the columns while still
            # inflating `total`, which is a silent wrong answer.
            print("reporter:counter:scp-siem,unknown_status,1", file=sys.stderr)

    # The final key never sees a key change, so it needs an explicit flush.
    # Forgetting this is the classic Streaming reducer bug: you silently lose
    # exactly one IP, and it is the alphabetically last one.
    if current_ip is not None:
        emit(current_ip, counts)


if __name__ == "__main__":
    main()
