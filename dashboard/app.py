"""
Streamlit dashboard — serving layer (M3).

Visualises the merged priority view: full-history reputation from Athena over
the parquet batch view, joined against live alert rows from DynamoDB (D25).

Four panels, per PROJECT_PLAN.md M3:
  1. spiking now          — the speed view alone, what is happening this minute
  2. merged priority      — the point of the whole architecture
  3. alert volume         — alerts over time, from the alert rows' windows
  4. all-time offenders   — the batch view alone, reputation without recency

Run locally for the demo (there is no hosted deployment):

    streamlit run dashboard/app.py

TWO THINGS TO KNOW BEFORE DEMOING

1. The speed view is EMPTY unless a producer is running or ran within the last
   hour. Alert rows carry ttl = window_start + 3600 and state rows only 6
   minutes, so between runs every IP correctly reports as batch-only. Start the
   producer first, then this:

       python3 producer/producer.py --file SSH.log --rate 1000

2. Batch data is cached for 60 s and live data for 3 s. The batch view only
   changes when the Spark job reruns, so re-querying Athena on every refresh
   would just add latency and cost ($5/TB scanned) for an identical answer.

No pandas: Streamlit renders lists of dicts directly.
"""

import os
import sys
from collections import Counter
from datetime import datetime, timezone

import streamlit as st

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "serving"))
import merge  # noqa: E402

st.set_page_config(page_title="scp-siem — SSH threat detection",
                   page_icon="🛡️", layout="wide")

# --- controls ---------------------------------------------------------------
st.sidebar.header("Controls")
min_failed = st.sidebar.slider(
    "Batch reputation threshold (failed auths, all time)",
    min_value=10, max_value=2000, value=merge.DEFAULT_MIN_FAILED, step=10,
    help="An IP needs at least this many all-time failures to count as a "
         "repeat offender. Defaults to the speed layer's alert threshold (D18) "
         "so both halves of the merge judge on the same basis.")
top_n = st.sidebar.slider("Rows per table", 5, 50, 15)
refresh_seconds = st.sidebar.select_slider(
    "Live refresh", options=[3, 5, 10, 30, 60], value=5,
    help="Only the live panels refresh on this interval; the batch view is "
         "cached for 60 s.")
st.sidebar.caption(
    f"Alert threshold (speed layer): **{merge.DEFAULT_MIN_FAILED}** failed "
    "auths per IP per 5-minute sliding window.")


# --- data access ------------------------------------------------------------
@st.cache_data(ttl=60, show_spinner=False)
def get_batch(min_failed_value):
    """Athena side. Cached: the batch view only changes when Spark reruns."""
    return merge.batch_offenders(min_failed=min_failed_value, limit=1000)


@st.cache_data(ttl=3, show_spinner=False)
def get_live():
    """DynamoDB side. Short TTL so rapid refreshes don't hammer the table."""
    return merge.live_alerts(), merge.alert_history()


def classify(batch_rows, alerts):
    """Same logic as merge.merge_views, over already-fetched data.

    Reimplemented here rather than calling merge_views() so the two halves can
    be cached on different clocks — the batch side is expensive and stable, the
    live side is cheap and volatile.
    """
    by_ip = {b["source_ip"]: b for b in batch_rows}
    merged = []
    for b in batch_rows:
        alert = alerts.get(b["source_ip"])
        merged.append({**b,
                       "category": "PRIORITY" if alert else "HISTORIC",
                       "window_count": alert["window_count"] if alert else None})
    for ip, alert in alerts.items():
        if ip not in by_ip:
            merged.append({"source_ip": ip, "failed": 0, "accepted": 0,
                           "invalid_user": 0, "other": 0, "total": 0,
                           "first_seen": None, "last_seen": None,
                           "category": "SPIKING NEW",
                           "window_count": alert["window_count"]})
    rank = {"PRIORITY": 0, "SPIKING NEW": 1, "HISTORIC": 2}
    merged.sort(key=lambda r: (rank[r["category"]], -(r["window_count"] or 0),
                               -r["failed"], r["source_ip"]))
    return merged


def hhmm(epoch_seconds):
    return datetime.fromtimestamp(epoch_seconds, tz=timezone.utc).strftime("%H:%M")


# --- header -----------------------------------------------------------------
st.title("🛡️ scp-siem — real-time SSH threat detection")
st.caption(
    "Which source IPs are exhibiting brute-force behaviour **right now** — "
    "exceeding an anomalous failed-authentication rate over a 5-minute sliding "
    "window — and how does that reconcile against each IP's full-history "
    "reputation?")


@st.fragment(run_every=refresh_seconds)
def live_view():
    alerts, history = get_live()
    batch_rows = get_batch(min_failed)
    merged = classify(batch_rows, alerts)

    priority = [r for r in merged if r["category"] == "PRIORITY"]
    spiking_new = [r for r in merged if r["category"] == "SPIKING NEW"]

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Spiking now", len(alerts))
    c2.metric("Priority offenders", len(priority),
              help="Bad history AND active right now.")
    c3.metric("New attackers", len(spiking_new),
              help="Alerting but absent from the batch view — no reputation yet.")
    c4.metric("Repeat offenders (all time)", len(batch_rows))

    if not alerts:
        st.info(
            "**Nothing is spiking.** The speed view is empty, which is normal "
            "between runs — alert rows expire about an hour after their window. "
            "Start the producer to populate it:  "
            "`python3 producer/producer.py --file SSH.log --rate 1000`")

    # --- panel 2: the merged priority view (the centrepiece) ----------------
    st.subheader("Priority offenders — confirmed repeat offender AND spiking now")
    st.caption(
        "The merge of both layers. PRIORITY outranks a quieter IP with a worse "
        "all-time record, because recency is what makes a signal actionable.")
    if priority or spiking_new:
        st.dataframe(
            [{"category": r["category"], "source_ip": r["source_ip"],
              "window count (5 min)": r["window_count"],
              "failed (all time)": r["failed"],
              "total events (all time)": r["total"],
              "first seen": r["first_seen"], "last seen": r["last_seen"]}
             for r in (priority + spiking_new)[:top_n]],
            width="stretch", hide_index=True)
    else:
        st.caption("_No IP is currently both a repeat offender and active._")

    col_left, col_right = st.columns(2)

    # --- panel 1: speed view alone ------------------------------------------
    with col_left:
        st.subheader("Spiking now — speed layer")
        st.caption("Live from DynamoDB. Sliding 5-minute window, threshold "
                   f"{merge.DEFAULT_MIN_FAILED}.")
        if alerts:
            st.dataframe(
                [{"source_ip": a["source_ip"],
                  "window count": a["window_count"],
                  "windows alerted": a["alert_rows"],
                  "window start (UTC)": hhmm(a["window_start"])}
                 for a in sorted(alerts.values(),
                                 key=lambda a: -a["window_count"])[:top_n]],
                width="stretch", hide_index=True)
        else:
            st.caption("_Speed view empty._")

    # --- panel 3: alert volume over time ------------------------------------
    with col_right:
        st.subheader("Alert volume")
        st.caption("Alert rows per minute. Each row is one IP crossing the "
                   "threshold in one 30-second sliding window.")
        if history:
            per_minute = Counter(hhmm(r["window_start"] - r["window_start"] % 60)
                                 for r in history)
            labels = sorted(per_minute)
            st.bar_chart({"minute (UTC)": labels,
                          "alerts": [per_minute[m] for m in labels]},
                         x="minute (UTC)", y="alerts", height=300)
        else:
            st.caption("_No alerts in the retention window._")

    st.caption(f"Live panels refreshed {datetime.now().strftime('%H:%M:%S')} · "
               f"batch view cached up to 60 s")


live_view()

# --- panel 4: batch view alone (outside the fragment; it rarely changes) -----
st.subheader("All-time top offenders — batch layer")
st.caption("Full-history reputation from Athena over the Spark batch view. "
           "Authoritative and complete, but says nothing about right now.")
batch_rows = get_batch(min_failed)
st.dataframe(
    [{"source_ip": b["source_ip"], "failed": b["failed"],
      "invalid_user": b["invalid_user"], "accepted": b["accepted"],
      "other": b["other"], "total": b["total"]}
     for b in batch_rows[:top_n]],
    width="stretch", hide_index=True)

with st.expander("How this works"):
    st.markdown(
        """
**Speed layer** — the producer replays OpenSSH events into Kinesis; a Lambda
counts failed authentications per IP into 30-second buckets in DynamoDB, sums
the trailing ten buckets for a 5-minute sliding window, and writes one alert
row per IP per window once the count crosses the threshold.

**Batch layer** — Firehose lands every record in S3 as the immutable master
dataset. A PySpark job on EMR aggregates full history into a parquet batch
view, which Athena queries. A Hadoop Streaming MapReduce variant and a
single-process Python reference produce identical numbers.

**The merge** — Athena ranks and filters the batch side in SQL, DynamoDB
supplies live alerts, and this dashboard joins the two small result sets on
`source_ip`. Neither view alone answers the question: batch is authoritative
but stale, speed is fresh but has no memory.
        """)
