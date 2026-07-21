# Scalable Cloud Programming — CA Project Plan

**Module:** MSc Cloud Computing — Scalable Cloud Programming (NCI)
**Deliverable:** Python-based scalable real-time analytics system on a Lambda architecture (AWS)
**Due:** 4 August 2026, 17:00 · **Team size:** 2 · **Weighting:** 50% of module
**Primary dataset:** Loghub OpenSSH logs (locked) · **Last updated:** 18 Jul 2026

---

## 1. Project Overview

A **SIEM-lite real-time threat-detection pipeline**. We ingest a continuous stream of SSH
authentication events, detect brute-force / credential-stuffing behaviour as it happens (speed
layer), reconcile it against each source IP's full-history reputation (batch layer), and merge the
two into a live "priority offender" view (serving layer). The whole thing runs on AWS and scales
elastically under load.

This is a textbook fit for a **Lambda architecture** and doubles as a strong Cloud + Security
portfolio piece.

---

## 2. Real-Time Question (for the sign-up sheet)

> **"Which source IPs are exhibiting brute-force / credential-stuffing behaviour right now —
> exceeding an anomalous failed-authentication rate over a 5-minute sliding window — and how does
> that live signal reconcile against each IP's full-history reputation?"**

**Action:** Put this exact wording in the **Short Description** column of the sign-up sheet, and
**read every existing row first** to confirm no other team is answering the same question on the
same source. Marks are for the question + design, not the dataset.

---

## 3. Why a Lambda Architecture (the 5-mark justification)

- **Speed layer -> freshness.** We must catch an attack *while it is happening*; a windowed rate
  count over recent events gives low-latency detection.
- **Batch layer -> correctness.** Authoritative forensic attribution (all-time offender profiles,
  status distributions, first/last-seen) needs a complete, accurate pass over full history.
- **Serving layer -> merge.** The value is in combining them: "**confirmed repeat offender AND
  spiking right now**" is a higher-confidence signal than either view alone.

Batch-only would be too slow to catch live attacks; stream-only can't hold authoritative
full-history reputation. The split is the point.

---

## 4. Tech Stack

| Layer / Concern       | AWS Service / Tool                         | Role                                                            |
|-----------------------|--------------------------------------------|-----------------------------------------------------------------|
| Ingestion             | **Python replay producer (boto3)** -> **Kinesis Data Streams** | Replay stored OpenSSH log lines at a controlled rate to simulate a live stream |
| Landing (master data) | **Kinesis Firehose -> S3** (time-partitioned) | Raw, immutable full-history dataset for the batch layer         |
| Speed layer           | **AWS Lambda** + **DynamoDB** (window state, TTL) | Sliding-window failed-auth counting per IP; emit alerts         |
| Batch layer           | **PySpark on EMR** + **Hadoop Streaming MapReduce** variant | Full-history aggregates; MapReduce variant enables the seq-vs-parallel benchmark |
| Serving layer         | **S3 + Athena** (batch view) merged with **DynamoDB** (speed view) | The Lambda "merge" — combined priority view                     |
| Visualisation         | **Streamlit** dashboard                    | Live spiking IPs + merged offender table + alert time-series    |
| Auto-scaling          | **EMR managed scaling** (primary, configured policy) · Lambda native concurrency (automatic, by shard) | Elastic compute; EMR scaling is also exercised by the batch benchmark |
| Metrics / benchmarks  | **CloudWatch** + **boto3** -> CSV -> **matplotlib *or* a spreadsheet** (benchmark graphs only) | Repeatable figures straight into the report |

> **No pandas, no data cleaning, no ML.** The real processing is distributed (Spark / MapReduce /
> Lambda). matplotlib (or Sheets/Excel) is only a thin presentation layer for the *required*
> benchmark graphs. Line-parsing the logs is `str.split()` / one regex — not preprocessing.

> Distinction discriminators covered: **sliding-window** speed layer, **auto-scaling with stated
> triggers** (EMR managed scaling), **sequential-vs-parallel** batch benchmark + **speedup-vs-nodes**.

---

## 5. Data Source (LOCKED) & Record Contract

**Source:** **Loghub OpenSSH server log** — 655,146 events, 70 MiB uncompressed, spanning 28.4 days,
already full of real brute-force `Failed password ... from <IP>` lines. Clean, one event per line,
not sanitised/modified. Replayed at a controlled rate into Kinesis (brief-allowed, lecturer-confirmed
as a valid stream; a one-shot file read is not).

### 5.1 Getting the data

```bash
# 2k-line sample for local parser/producer dev (commit to repo under fixtures/)
curl -sL https://raw.githubusercontent.com/logpai/loghub/master/OpenSSH/OpenSSH_2k.log -o OpenSSH_2k.log

# full log for S3 master dataset + benchmarks
wget "https://zenodo.org/records/8196385/files/SSH.tar.gz?download=1" -O SSH.tar.gz
tar -xzf SSH.tar.gz && ls -lh          # extracted log is typically OpenSSH.log

# sanity checks (also a ground-truth reference for the Spark batch job)
wc -l OpenSSH.log
grep -c "Failed password" OpenSSH.log
grep "Failed password" OpenSSH.log | grep -oE 'from [0-9.]+' | awk '{print $2}' \
  | sort | uniq -c | sort -rn | head        # top offender IPs
```

**Citation (required in report references):** Jieming Zhu, Shilin He, Pinjia He, Jinyang Liu,
Michael R. Lyu. *Loghub: A Large Collection of System Log Datasets for AI-driven Log Analytics.*
ISSRE 2023 (arXiv:2008.06448). Repo: github.com/logpai/loghub

### 5.2 Record schema (LOCKED)

Producer parses each line into this JSON and pushes to Kinesis with **partition key = `source_ip`**
(so one IP's events land on the same shard — natural for per-IP windowing):

```json
{
  "producer_ts": "2026-07-24T14:03:22.481Z",   // set at EMIT time; drives latency benchmark
  "source_ip": "52.80.34.196",
  "status": "failed",                            // failed | accepted | invalid_user | other
  "user": "invalid user test",
  "port": 36034,
  "host": "LabSZ"
}
```

> **Windowing note:** the log lines carry Dec-2018 timestamps with no year. We window on
> **replay/arrival time (`producer_ts` / processing time)**, NOT the log's own timestamp. This keeps
> the sliding-window logic clean and is stated explicitly in the report.

---

## 6. Architecture Flow

```
 OpenSSH logs ──(paced replay, boto3)──► Kinesis Data Streams ──┬──► Lambda (speed layer)
 (Loghub, 655k events)                                          │      └► DynamoDB (5-min sliding-window state + alerts, TTL)
                                                                │
                                                                └──► Firehose ──► S3 (raw master dataset, time-partitioned)
                                                                                    └► PySpark on EMR (batch view) + MapReduce variant
                                                                                         └► batch aggregates back to S3

 Serving:  Athena (batch view over S3)  +  DynamoDB (speed view)  ──►  merged priority view  ──►  Streamlit dashboard

 Auto-scaling boundary:  EMR managed scaling (configured, stated triggers)  ·  Lambda concurrency (native, by shard)
```

---

## 7. Benchmark Plan (summary — detailed method finalised at M4)

**Experiment 1 — Batch, sequential vs parallel.** Hold input fixed, vary parallelism, measure
wall-clock runtime. Baselines: (a) plain single-process Python / single-node MapReduce; (b) Spark
on 1 vs N EMR core nodes across **1 / 3 / 5 / 7**. Median of 3 runs each, warm-up discarded, timed
driver-side (exclude cluster bootstrap). Plots: runtime vs nodes, speedup S(N)=T(1)/T(N) vs nodes
(with ideal line), efficiency S(N)/N. Analysis anchored on Amdahl (shuffle + startup are the ceiling).

**Experiment 2 — Speed-layer latency under load.** Hold code fixed, vary producer records/sec
(e.g. 100 / 500 / 1,000 / 2,500 / 5,000). Metrics: end-to-end latency p50/p95/p99 (via
`producer_ts`), Kinesis `GetRecords.IteratorAgeMilliseconds`, Lambda `Duration` /
`ConcurrentExecutions` / `Throttles` (CloudWatch, pulled via boto3). Plots: latency vs rate,
iterator age over time, throughput vs offered load. Expect the knee near the single-shard
1,000 rec/s limit; optionally add a shard and show recovery.

---

## 8. Milestone Tracker (comprehensive, dated)

Working model: **paired on scheduled work-time calls** (no member split — ownership is shared).
Target windows assume part-time evenings/weekends around study + work; slide them onto your actual
call calendar. **DoD** = "definition of done" for the whole milestone. Tick tasks as you go.

| Milestone | Focus | Target window |
|---|---|---|
| M0 | Foundation & contracts | Jul 18–20 |
| M1 | Walking skeleton (retire EMR risk) | Jul 21–23 |
| M2 | Build the two layers | Jul 24–28 |
| M3 | Serving merge + dashboard | Jul 29–30 |
| M4 | Auto-scaling + benchmarks | Jul 31 – Aug 1 |
| M5 | Report + video + submit | Aug 2–3 |
| — | **Buffer + submit** | **Aug 4 (by 17:00)** |

---

### M0 — Foundation & Contracts  ▢  (Jul 18–20)
**DoD:** both members can build independently; nothing downstream is blocked.

*Repo & admin*
- [ ] Create GitHub repo + folders: `producer/ speed/ batch/ serving/ dashboard/ benchmarks/ report/ infra-notes/ fixtures/`
- [ ] Commit this plan + a README (project summary, run instructions stub)
- [ ] Fill sign-up sheet row (team names + exact question in Short Description; check all rows for collisions)
- [ ] Send one-line heads-up email to lecturer (replayed OpenSSH logs as the source)

*AWS guardrails*
- [ ] Choose region (e.g. `eu-west-1` Ireland — low latency, cheap)
- [ ] Create least-privilege IAM users/roles
- [ ] **AWS Budgets alarm at $80** (+ early alert at $40); enable cost-allocation tags
- [ ] Agree naming/tagging convention (e.g. prefix `scp-siem-*`, `Project` tag on everything)

*Contracts (the critical bit — everything builds against these)*
- [ ] Finalise record schema (§5.2) and Kinesis partition key = `source_ip`
- [ ] S3 layout: `raw/` (Firehose) + `batch-views/`, time-partitioned `yyyy/mm/dd/hh/`
- [ ] DynamoDB speed-view table design (PK=`source_ip`, window-bucket attrs, `ttl` attribute)
- [ ] Draw architecture diagram (draw.io/Excalidraw) — reused for Phase 1 + report + video

*Data*
- [ ] Download 2k sample -> `fixtures/`; download full `OpenSSH.log` (kept out of git — it's 70 MB)
- [ ] Run the §5.1 sanity checks; note top-offender counts to ballpark the window threshold

---

### M1 — Walking Skeleton  ▢  (Jul 21–23)  *retire the risky unknowns early*
**DoD:** one record flows end-to-end on **both** paths; EMR bootstrap proven.

- [ ] Create Kinesis stream (1 shard)
- [ ] Minimal producer: read 2k sample, parse one line, `put_record` to Kinesis (fixed rate)
- [ ] Stub Lambda (event source mapping from Kinesis): log + write raw record to DynamoDB; confirm one record visible
- [ ] Firehose -> S3: create delivery stream; confirm objects landing in `raw/`
- [ ] **EMR de-risk:** launch small cluster (1 master + 1 core, m5.xlarge, spot core), run trivial PySpark "count rows from S3" job, confirm `spark-submit` works, **then tear the cluster down**
- [ ] Save the exact IAM roles/policies/networking that worked -> `infra-notes/`
- [ ] **Checkpoint:** if EMR isn't running by end of M1, pair on it hard — don't let it bleed into M2

---

### M2 — Build the Two Layers  ▢  (Jul 24–28)  *the biggest block*
**DoD:** real speed layer + real batch layer each work in isolation, validated against the §5.1 shell-command ground truth.

*Producer (harden)*
- [ ] Full line-parser: handle `Failed password`, `Accepted password`, `invalid user`, `message repeated N times`, pam `authentication failure` lines -> schema
- [ ] Rate control: configurable records/sec (token-bucket or paced sleep); add `producer_ts` at emit; CLI flags for rate + optional file loop (to sustain long benchmark runs)

*Speed layer (Lambda)*
- [ ] Sliding-window failed-auth count per `source_ip` over trailing 5 min
- [ ] Pick implementation: **bucketed** (30s buckets x10 — recommended, less state) vs exact (timestamp list, evict >5 min)
- [ ] DynamoDB state table with TTL; write window counts; emit alert when threshold crossed
- [ ] Handle Kinesis at-least-once semantics (idempotent updates)

*Batch layer (Spark on EMR)*
- [ ] PySpark job: per-IP failed/accepted counts, top-N offenders, status distribution, first/last-seen -> write batch view to S3 (parquet or csv), partitioned
- [ ] Hadoop Streaming **MapReduce** variant of the core per-IP count (`mapper.py` / `reducer.py`) — for the seq-vs-parallel benchmark + Lecture 5
- [ ] Validate Spark output against §5.1 ground truth (top offender IPs must match)
- [ ] Configure **EMR managed scaling** (min/max core units + stated trigger, e.g. `YARNMemoryAvailablePercentage` / pending-container ratio, + cooldown)

---

### M3 — Serving Merge + Dashboard  ▢  (Jul 29–30)
**DoD:** system behaves as one — merged priority view queryable and visualised.

- [ ] Athena: external table(s) over the S3 batch view; test queries
- [ ] Merge logic: batch reputation (Athena) + live speed view (DynamoDB) -> "confirmed repeat offender AND spiking now" list (simplest: dashboard-side join; alt: Athena over an exported DynamoDB snapshot)
- [ ] Streamlit dashboard: (1) spiking-now table (DynamoDB), (2) merged priority offenders, (3) alert-volume time-series, (4) all-time top offenders (batch view) — run locally for the demo
- [ ] End-to-end smoke test: producer running -> alerts appear -> dashboard updates live

---

### M4 — Auto-scaling + Benchmarks  ▢  (Jul 31 – Aug 1)
**DoD:** both experiments run; raw metrics captured as CSV; figures generated; analysis notes drafted.

*Experiment 1 — batch seq-vs-parallel*
- [ ] Fix input size (full 655k; replicate xN into a larger master dataset if runtimes are too short to measure cleanly)
- [ ] Runs: sequential Python / single-node MapReduce baseline; Spark on 1/3/5/7 core nodes; identical instance types; 3 runs each, warm-up discarded, median; time driver-side (Spark event log), exclude bootstrap
- [ ] Metrics -> CSV in `benchmarks/`; compute speedup S(N)=T(1)/T(N) and efficiency S(N)/N

*Experiment 2 — speed-layer latency under load*
- [ ] Ramp producer 100/500/1,000/2,500/5,000 rec/s; hold each at steady state a few minutes
- [ ] Capture end-to-end latency p50/p95/p99, `IteratorAgeMilliseconds`, Lambda `Duration`/`ConcurrentExecutions`/`Throttles`; pull via boto3 `get_metric_statistics` -> CSV
- [ ] Show the shard-saturation knee; optionally add a shard and show recovery

*Both*
- [ ] Generate figures (matplotlib or spreadsheet); keep raw results tables
- [ ] Draft critical-analysis notes (Amdahl ceiling; shard/concurrency limit) alongside the numbers
- [ ] **Tear everything down after the runs** (protect the budget)

---

### M5 — Report + Video + Submit  ▢  (Aug 2–3)
**DoD:** all artifacts submitted before the deadline, with buffer.

*Report (IEEE, 2-column, <= 10 pages)*
- [ ] Sections: intro/objectives; problem & use-case justification (5-mark); architecture + tools; implementation (batch/speed/serving); performance results + graphs; critical analysis; conclusion
- [ ] References incl. loghub citation (§5.1) + relevant lecture material
- [ ] Cross-review both — one voice, no errors

*Demo video*
- [ ] Walk the architecture diagram; show live pipeline (producer -> alerts -> dashboard); batch view; **auto-scaling in action** (CloudWatch graph); benchmark graphs
- [ ] Record only once the system is stable; upload (OneDrive/YouTube); link in report

*Submit*
- [ ] Clean GitHub repo (README + run instructions); link in report
- [ ] Upload report PDF to Moodle **before 4 Aug 17:00**
- [ ] Final teardown; confirm no lingering AWS resources still billing

---

## 9. Logistics & Guardrails

**Cost control ($80 ceiling — personal AWS account):**
- Tear down the **EMR cluster** whenever not actively benchmarking (biggest line item).
- **Never create a NAT gateway** — keep EMR/EC2 in a public subnet or use VPC endpoints.
- Kinesis at **1 shard** baseline; scale up only during benchmark runs.
- Spot instances for EMR core nodes. Budget alarm live from M0. Realistic total: **$30–50**.

**Submission checklist:** IEEE PDF report to Moodle · GitHub link in report · video link (OneDrive/YouTube) in report.

**Key risk:** first-time EMR bootstrap / spark-submit is the usual time-sink. The M1 walking
skeleton exists to kill that risk before it can touch the critical path.
