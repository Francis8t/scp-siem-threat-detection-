# Scalable Cloud Programming — CA Project Plan

**Module:** MSc Cloud Computing — Scalable Cloud Programming (NCI)
**Deliverable:** Python-based scalable real-time analytics system on a Lambda architecture (AWS)
**Due:** 4 August 2026, 17:00 · **Team size:** 2 · **Weighting:** 50% of module
**Primary dataset:** Loghub OpenSSH logs (locked) · **Region:** `us-east-1`
**Last updated:** 28 Jul 2026 (M2 in progress — producer + speed layer done)

> **Status at a glance:** M0 and M1 complete. M2 **producer hardening** and the **real speed
> layer** are done and verified live: the hardened producer (rate control + `put_records`
> batching + loop + Option-A skip) lands correct records in Kinesis, and the sliding-window
> Lambda fires exactly-one-per-IP-per-window alerts at threshold 50 — confirmed in DynamoDB
> against the three top-offender fixture IPs (183.62.140.253, 187.141.143.180, 103.99.0.122).
> **Next in M2: the batch layer — PySpark job + Hadoop Streaming MapReduce variant + EMR
> managed scaling.**

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

> **No Terraform / IaC.** Scrapped deliberately (see decision log D2) in favour of disciplined
> manual create/teardown via the AWS console.

> Distinction discriminators covered: **sliding-window** speed layer, **auto-scaling with stated
> triggers** (EMR managed scaling), **sequential-vs-parallel** batch benchmark + **speedup-vs-nodes**.

---

## 4b. Decision Log

Decisions already made, with the reasoning. **Do not re-open these without team agreement** —
they're recorded here so neither member (nor a fresh Claude session) relitigates settled ground.

| # | Decision | Rationale | Date |
|---|---|---|---|
| D1 | **Region = `us-east-1`** (changed from the originally-planned `eu-west-1`) | Team decision; all resources now live here. Check the console region selector every session. | 22 Jul |
| D2 | **No Terraform / IaC** | Not graded by the module rubric; the time cost isn't justified at this deadline. Replaced by disciplined manual start/stop + `infra-notes/` as the written register. | 20 Jul |
| D3 | **Shared AWS account**, teammate given an IAM user | Single budget/meter, single set of resources, no cross-account complexity. Requires coordination before create/delete. | 22 Jul |
| D4 | **Kinesis: provisioned, 1 shard** (not on-demand) | Cheaper, and the visible ~1,000 rec/s shard ceiling is exactly what Experiment 2 is designed to hit. On-demand would hide the knee. | 22 Jul |
| D5 | **DynamoDB: on-demand billing** | Bursty, unpredictable write pattern; effectively free at this volume; no capacity planning. | 22 Jul |
| D6 | **Bucketed windowing** (30s x 10) over exact timestamp lists | Far less state to hold; atomic `ADD` plays well with Kinesis at-least-once delivery. | 20 Jul |
| D7 | **Window on processing time (`producer_ts`)**, not the log's own timestamp | Log lines carry Dec-2018 timestamps with no year. Avoids year-inference hacks. Stated in the report. | 20 Jul |
| D8 | **`user` field = username only**; invalid-ness carried by `status` | One fact per field. Amends the original schema example. Consumers must tolerate null `user`/`port`. | 23 Jul |
| D9 | **2k fixture committed to git**; full 70 MB log excluded | Small, immutable test fixture — travels with the code that consumes it, keeps validation reproducible on clone. | 21 Jul |
| D10 | **`*.log -text` in `.gitattributes`** | Fixture is CRLF; preserving exact bytes avoids a silent parser corruption. | 21 Jul |
| D11 | **AWS console over CLI** for resource creation | Deliberate learning choice — click-through builds the mental model CLI one-liners hide. | 23 Jul |
| D12 | **Single canonical parser** (`producer/producer.py::parse_line`), reused by Spark + Lambda | Divergent parsers would let batch and speed layers silently disagree about the same event. | 23 Jul |
| D13 | **Newline-delimit Kinesis payloads** (`json.dumps(rec) + "\n"`) | Firehose concatenates records with no delimiter — without this, S3 objects are unparseable by Spark. Lambda is unaffected. | 23 Jul |
| D14 | **Lambda memory 256 MB** (up from 128 MB) | 128 MB ran at ~73% utilisation; Lambda scales CPU with memory, so this roughly halves `Duration`. Fixed *before* benchmarks so Experiment 2 numbers are stable. | 23 Jul |
| D15 | **`recursiveFileLookup=true` on every read of `raw/`** | Spark doesn't recurse into Firehose's nested `yyyy/MM/dd/HH/` dirs by default; without it, `UNABLE_TO_INFER_SCHEMA`. | 23 Jul |
| D16 | **EMR submitted via console Steps** (Deploy mode = Client) | Client mode puts driver stdout in the step log where it's readable. `command-runner.jar` is the fallback when console fields misbehave. | 23 Jul |
| D17 | **Option A — skip `message repeated N times` + PAM `authentication failure` lines** in the producer | Streaming project; underlying-data completeness is not the evaluation criterion. Repeat lines (~37k, each = N failures) and PAM lines (~231k, a duplicate view of the same failures + hostname `rhost=` values that violate the dotted-quad `source_ip` contract) are dropped. Accepted as a **deliberate, stated undercount** in the report. Emitting PAM would *double-count* every failure. | 28 Jul |
| D18 | **Alert threshold = 50 failed auths / IP / 5-min window** | Evidence-based, not a guess. Full-log analysis (skip applied) shows confirmed brute-forcers peak at **126–157** failures in a single 5-min window, tightly banded; legitimate users produce single digits. 50 sits ~10x above legitimate behaviour and below the attacker band, so zero false negatives on real threats and near-zero false positives. 50 (vs 100) chosen for a more sensitive, faster-firing live demo. | 28 Jul |
| D19 | **Alerts stored in the same DynamoDB state table**, distinguished by `kind="alert"` + a synthetic `bucket ≥ 10_000_000_000` (= `10e9 + window_start`); written via an **idempotent conditional put** | No second table, no extra IAM. Synthetic bucket sorts alert rows clear of state rows so they never fall inside a `[lo,hi]` window query. Conditional `attribute_not_exists` put = exactly one alert per IP per window, even under Kinesis at-least-once redelivery. Note: the *count* still double-counts on redelivery (D6 tradeoff); the *alert* does not. | 28 Jul |

---

## 5. Data Source (LOCKED) & Record Contract

**Source:** **Loghub OpenSSH server log** — 655,146 events, 70 MiB uncompressed, spanning 28.4 days,
already full of real brute-force `Failed password ... from <IP>` lines. Clean, one event per line,
not sanitised/modified. Replayed at a controlled rate into Kinesis (brief-allowed, lecturer-confirmed
as a valid stream; a one-shot file read is not).

### 5.1 Getting the data

```bash
# 2k-line sample for local parser/producer dev (committed at fixtures/OpenSSH_2k.log)
curl -sL https://raw.githubusercontent.com/logpai/loghub/master/OpenSSH/OpenSSH_2k.log -o OpenSSH_2k.log

# full log for S3 master dataset + benchmarks (kept OUT of git)
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

### 5.2 Record schema (LOCKED — full detail in `CONTRACTS.md`)

Producer parses each line into this JSON and pushes to Kinesis with **partition key = `source_ip`**
(so one IP's events land on the same shard — natural for per-IP windowing):

```json
{
  "producer_ts": "2026-07-24T14:03:22.481Z",
  "source_ip": "52.80.34.196",
  "status": "failed",
  "user": "test",
  "port": 36034,
  "host": "LabSZ"
}
```

- `producer_ts` set at EMIT time; drives the latency benchmark.
- `status` enum: `failed` | `accepted` | `invalid_user` | `other`.
- `user` is the **username only** (D8) — invalid-ness lives in `status`.
- **`user` and `port` are null on ~55% of records** (the `other` class). Consumers must tolerate it.

> **Windowing note:** the log lines carry Dec-2018 timestamps with no year. We window on
> **replay/arrival time (`producer_ts` / processing time)**, NOT the log's own timestamp. This keeps
> the sliding-window logic clean and is stated explicitly in the report.

### 5.3 Validated ground truth — 2k fixture

The producer's parser reproduces these exactly; use as the regression anchor for the Spark job.

| Metric | Value |
|---|---|
| Total lines | 1,999 |
| `failed` | **520** |
| `invalid_user` | 113 |
| `accepted` | 1 |
| `other` | 1,100 |
| Dropped (no IP) | 265 |

Top failed-auth IPs: `183.62.140.253` (286) · `187.141.143.180` (80) · `103.99.0.122` (46)

> **Updated under Option-A skip (D17):** the numbers above are the original M1 parser (and
> `grep -c`, which also counts the skipped repeat lines). With the skip applied, the fixture
> `failed` count is **518** (2 embedded-repeat failures dropped), `other` is **601**, and dropped
> is **767**. The batch job should reconcile to the *skip-applied* numbers, not the raw grep.

### 5.4 Full-log ground truth (skip applied) — the alert-threshold basis

Computed with the canonical `parse_line()` + Option-A skip over the full 655,146-line `SSH.log`:

| Metric | Value |
|---|---|
| Total lines | 655,146 |
| `failed` events (skip applied) | **160,616** |
| Distinct (IP, 5-min window) pairs | 7,026 |
| **Worst single 5-min window** | **157** (`183.63.110.206`, a Jan-03 burst) |
| Top-IP peak-window band | **126–157** (tightly clustered) |

Raw `grep -c "Failed password"` on the full log returns **197,587** — higher than 160,616
because grep counts the ~37k `message repeated` lines the producer skips. This gap is expected
and is the D17 undercount, stated in the report.

> The 126–157 band is the justification for the **threshold = 50** decision (D18): well below any
> real attacker's peak (no false negatives), well above any legitimate user's 5-min failures
> (near-zero false positives).

**Data-quality note for the report:** the busiest windows are nearly all `183.63.110.206` on
"Jan 03". The log nominally spans Dec 2018 but crosses into Jan 2019 with **no year field** — the
exact reason we window on `producer_ts`, not the log clock (D7). The threshold analysis pins a
nominal year only to get *relative* 5-min bucketing; absolute dates are irrelevant to the max-burst
figure.

---

## 6. Architecture Flow

Rendered diagram: **`report/architecture.svg`** (reused in the report + demo video).

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
**DoD** = "definition of done" for the whole milestone.

| Milestone | Focus | Target window | Status |
|---|---|---|---|
| M0 | Foundation & contracts | Jul 18–20 | ✅ **Complete** |
| M1 | Walking skeleton (retire EMR risk) | Jul 21–23 | ✅ **Complete** |
| M2 | Build the two layers | Jul 24–28 | 🔄 **Next** |
| M3 | Serving merge + dashboard | Jul 29–30 | ⬜ Not started |
| M4 | Auto-scaling + benchmarks | Jul 31 – Aug 1 | ⬜ Not started |
| M5 | Report + video + submit | Aug 2–3 | ⬜ Not started |
| — | **Buffer + submit** | **Aug 4 (by 17:00)** | — |

---

### M0 — Foundation & Contracts  ✅ **COMPLETE**  (Jul 18–20)
**DoD:** both members can build independently; nothing downstream is blocked. — *met*

*Repo & admin*
- [x] Create GitHub repo + folders: `producer/ speed/ batch/ serving/ dashboard/ benchmarks/ report/ infra-notes/ fixtures/`
- [x] Commit this plan + a README (project summary, run instructions stub)
- [x] Teammate added as GitHub collaborator; `TEAMMATE_ONBOARDING.md` written for context handoff
- [ ] Fill sign-up sheet row (team names + exact question in Short Description; check all rows for collisions) — **STILL OPEN**
- [ ] Send one-line heads-up email to lecturer (replayed OpenSSH logs as the source) — **STILL OPEN**

*AWS guardrails*
- [x] Region chosen: **`us-east-1`** (D1)
- [x] Shared AWS account; IAM user created and shared with teammate (D3)
- [x] **AWS Budget live:** `scp-siem-monthly`, $80 ceiling, email alerts at 50% ($40), 100% ($80), forecast-over-100%
- [x] Naming/tagging convention agreed: prefix `scp-siem-*`, tag `Project=scp-siem` on everything

*Contracts*
- [x] Record schema finalised + Kinesis partition key = `source_ip` (`CONTRACTS.md`)
- [x] S3 layout: `raw/` + `batch-views/` + `athena-results/`, time-partitioned `yyyy/mm/dd/hh/`
- [x] DynamoDB speed-view table design (PK=`source_ip`, SK=`bucket`, `ttl` attribute)
- [x] Architecture diagram -> `report/architecture.svg`

*Data*
- [x] 2k sample committed -> `fixtures/OpenSSH_2k.log` (D9)
- [x] Ground-truth counts captured for the 2k fixture (§5.3)
- [ ] Download full `OpenSSH.log` + run §5.1 sanity checks on it — **carried into M2**

---

### M1 — Walking Skeleton  ✅ **COMPLETE**  (Jul 21–23)  *risky unknowns retired*
**DoD:** one record flows end-to-end on **both** paths; EMR bootstrap proven. — *met, on schedule*

- [x] Create Kinesis stream `scp-siem-stream` (1 shard, provisioned) — ACTIVE
- [x] Create DynamoDB table `scp-siem-speed-state` (on-demand, TTL on `ttl`) — ACTIVE
- [x] Minimal producer: parses fixture -> locked schema, `put_record` to Kinesis
- [x] Parser validated against shell ground truth (520 failed = `grep -c`; top-3 IPs match)
- [x] Producer run against live Kinesis; records confirmed in the Data viewer
- [x] Speed Lambda `scp-siem-speed`: Kinesis event source mapping -> 30s-bucketed counts in DynamoDB
- [x] Lambda output verified (5 IPs / 15 failed for `--limit 50`) — **exact match to fixture**
- [x] Lambda memory raised to 256 MB (D14)
- [x] S3 bucket `scp-siem-data-009910375264` created
- [x] Firehose `scp-siem-firehose` -> S3 `raw/`; objects confirmed, newline-delimited (D13)
- [x] **EMR de-risk PASSED:** emr-7.13.0 / Spark 3.5.6, 1 primary + 1 core m5.xlarge, `spark-submit` via console Step, read `s3://.../raw/`, groupBy shuffle completed
- [x] EMR output reconciled exactly against fixture (200 records: 119 other / 62 failed / 19 invalid_user; top-3 IPs match)
- [x] Cluster terminated
- [x] IAM roles/policies + the four EMR failure modes documented -> `infra-notes/resources.md`

**Round-trip integrity proven:** producer → Kinesis → Firehose → S3 → Spark preserves every
record and field with zero loss or corruption. The `CONTRACTS.md` schema survives intact.

**Cost so far:** well inside budget. EMR de-risk run ≈ $0.35.

---

### M2 — Build the Two Layers  ⬜  (Jul 24–28)  *the biggest block*
**DoD:** real speed layer + real batch layer each work in isolation, validated against the §5.1 shell-command ground truth.

*Producer (harden)* — ✅ **DONE (28 Jul), verified live in Kinesis**
- [x] Option A (D17): explicitly **skip** `message repeated N times` + PAM `authentication failure` lines via `RE_SKIP` (rather than let them fall through to `status:other`)
- [x] Rate control: `--rate` records/sec (paced per 1s window); `--loop` to repeat the file for sustained benchmark runs
- [x] `put_records` batching (`--batch-size`, capped at Kinesis' 500) so the 2.5k–5k rec/s tiers are actually reachable; failed-record retry-once
- [x] `--limit 0` = unlimited (M1 default of 10 was a benchmarking footgun)
- [x] Parser re-validated against the 2k fixture under skip: failed 520→**518**, other 1,100→**601**, dropped 265→**767** (see CONTRACTS §3)
- [x] Live send confirmed: records decoded from the shard show correct schema, nulls, `\n` delimiter, and `source_ip` partition key

*Speed layer (Lambda)* — ✅ **DONE (28 Jul), alerts verified in DynamoDB**
- [x] Trailing-10-bucket (5-min) window sum per `source_ip` via a bounded `Query` over `[newest-270s, newest]`
- [x] DynamoDB state writes with TTL (unchanged from M1); alert emitted when window sum ≥ threshold
- [x] **Alert threshold set to 50** off full-log ground truth (D18; attacker band 126–157)
- [x] Kinesis at-least-once handled: atomic `ADD` for counts (D6) + **idempotent conditional-put alert** (D19) → exactly one alert per IP per window under redelivery
- [x] Locally tested with `moto` (mock DynamoDB): window sum, threshold crossing, status filtering, and redelivery-idempotency all pass before deploy
- [x] IAM role widened: added `dynamodb:Query` + `dynamodb:PutItem` (M1 had only `UpdateItem`) — record in `infra-notes/resources.md`

*Batch layer (Spark on EMR)* — ⬅ **NEXT, the remaining M2 block**
- [ ] PySpark job: per-IP failed/accepted counts, top-N offenders, status distribution, first/last-seen -> batch view to S3 (parquet or csv), partitioned
- [ ] Hadoop Streaming **MapReduce** variant of the core per-IP count (`mapper.py` / `reducer.py`)
- [ ] **Reuse `parse_line()`** rather than rewriting the parser (D12)
- [ ] Validate Spark output against §5.1 / §5.3 ground truth (top offender IPs must match)
- [ ] Configure **EMR managed scaling** (min/max core units + stated trigger + cooldown)

> **Build strategy for the batch layer:** develop the PySpark job against the local `SSH.log`
> first (fast, no EMR cost), reconcile counts to the shell ground truth, *then* switch the read
> path to `s3://<bucket>/raw/` with `recursiveFileLookup=true` (D15). Don't debug Spark logic and
> EMR config at the same time.

---

### M3 — Serving Merge + Dashboard  ⬜  (Jul 29–30)
**DoD:** system behaves as one — merged priority view queryable and visualised.

- [ ] Athena: external table(s) over the S3 batch view; test queries
- [ ] Merge logic: batch reputation (Athena) + live speed view (DynamoDB) -> "confirmed repeat offender AND spiking now" list (simplest: dashboard-side join; alt: Athena over an exported DynamoDB snapshot)
- [ ] Streamlit dashboard: (1) spiking-now table (DynamoDB), (2) merged priority offenders, (3) alert-volume time-series, (4) all-time top offenders (batch view) — run locally for the demo
- [ ] End-to-end smoke test: producer running -> alerts appear -> dashboard updates live

---

### M4 — Auto-scaling + Benchmarks  ⬜  (Jul 31 – Aug 1)
**DoD:** both experiments run; raw metrics captured as CSV; figures generated; analysis notes drafted.

*Experiment 1 — batch seq-vs-parallel*
- [ ] Fix input size (full 655k; replicate xN into a larger master dataset if runtimes are too short to measure cleanly)
- [ ] Runs: sequential Python / single-node MapReduce baseline; Spark on 1/3/5/7 core nodes; identical instance types; 3 runs each, warm-up discarded, median; time driver-side, exclude bootstrap
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

### M5 — Report + Video + Submit  ⬜  (Aug 2–3)
**DoD:** all artifacts submitted before the deadline, with buffer.

*Report (IEEE, 2-column, <= 10 pages)*
- [ ] Sections: intro/objectives; problem & use-case justification (5-mark); architecture + tools; implementation (batch/speed/serving); performance results + graphs; critical analysis; conclusion
- [ ] Embed `report/architecture.svg`
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

**Cost control ($80 ceiling — shared personal AWS account):**
- Budget alarm **live** since M0: `scp-siem-monthly`, alerts at $40 / $80 / forecast-over-$80.
- Tear down the **EMR cluster** whenever not actively benchmarking (biggest line item).
- **Never create a NAT gateway** — keep EMR/EC2 in a public subnet or use VPC endpoints.
- Kinesis at **1 shard** baseline; scale up only during benchmark runs, then back down.
- Spot instances for EMR core nodes. Realistic total: **$30–50**.
- Kinesis (~$0.36/day) + DynamoDB (on-demand) are cheap enough to leave running M1–M4.

**Shared-account coordination (D3):** resource names are unique per account+region — check
`infra-notes/resources.md` before creating anything, never tear down what you didn't create
(especially EMR mid-run), and prefix experiments with your initials.

**Live resources:** see `infra-notes/resources.md` — the register of what exists.

**Submission checklist:** IEEE PDF report to Moodle · GitHub link in report · video link (OneDrive/YouTube) in report.

**Key risk:** first-time EMR bootstrap / spark-submit is the usual time-sink. The M1 walking
skeleton exists to kill that risk before it can touch the critical path.

**Key risk — RETIRED (23 Jul).** EMR bootstrap and `spark-submit` are proven working, and the
four failure modes hit during the de-risk are documented with fixes in
`infra-notes/resources.md` §4. Cluster relaunch at M2/M4 should be minutes, not hours.

**Current top risk:** M2 is the largest block (speed layer + batch layer + MapReduce variant +
managed scaling) in a 5-day window. The MapReduce variant and EMR managed-scaling config are the
two items most likely to be squeezed — and both are distinction discriminators. Do not let them
slide into M4.
