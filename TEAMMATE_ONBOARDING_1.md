# Project Context Handoff — Scalable Cloud Programming CA

**Purpose of this file:** bring a fresh Claude session (or a human) fully up to speed on this
project — what it is, what's been decided and *why*, what's already built, and what happens next.
It is self-contained: everything needed to contribute is below.

**Last updated:** 23 July 2026 (M1 in progress) · **Deadline:** 4 August 2026, 17:00

---

## 0. How to use this file with Claude

Start a new Claude conversation, attach this file plus `PROJECT_PLAN.md` and `CONTRACTS.md`
from the repo, and paste something like:

> I'm working on an MSc Scalable Cloud Programming CA project with a teammate. The attached
> files describe the project, the decisions already locked, and how far we've got. Read them,
> then act as my guide for the remaining steps. Work with me **one step at a time** — give me
> a single step, wait for me to confirm it worked, then move to the next. Prefer **AWS console
> click-through instructions over CLI commands**, because I'm using this project to learn.
> Don't re-open decisions marked LOCKED unless I ask.

That last paragraph matters — it's the exact working cadence the other half of the team is
using, so both sides stay in sync.

---

## 1. What the project is

**Module:** MSc Cloud Computing — Scalable Cloud Programming (NCI)
**Deliverable:** a Python-based scalable real-time analytics system on a **Lambda architecture**, on AWS
**Weighting:** 50% of the module · **Team size:** 2 · **Ambition level:** distinction

A **SIEM-lite real-time threat-detection pipeline**. We replay a stream of SSH authentication
events, detect brute-force / credential-stuffing behaviour as it happens (speed layer),
reconcile it against each source IP's full-history reputation (batch layer), and merge the two
into a live "priority offender" view (serving layer).

### The locked real-time question

> **"Which source IPs are exhibiting brute-force / credential-stuffing behaviour right now —
> exceeding an anomalous failed-authentication rate over a 5-minute sliding window — and how
> does that live signal reconcile against each IP's full-history reputation?"**

This exact wording goes in the sign-up sheet's Short Description column.

### Why a Lambda architecture (this is the 5-mark justification in the report)

- **Speed layer → freshness.** Catch the attack *while it's happening*; a windowed rate count
  over recent events gives low-latency detection.
- **Batch layer → correctness.** Authoritative forensic attribution (all-time offender
  profiles, status distributions, first/last-seen) needs a complete, accurate pass over history.
- **Serving layer → merge.** The value is the combination: "**confirmed repeat offender AND
  spiking right now**" is a higher-confidence signal than either view alone.

Batch-only is too slow to catch live attacks; stream-only can't hold authoritative full-history
reputation. **The split is the point** — that's the argument the report leans on.

---

## 2. Tech stack (LOCKED)

| Layer / Concern | Service / Tool | Role |
|---|---|---|
| Ingestion | Python replay producer (boto3) → **Kinesis Data Streams** | Replay stored OpenSSH lines at a controlled rate |
| Landing | **Kinesis Firehose → S3** (time-partitioned) | Raw immutable master dataset for batch |
| Speed layer | **AWS Lambda** + **DynamoDB** (window state, TTL) | Sliding-window failed-auth count per IP; alerts |
| Batch layer | **PySpark on EMR** + **Hadoop Streaming MapReduce** variant | Full-history aggregates; MapReduce enables the seq-vs-parallel benchmark |
| Serving layer | **S3 + Athena** (batch view) ⋈ **DynamoDB** (speed view) | The merged priority view |
| Visualisation | **Streamlit** dashboard | Live spiking IPs, merged offenders, alert time-series |
| Auto-scaling | **EMR managed scaling** (configured policy) · Lambda concurrency (native, per shard) | Elastic compute |
| Benchmarks | **CloudWatch** + boto3 → CSV → **matplotlib** | Repeatable figures for the report |

**Deliberately excluded — do not reintroduce:**
- **No Terraform / IaC.** Scrapped in favour of disciplined manual start/stop. Rationale: the
  module isn't graded on IaC, and the time cost isn't worth it at this deadline.
- **No pandas.** It's data-science-adjacent and out of scope — the *real* processing must be
  distributed (Spark / MapReduce / Lambda). Line-parsing is `str.split()` + one regex, which is
  parsing, not preprocessing.
- **No ML, no data cleaning.**
- **matplotlib is a thin presentation layer only**, for the required benchmark graphs. A
  spreadsheet would be an equally valid substitute.

---

## 3. Dataset (LOCKED)

**Loghub OpenSSH server log** — 655,146 events, ~70 MiB, spanning 28.4 days, full of real
`Failed password ... from <IP>` brute-force lines. Replayed at a controlled rate into Kinesis
(paced replay is brief-allowed and lecturer-confirmed as a valid stream; a one-shot file read
is **not**).

```bash
# 2k sample for local dev — already committed at fixtures/OpenSSH_2k.log
curl -sL https://raw.githubusercontent.com/logpai/loghub/master/OpenSSH/OpenSSH_2k.log -o OpenSSH_2k.log

# full log for S3 master dataset + benchmarks (kept OUT of git — 70 MB)
wget "https://zenodo.org/records/8196385/files/SSH.tar.gz?download=1" -O SSH.tar.gz
tar -xzf SSH.tar.gz

# ground-truth sanity checks — the Spark output must match these
wc -l OpenSSH.log
grep -c "Failed password" OpenSSH.log
grep "Failed password" OpenSSH.log | grep -oE 'from [0-9.]+' | awk '{print $2}' \
  | sort | uniq -c | sort -rn | head
```

A sample line, for reference:

```
Dec 10 06:55:48 LabSZ sshd[24200]: Failed password for invalid user webmaster from 173.234.31.186 port 38926 ssh2
```

**Required citation in the report:** Zhu, He, He, Liu, Lyu. *Loghub: A Large Collection of
System Log Datasets for AI-driven Log Analytics.* ISSRE 2023 (arXiv:2008.06448).
Repo: github.com/logpai/loghub

---

## 4. Shared contracts (LOCKED — see `CONTRACTS.md` in the repo)

Everything downstream builds against these. Changing one means changing the producer, the
speed layer, the batch job, *and* the architecture diagram — so change only by team agreement.

### 4.1 Record schema (the Kinesis payload)

```json
{
  "producer_ts": "2026-07-24T14:03:22.481Z",
  "source_ip":   "52.80.34.196",
  "status":      "failed",
  "user":        "test",
  "port":        36034,
  "host":        "LabSZ"
}
```

- `producer_ts` — ISO-8601 UTC, ms precision, set at **emit** time. Drives the latency benchmark.
- `status` — enum: `failed` | `accepted` | `invalid_user` | `other`.
- `user` — **username only**, no "invalid user" prefix. Invalid-ness is carried by `status`,
  not duplicated into this field. One fact per field.
- `user` and `port` are **null** whenever the line has no username/port. This is the common
  case, not an edge case — roughly **1,100 of the 1,999** fixture lines are `status: "other"`
  with both null. Tolerate nulls everywhere or your consumer throws on most records.
- **Kinesis partition key = `source_ip`.** All of one IP's events land on the same shard, so
  per-IP window counting needs no cross-shard coordination.

### 4.2 Two decisions worth understanding before you touch the code

- **We window on processing time (`producer_ts`), NOT the log's own timestamp.** The raw lines
  carry Dec-2018 timestamps with no year. Windowing on replay/arrival time keeps the
  sliding-window logic clean. This is stated explicitly in the report.
- **The fixture has CRLF line endings.** `.gitattributes` sets `*.log -text` so git doesn't
  rewrite them. Strip `\r` when parsing — it's a known gotcha that will silently corrupt the
  last field of every parsed line if missed.

### 4.3 The parser — reuse it, don't rewrite it

The canonical parser is `parse_line()` in `producer/producer.py`. **The Spark job and the Lambda
must import/copy this same function rather than writing their own.** Divergent parsers mean the
batch and speed layers silently disagree about what happened — the worst class of bug here.

Two irregularities already found and handled:

- **CRLF endings.** The fixture is CRLF; parsers must `rstrip("\r\n")`. `.gitattributes` sets
  `*.log -text` so git preserves the bytes.
- **Irregular whitespace.** One fixture line reads `Failed password for invalid user  0101 from ...`
  with a **double space**. A regex assuming single spaces misclassifies it and the failed-count
  comes out 519 instead of 520. All patterns use `\s+`. Expect more of this in the full 655k log —
  always validate against the shell ground truth rather than trusting the parser.

### 4.4 Validated ground truth — 2k fixture

The parser reproduces these exactly. **Use as the regression anchor for the Spark batch job.**

| Metric | Value |
|---|---|
| Total lines | 1,999 |
| `failed` | **520** |
| `invalid_user` | 113 |
| `accepted` | 1 |
| `other` | 1,100 |
| Dropped (no IP) | 265 |

Top failed-auth IPs: `183.62.140.253` (286) · `187.141.143.180` (80) · `103.99.0.122` (46)

Cross-check: `grep -c "Failed password" fixtures/OpenSSH_2k.log`

### 4.5 S3 layout

Bucket `scp-siem-data-<ACCOUNT_ID>`:

```
raw/yyyy/mm/dd/hh/     <- Firehose landing, immutable master dataset
batch-views/           <- Spark aggregates (parquet/csv), partitioned
athena-results/        <- Athena query output
```

`raw/` is append-only source of truth. Batch jobs **read** `raw/` and write only to `batch-views/`.

### 4.6 DynamoDB speed-view state table

Table `scp-siem-speed-state`, on-demand billing:
- Partition key `source_ip` (String) · Sort key `bucket` (Number — epoch seconds floored to 30s)
- Attributes: `count` (Number), `ttl` (Number = bucket + 360)
- TTL enabled on `ttl`

**Windowing scheme:** ten 30-second buckets = a 5-minute window. On each failed event: floor
`producer_ts` to 30s, atomically `ADD` to the (ip, bucket) count, query the trailing 10 buckets
for that IP, sum = current 5-min count. Alert if sum ≥ threshold. Chosen over an exact
timestamp-list approach because it holds far less state.

Note: DynamoDB TTL deletes within ~48h of expiry, not on the dot. Harmless — the speed layer
only ever sums the trailing 10 buckets, so stale items never affect a count. TTL is housekeeping.

---

## 5. Benchmark plan (two experiments — both required)

**Experiment 1 — batch, sequential vs parallel.** Hold input fixed, vary parallelism, measure
wall-clock. Baselines: (a) plain single-process Python / single-node MapReduce; (b) Spark on
**1 / 3 / 5 / 7** EMR core nodes. Median of 3 runs, warm-up discarded, timed driver-side,
bootstrap excluded. Plots: runtime vs nodes, speedup S(N)=T(1)/T(N) with ideal line, efficiency
S(N)/N. Analysis anchored on **Amdahl's law** (shuffle + startup are the ceiling).

**Experiment 2 — speed-layer latency under load.** Vary producer rate across
**100 / 500 / 1,000 / 2,500 / 5,000** rec/s. Metrics: end-to-end latency p50/p95/p99 (via
`producer_ts`), Kinesis `GetRecords.IteratorAgeMilliseconds`, Lambda `Duration` /
`ConcurrentExecutions` / `Throttles`, pulled via boto3 → CSV. Expect the knee near the
single-shard ~1,000 rec/s limit; optionally add a shard and show recovery.

These two, plus **auto-scaling with stated triggers** and the **sliding window**, are the
distinction discriminators. They're the highest-value sections — don't let them get squeezed.

---

## 6. Repo layout

| Folder | Contents |
|---|---|
| `producer/` | Paced replay producer (boto3 → Kinesis) |
| `speed/` | Speed-layer Lambda |
| `batch/` | PySpark job + Hadoop Streaming MapReduce variant |
| `serving/` | Athena DDL + merge logic |
| `dashboard/` | Streamlit dashboard |
| `benchmarks/` | Benchmark drivers, raw CSVs, figures |
| `report/` | IEEE report + `architecture.svg` |
| `infra-notes/` | Working IAM/networking config, `resources.md`, conventions, budget |
| `fixtures/` | `OpenSSH_2k.log` (committed — small, immutable test fixture) |

Root docs: `PROJECT_PLAN.md` (full plan + milestone tracker), `CONTRACTS.md` (the locked
interfaces), `README.md`.

Local setup:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Note `requirements.txt` deliberately **excludes PySpark** — the batch job runs on EMR where
Spark is preinstalled.

---

## 7. Where we've got to

### ✅ M0 — Foundation & contracts (COMPLETE)
- Repo scaffolded: 9 folders, README, `.gitignore`, `.gitattributes`, `requirements.txt`
- Python venv + local deps (boto3, streamlit, matplotlib, python-dotenv)
- 2k fixture committed to `fixtures/`
- **AWS Budget live:** `scp-siem-monthly`, **$80 ceiling**, email alerts at 50% ($40),
  100% ($80), and forecast-over-100%
- Naming/tagging convention agreed: prefix `scp-siem-*`, tag `Project=scp-siem` on everything
- Contracts locked and committed (`CONTRACTS.md`)
- Architecture diagram produced → `report/architecture.svg` (reused in report + demo video)

### 🔄 M1 — Walking skeleton (IN PROGRESS)
The goal of M1 is to **retire the risky unknowns early** — one record flowing end-to-end on
both paths, and EMR proven to bootstrap.

- ✅ **Step 6 — DONE.** Kinesis stream `scp-siem-stream` (1 shard, provisioned) is **Active**;
  DynamoDB table `scp-siem-speed-state` (on-demand) is **Active** with TTL **On**. Both created
  via the **AWS console** rather than CLI, deliberately, for learning value.
- 🔄 **Step 7 (current) — producer.** `producer/producer.py` is written and its parser is
  validated against shell ground truth (520 `failed` records = `grep -c "Failed password"`;
  top-3 offender IPs match exactly). Remaining: run it against live Kinesis and confirm records
  appear in the Kinesis console's **Data viewer** tab.
- ⬜ Step 8 — stub Lambda (Kinesis event source mapping): log + write a raw record to DynamoDB
- ⬜ Step 9 — Firehose → S3: confirm objects landing in `raw/`
- ⬜ Step 10 — **EMR de-risk**: launch a small cluster (1 master + 1 core, m5.xlarge, spot core),
  run a trivial PySpark "count rows from S3" job, confirm `spark-submit` works, **tear it down**

> **Biggest risk on the project:** first-time EMR bootstrap / `spark-submit` is the classic
> time-sink. The M1 walking skeleton exists purely to kill that risk before it can touch the
> critical path. If EMR isn't running by end of M1, pair on it hard — don't let it bleed into M2.

### ⬜ Ahead — M2 to M5

| Milestone | Focus | Target |
|---|---|---|
| **M2** | Build the two layers for real — hardened producer (full line-parser + rate control), sliding-window Lambda, PySpark batch job + MapReduce variant, EMR managed scaling configured. Validate Spark output against the shell-command ground truth. *Biggest block.* | Jul 24–28 |
| **M3** | Serving merge + Streamlit dashboard; end-to-end smoke test | Jul 29–30 |
| **M4** | Both benchmark experiments; capture CSVs, generate figures, draft analysis; **tear everything down after** | Jul 31 – Aug 1 |
| **M5** | IEEE report (≤10 pages, 2-column), demo video, submit | Aug 2–3 |
| — | Buffer + submit | **Aug 4 by 17:00** |

**Submission checklist:** IEEE PDF to Moodle · GitHub link in the report · video link
(OneDrive/YouTube) in the report · final teardown with no lingering billable resources.

---

## 8. AWS account, access & cost guardrails — read before creating anything

### Account access

We build in **one shared personal AWS account**. An **IAM user has already been created for you**
and the credentials shared separately — sign in with that, never with root. Set up MFA on your
IAM user, and configure the CLI (`aws configure`) with your own access key if you want CLI access
alongside the console.

**Because the account is shared, coordinate before you create or delete anything:**

- **Resource names are unique per account + region.** `scp-siem-stream` and
  `scp-siem-speed-state` already exist (or are being created) — attempting to create them again
  will fail or, worse, you'll unknowingly work against the other person's resource. Check what's
  live in `infra-notes/resources.md` first, and add anything new you create.
- **Never delete or tear down a resource you didn't create** without checking first. The obvious
  landmine: tearing down the EMR cluster while the other person is mid-benchmark run.
- **The $80 budget is account-wide**, not per person. Both of your spend lands on the same
  meter, and both of you get the alert emails.
- Prefix anything experimental with your own initials (e.g. `scp-siem-test-jd-*`) so it's
  obvious who owns it and it's safe to clean up.

### Cost guardrails

**$80 self-imposed ceiling** (alerts at $40, $80, and forecast-over-$80). Realistic expected
total: **$30–50**.

- **Region is `us-east-1`** (N. Virginia). Check the top-right of the console every time —
  resources are region-scoped, and if you create something in the wrong region the other person
  simply won't see it.
- **Tear down the EMR cluster whenever you're not actively benchmarking.** It's by far the
  biggest line item.
- **Never create a NAT gateway** — keep EMR/EC2 in a public subnet or use VPC endpoints.
  A forgotten NAT gateway alone can eat the whole budget.
- **Kinesis stays at 1 shard** baseline; scale up only during a benchmark run, then back down.
- Use **spot instances** for EMR core nodes.
- Kinesis (~$0.36/day) and DynamoDB (on-demand, effectively free at this volume) are cheap
  enough to leave running through M1–M4. EMR is the one to be strict about.
- Tag everything `Project=scp-siem` so spend is filterable in Cost Explorer.

---

## 9. Working model

- **Paired, not split.** Both members work all sections together on scheduled work-time calls —
  shared ownership rather than divided modules.
- **Iterative with Claude:** one step at a time, confirm it worked, then the next step. Don't
  let Claude run ahead and dump a whole milestone at once.
- **Console over CLI** for creating AWS resources, deliberately — the click-through builds the
  mental model that CLI one-liners hide.
- **Keep `infra-notes/` current.** Every IAM role, policy, and networking config that actually
  worked goes in there. It's what saves you when something breaks at 11pm on Aug 3, and it
  feeds the report's implementation section. On a shared account this doubles as the register
  of what exists and who made it — `infra-notes/resources.md` is the source of truth, so update
  it in the same commit as the change.

---

## 10. Open items / decisions still to make

- [ ] **Sign-up sheet:** confirm the row is filled with the exact question above, and check all
      existing rows for collisions with another team
- [ ] **Lecturer heads-up email** re: replayed OpenSSH logs as the source
- [ ] **Alert threshold** for the speed layer (how many failed auths in 5 min = alert) — to be
      set at M2 off the back of the ground-truth top-offender counts
- [ ] **Batch-view format** — parquet vs csv (leaning parquet)
- [ ] **Merge implementation** — simplest is a dashboard-side join; alternative is Athena over
      an exported DynamoDB snapshot
- [ ] **EMR managed scaling trigger** — which metric + cooldown (e.g. `YARNMemoryAvailablePercentage`
      or pending-container ratio); must be *stated* in the report, not just enabled
- [ ] **Download the full `OpenSSH.log`** (70 MB, kept out of git) and run the §3 sanity checks —
      needed before the alert threshold can be set sensibly
- [ ] **Alerts storage** — separate DynamoDB table vs an attribute on the state table

> **Already settled — don't re-open.** `PROJECT_PLAN.md` §4b holds a numbered decision log (D1–D12)
> with the reasoning behind region, no-Terraform, shared account, shard/billing modes, windowing
> scheme, schema semantics, and the single-canonical-parser rule. Read it before proposing a
> change to any of them.
