# Project Context Handoff — Scalable Cloud Programming CA

**Purpose of this file:** bring a fresh Claude session (or a human) fully up to speed on this
project — what it is, what's been decided and *why*, what's already built, and what happens next.
It is self-contained: everything needed to contribute is below.

**Last updated:** 29 July 2026 (M2 — producer + speed layer done; full log verified) · **Deadline:** 4 August 2026, 17:00

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

# file-integrity checks — must return 655146 and 197587
wc -l SSH.log
grep -c "Failed password" SSH.log

# top offender IPs by grep — WARNING: this ranking is NOT the Spark reconciliation
# target. grep and the parser disagree; see 4.3 and PROJECT_PLAN.md D22.
grep "Failed password" SSH.log | grep -oE 'from [0-9.]+' | awk '{print $2}' \
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
- **Option-A skip (M2 / D17).** `parse_line()` now explicitly drops `message repeated N times`
  and PAM `authentication failure` lines via `RE_SKIP`. Deliberate stated undercount — the
  streaming pipeline is what's graded, not parsing completeness. This is why the skip-applied
  fixture `failed` count is **518, not 520** (see 4.4), and why `grep -c "Failed password"` on
  the full log (197,587) exceeds the parser's failed count (160,616): grep counts the repeat
  lines the parser skips.
- **⚠️ The skip also changes the top-offender *ranking* (M2 / D22).** This one will waste your
  afternoon if you don't know it. `message repeated N times: [ Failed password for root from
  ... ]` contains the string "Failed password" inside the brackets, so `grep` counts it and the
  parser drops it. The effect lands almost entirely on the heavy-repeat IPs: `59.63.188.30` reads
  **28,766 by grep but 14,384 by the parser**, and `58.242.83.25` reads 14,383 vs 7,192, while
  most other IPs are identical in both. That is enough to give grep a completely different #1.
  **Reconcile the batch job to the parser figures** (`CONTRACTS.md` §3), never to the grep
  ranking. The parser's #1, `183.63.110.206`, is also the IP owning the worst 5-minute burst in
  the threshold analysis — the parser view is the self-consistent one.

Two more that will bite you when reading `raw/` from S3:

- **Newline delimiting.** Firehose concatenates payloads with no delimiter, so the producer
  appends `\n` to each record. Don't remove it — without it, S3 objects are unparseable.
- **Spark directory recursion.** Firehose writes to `raw/yyyy/MM/dd/HH/`, and `spark.read.json()`
  does not descend into subdirectories by default. Every job reading `raw/` needs
  `.option("recursiveFileLookup", "true")` or it fails with `UNABLE_TO_INFER_SCHEMA`.

### 4.4 Validated ground truth — 2k fixture

**Reconcile the Spark job to the skip-applied column** (it reuses the skipping parser).

| Metric | M1 (raw) | M2 (skip applied) |
|---|---|---|
| Total lines | 1,999 | 1,999 |
| `failed` | 520 | **518** |
| `invalid_user` | 113 | 113 |
| `accepted` | 1 | 1 |
| `other` | 1,100 | **601** |
| Dropped | 265 | **767** |

Top failed-auth IPs (unchanged): `183.62.140.253` (286) · `187.141.143.180` (80) ·
`103.99.0.122` (46). **These three fired the alerts in the live speed-layer test** — nice
end-to-end confirmation.

**Full log (skip applied), re-verified locally 29 Jul 2026:** 160,616 `failed` · 14,581
`invalid_user` · 182 `accepted` · 138,387 `other` · 341,381 dropped. Worst single 5-min window =
**157** (`183.63.110.206`); attacker peak band **126–157** → basis for threshold 50 (D18).
Top five by the parser: `183.63.110.206` (17,340) · `183.238.178.195` (14,519) ·
`59.63.188.30` (14,384) · `183.62.140.253` (10,852) · `139.219.191.138` (10,852).

Cross-check: `grep -c "Failed password" fixtures/OpenSSH_2k.log` (returns more than the parser —
that gap is the D17 skip, not a bug).

**Line counts are off by one against `wc -l`** — both files lack a trailing newline, so `wc -l`
says 655,146 / 1,999 while a Python line loop sees 655,147 / 2,000. Expected.

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

**Alerts (M2 / D18 + D19):** threshold = **50** failed/IP/5-min. Alerts live in the *same* table:
`kind="alert"`, synthetic `bucket = 10_000_000_000 + window_start` (sorts clear of state rows),
plus `window_start`, `window_count`, `first_seen_ts`, `ttl = window_start + 3600`. Written via an
**idempotent conditional put** (`attribute_not_exists(source_ip)`) → exactly one alert per IP per
window, even under Kinesis redelivery. The *count* still double-counts on redelivery (D6); the
*alert* doesn't.

**IAM:** the speed Lambda role needed `dynamodb:Query` + `dynamodb:PutItem` added in M2 (M1 had
only `UpdateItem`). If the window query throws `AccessDeniedException`, that's the missing grant.

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

### ✅ M1 — Walking skeleton (COMPLETE, on schedule)
The goal of M1 was to **retire the risky unknowns early**. Both paths now work end-to-end and
the EMR risk is gone.

- ✅ Kinesis `scp-siem-stream` + DynamoDB `scp-siem-speed-state` live (created via console)
- ✅ Producer `producer/producer.py` — parser validated against shell ground truth (520 failed)
- ✅ Speed Lambda `scp-siem-speed` — Kinesis trigger → 30s-bucketed counts in DynamoDB, verified
      exactly against the fixture. Memory raised to 256 MB before benchmarking.
- ✅ Firehose `scp-siem-firehose` → S3 `raw/`, newline-delimited JSON
- ✅ **EMR de-risk PASSED** — emr-7.13.0 / Spark 3.5.6, `spark-submit` via console Step, read
      `s3://.../raw/`, groupBy shuffle completed, output reconciled exactly to the fixture
      (200 records: 119 other / 62 failed / 19 invalid_user). Cluster terminated.

**Round-trip integrity proven:** producer → Kinesis → Firehose → S3 → Spark preserves every
record and field. The locked schema survives intact.

> **Read `infra-notes/resources.md` §4 before you touch EMR.** The first cluster took ~1 hour,
> almost entirely on four config failures: the EC2 Spot service-linked role, the instance-profile
> S3 policy, the step's Application-location field, and Spark's directory recursion. All four are
> documented with exact fixes. Rebuilding should now take minutes.

### 🔄 M2 — producer + speed layer DONE (28 Jul); batch layer next

**Producer (`producer/producer.py`) — done, verified live in Kinesis.** Option-A skip; `--rate`,
`--loop`, `--batch-size` (put_records, capped 500, retry-once), `--limit 0`=unlimited. Live send
decoded off the shard: correct schema, nulls, `\n` delimiter, `source_ip` partition key.

Run it:
```bash
# parse-only, no AWS
python3 producer/producer.py --file fixtures/OpenSSH_2k.log --limit 50 --dry-run
# live, paced 200/s, 2000 records (crosses the alert threshold for hot IPs)
python3 producer/producer.py --file SSH.log --rate 200 --limit 2000
```

**Speed layer (`speed/lambda_function.py`) — done, alerts verified in DynamoDB.** Trailing-10-bucket
window sum; threshold 50; idempotent per-IP-per-window alerts. Verified locally with `moto`
(window sum, crossing, filtering, redelivery-idempotency) *and* live — the three fixture
top-offender IPs fired alert rows. Env var `ALERT_THRESHOLD=50` set on the function.

**Full log verified (29 Jul).** `SSH.log` is now at the repo root (gitignored, MD5
`fc38b9b464eae746aaed8ceb044bd743`) and every documented ground-truth figure reproduced exactly.
The batch layer has a trustworthy reconciliation target.

**Batch layer — the remaining M2 block.** The build strategy changed on 29 Jul (D21): **there is
no local Spark step.** The dev machine has no JRE and a Python 3.14 venv PySpark 3.5 doesn't
support, so instead of prototyping in Spark we write `batch/reference_counts.py` — a plain-Python
single-process implementation of the same aggregates that reconciles to the ground truth and
emits the expected-output file the Spark job gets diffed against. It doubles as the Experiment 1
sequential baseline M4 needs anyway. The `mapper.py`/`reducer.py` variant is testable locally
with no Hadoop at all (`cat SSH.log | mapper.py | sort | reducer.py`). Only the PySpark job runs
on EMR, against `s3://.../raw/` with `recursiveFileLookup=true`. Batch view is **parquet** (D20).

> **Open question before the EMR run:** `raw/` contains only what has actually been replayed
> through Kinesis, which is a *subset* of `SSH.log`. A like-for-like diff against the reference
> output means either replaying the full log to S3 first, or pointing the reference script at the
> same subset. Decide which before spinning up the cluster.

### ⬜ Ahead — rest of M2 to M5

| Milestone | Focus | Target |
|---|---|---|
| **M2** (finishing) | Batch layer: Python reference baseline → PySpark job + MapReduce variant + EMR managed scaling. Validate against parser ground truth. | Jul 28–29 |
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

**Still open:**

- [ ] **Sign-up sheet:** confirm the row is filled with the exact question above, and check all
      existing rows for collisions with another team — **still open since M0, worth closing now**
- [ ] **Lecturer heads-up email** re: replayed OpenSSH logs as the source — **still open since M0**
- [ ] **Merge implementation** — simplest is a dashboard-side join; alternative is Athena over
      an exported DynamoDB snapshot
- [ ] **EMR managed scaling trigger** — which metric + cooldown (e.g. `YARNMemoryAvailablePercentage`
      or pending-container ratio); must be *stated* in the report, not just enabled
- [ ] **Reference vs `raw/` scope** — `raw/` holds only what's been replayed, a subset of
      `SSH.log`. Decide how the Spark output gets diffed like-for-like against the reference
- [ ] **Write `benchmarks/find_threshold.py`** — designed and specified in `CONTRACTS.md` §7.3
      but never actually written. Its numbers were independently verified on 29 Jul, so D18 is
      sound; committing the script is what makes the threshold justification *reproducible* for
      the marker

**Closed since this list was written:**

- [x] **Alert threshold** — 50 failed auths / IP / 5-min window (D18, 28 Jul)
- [x] **Alerts storage** — same state table, `kind="alert"` + synthetic bucket (D19, 28 Jul)
- [x] **Batch-view format** — parquet (D20, 29 Jul)
- [x] **Download the full log** — done and verified 29 Jul; `SSH.log` at repo root, gitignored

> **Already settled — don't re-open.** `PROJECT_PLAN.md` §4b holds a numbered decision log
> (**D1–D22**) covering region, no-Terraform, shared account, shard/billing modes, windowing
> scheme, schema semantics, the single-canonical-parser rule, the Option-A skip, the alert
> threshold and storage, the parquet batch view, the no-local-Spark build strategy, and the
> grep-vs-parser reconciliation target. Read it before proposing a change to any of them.
