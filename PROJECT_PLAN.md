# Scalable Cloud Programming — CA Project Plan

**Module:** MSc Cloud Computing — Scalable Cloud Programming (NCI)
**Deliverable:** Python-based scalable real-time analytics system on a Lambda architecture (AWS)
**Due:** 4 August 2026, 17:00 · **Team size:** 2 · **Weighting:** 50% of module
**Primary dataset:** Loghub OpenSSH logs (locked) · **Region:** `us-east-1`
**Last updated:** 31 Jul 2026 (M0–M4 complete; M5 report + video remaining)

> **Status at a glance: M0, M1, M2 and M3 are all complete.** The speed layer fires
> exactly-one-per-IP-per-window alerts at threshold 50, and after the D26 batching optimisation it
> keeps pace with a 1,000 rec/s producer at **zero** iterator age (down from 138 s). The batch
> layer is validated four ways — the PySpark job, the Hadoop Streaming MapReduce variant, a
> single-process Python reference oracle, and the local `mapper | sort | reducer` pipeline all
> produce **byte-identical** results across 1,238 source IPs. The serving layer merges Athena
> (batch reputation) with DynamoDB (live alerts) into a working Streamlit dashboard.
>
> **M4 is complete.** Experiment 1 over 4.44 GB / 31.4M records: sequential **612 s**, Spark
> **155.4 / 56.3 / 37.0 / 30.5 s** at 1/3/5/7 nodes (**5.09× speedup, 72.8% efficiency**), Hadoop
> Streaming **3,732 s** at 7 nodes — 47 node-verified measurements, all outputs byte-identical.
> Experiment 2 found the shard knee at **~1,240 rec/s** and corrected our own burst-based capacity
> claim. Custom auto-scaling was demonstrated firing in both directions (**1 → 3 → 5 → 4** nodes)
> with the trigger and response both captured. Five figures in `report/figures/`. All EMR
> clusters terminated.
>
> **Remaining: M5 — the IEEE report and the demo video.**

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
| Auto-scaling          | **EMR custom automatic scaling** (CloudWatch metric rules, D29) · Lambda native concurrency (automatic, by shard) | Elastic compute with explicitly stated triggers; exercised by the batch benchmark |
| Metrics / benchmarks  | **CloudWatch** + **boto3** -> CSV -> **matplotlib *or* a spreadsheet** (benchmark graphs only) | Repeatable figures straight into the report |

> **No pandas, no data cleaning, no ML.** The real processing is distributed (Spark / MapReduce /
> Lambda). matplotlib (or Sheets/Excel) is only a thin presentation layer for the *required*
> benchmark graphs. Line-parsing the logs is `str.split()` / one regex — not preprocessing.

> **No Terraform / IaC.** Scrapped deliberately (see decision log D2) in favour of disciplined
> manual create/teardown via the AWS console.

> Distinction discriminators covered: **sliding-window** speed layer, **auto-scaling with stated
> triggers** (EMR **custom** auto scaling — explicit metric, threshold, cooldown),
> **sequential-vs-parallel** batch benchmark + **speedup-vs-nodes**.

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
| D20 | **Batch-view format = parquet** (closes the CONTRACTS §6 open item) | Columnar + compressed, and the standard Athena input — a defensible choice in the report. The dashboard is unaffected because it reads the batch view *through Athena* via boto3, never off the filesystem, so the "no pandas" rule is not strained by parquet's lack of a trivial local reader. | 29 Jul |
| D21 | **No local Spark. The batch logic is validated first by a single-process Python reference implementation**, and only the PySpark job runs on EMR | The dev machine has no JRE and its venv is Python 3.14, which PySpark 3.5 does not support — installing a JDK plus a second interpreter is a detour with six days left. The reference implementation is **not throwaway**: it is the sequential baseline Experiment 1 requires anyway, and it gives a verified expected-output file to diff the Spark job against, so ground-truth reconciliation still happens before EMR. The Hadoop Streaming variant is testable locally with no Hadoop at all (`cat SSH.log \| mapper.py \| sort \| reducer.py`). | 29 Jul |
| D34 | **Auto-scaling demonstrated end to end — trigger and response both captured** | Cluster `j-08215763I5JEY4IRNFK4`, custom policy attached to the core group (min 1 / max 7), 30 Jul 23:45–00:10 UTC. Measured trace: **23:45** 1 node, 0 pending, YARN memory 100% available · **23:50** load arrives, **158 containers pending**, memory available collapses to **1%** · **23:55** scale-out fires, 2 nodes · **00:00** 3 nodes, pending draining to 32 · **00:05** **5 nodes**, pending 0, memory back to 100% · **00:10** scale-in fires, **4 nodes**. Both rules exercised: `ContainerPendingRatio > 0.75` (+2, 300 s cooldown) twice, `YARNMemoryAvailablePercentage > 75` (−1) once. The causal chain is visible in the metrics rather than asserted — queue builds, capacity is added, queue drains, capacity is released. **For the report, pair the `ContainerPending` graph (trigger) with `CoreNodesRunning` (response)**; either alone is only half the evidence. Note the ~5 min lag between demand and new nodes serving — the cost of the 5-minute metric cadence that D29 accepted in exchange for stated triggers. | 31 Jul |
| D33 | **Experiment 2 complete — and it CORRECTS D26's headline claim** | Five rates, 180 s sustained each. Results: 100 → 102.8 achieved, iter age 0, lag p50 1,547 ms · 500 → 522.2, iter age **0**, lag p50 1,180 ms · 1,000 → 1,075, iter age **34 s**, lag p50 11,676 ms · 2,500 → **1,238.9**, iter age 43 s, lag p50 18,001 ms · 5,000 → **1,249.2**, iter age 48 s, lag p50 31,941 ms. **(1) The shard knee is clean:** achieved throughput plateaus at ~1,240 rec/s regardless of offered load — one shard, exactly as predicted. **(2) CORRECTION to D26:** that entry recorded 1,000 rec/s at *zero* iterator age, but it measured a 30-second burst the consumer could absorb. Sustained for 180 s, 1,000 rec/s produces **34 s of lag**. True sustained capacity is **between 500 and 1,000 rec/s** — the optimisation moved the ceiling from ~90 rec/s to that band, not to 1,000. The report must state the sustained figure, not the burst one. **(3) Lambda is never the bottleneck:** Duration 98–155 ms, ConcurrentExecutions pinned at 2, **zero throttles** at every tier — with one shard the consumer cannot be parallelised further, so records queue instead. **(4) Latency is U-shaped:** p50 lag is *higher* at 100 rec/s (1,547 ms) than at 500 (1,180 ms), because a 100-record batch takes a full second to fill at 100 rec/s. Batching cost dominates when slow; queueing dominates when saturated. | 31 Jul |
| D32 | **Experiment 1 complete — and distribution is not automatically a win** | All measured on the same 4.44 GB / 31,376,600 records, all verified byte-identical output. **Sequential (1 process, 1 thread, streaming the same S3 input): 612 s** — remarkably stable at 611.6 s and 612.1 s across runs. Against that baseline: **Spark 1 node 155.4 s = 3.94×**, 3 nodes 56.3 s = 10.9×, 5 nodes 37.0 s = 16.5×, **7 nodes 30.5 s = 20.1×**; **Hadoop Streaming 7 nodes 3,732 s = 0.16×, i.e. six times SLOWER than not distributing at all.** Two findings the report should lead with. (1) Spark beats sequential *on a single node* by 3.94× — that is intra-machine parallelism, four cores fetching and parsing S3 objects concurrently against one thread doing it serially, so distribution pays before a second machine is added. (2) Seven machines and 28 CPUs running Hadoop Streaming lose to one Python process, because 4,300 map tasks each pay JVM launch + Python fork and that overhead exceeds the entire workload. The lesson is that a framework's per-task cost must be small relative to per-task work; with ~1 MB input files it is not. | 31 Jul |
| D31 | **Spark vs Hadoop Streaming on identical input: 122× — and the cause is task startup, not parallelism** | Same 4.44 GB, same 7 m5.xlarge core nodes, same aggregation, verified byte-identical output (1,238 IPs, 31,376,600 records). **Spark median 30.5 s; Hadoop Streaming 3,732 s (62.2 min).** Counters explain it: **4,300 map tasks** (one per input object) consumed **45,208 s of aggregate task time** — about **10.5 s per task** to process a ~1 MB file. That is almost entirely JVM launch plus a forked Python subprocess plus text serialisation through a pipe, not counting work. The 28 reduce tasks added 13,948 s. Spark reads the same 4,300 objects with threads inside long-lived reused executors, so it pays that cost once per executor rather than 4,300 times. **Two lessons for the report:** the small-files problem punishes the two engines completely differently, and an engine comparison at fixed node count isolates *framework overhead* in a way the speedup curve cannot. Also note the earlier attempt with EMR-default reducers was cancelled at 26 min; adding `mapreduce.job.reduces=28` slightly *slowed* the map phase, because reduce containers begin competing for slots at 5% map completion. | 30 Jul |
| D30 | **On-Demand core nodes for benchmark runs; Spot only for exploratory work** | **Learned the hard way, 30 Jul.** The first Experiment 1 attempt died 26 minutes in: `TERMINATED_WITH_ERRORS / INSTANCE_FAILURE — All slaves in the job flow were terminated due to Spot`. AWS reclaimed the core capacity and the in-flight step was cancelled. Losing *all* core nodes terminates the cluster outright, so a single reclamation event destroys a two-hour measurement run. Costed it: the full sweep is **~$1.92 On-Demand vs ~$0.58 Spot** — $1.34 to make the run survivable, against a deadline where a lost afternoon cannot be bought back. §9's blanket "Spot for EMR core nodes" is now qualified: **Spot for exploration and de-risking, On-Demand for anything whose result you need in one uninterrupted pass.** Also a genuine critical-analysis point for the report — a real interruption, a measured cost delta, and an explicit reliability-vs-cost decision. | 30 Jul |
| D29 | **Auto-scaling = EMR custom automatic scaling, NOT EMR-managed scaling** (supersedes the 29 Jul managed-scaling config) | The rubric rewards auto-scaling **with stated triggers**, and the two modes differ exactly there. Managed scaling is an AWS-internal YARN heuristic — the most the report can honestly say is "AWS evaluates cluster pressure", with no threshold we chose. Custom auto scaling is driven by CloudWatch alarms **we** define, so metric, threshold, evaluation window, cooldown and increment are all explicit and defensible in writing. Requires the `EMR_AutoScaling_DefaultRole` (absent from this account until 30 Jul — custom scaling is not selectable without it) and instance **groups** rather than fleets, which is what `scp-siem-emr` uses. The two modes are mutually exclusive. **Stated tradeoff:** EMR publishes these metrics at ~5-minute granularity, so custom scaling reacts in minutes where managed scaling reacts in 5–10 s. We accept slower reaction to gain explicit, defensible triggers — and say so in the report rather than hiding it. | 30 Jul |
| D28 | **Auto-scaling must be OFF during Experiment 1, and demonstrated separately** | The two requirements conflict head-on. Experiment 1 holds the core count fixed at 1/3/5/7 to measure S(N)=T(1)/T(N); *any* auto-scaling mode resizes the cluster mid-run, so N becomes an unknown drifting variable and every timing is meaningless. **Therefore:** run the sweep with scaling disabled and fixed instance counts, then enable the D29 policy and capture a separate CloudWatch graph of it scaling out under load. Both are graded; they simply cannot be measured simultaneously. `benchmarks/run_experiment1.py` refuses to start if either scaling mode is active, so this cannot be forgotten. Worth a sentence in the report — noticing the interference is itself analysis. | 30 Jul |
| D27 | **Experiment 1 input = the 29 Jul replay replicated 100× into `s3://<bucket>/benchmark-input/`** — final: **4,300 objects / 4.44 GB / 31,376,600 records** (16,061,600 failed, 1,238 distinct IPs) | **Measured:** one replica is 42 MB / 313,766 JSON records, and the single-process baseline chews through it in **0.34 s**. Any speedup curve built on that would measure Spark's fixed scheduling overhead and nothing else. ~100 replicas ≈ **4.2 GB / 31.4M records**, putting the sequential baseline near 35 s and Spark's T(1) into the minutes — the range where S(N)=T(1)/T(N) is actually meaningful. Replication is a **server-side S3 copy**, so it needs no cluster, transfers no data through the laptop, and costs pennies (~$0.10/month storage, ~$0.03 in PUTs). Written to a **separate prefix**, never into `raw/`, which CONTRACTS §4 locks as the immutable Firehose landing zone. | 30 Jul |
| D26 | **Speed Lambda aggregates each Kinesis batch before writing** (M3, 29 Jul) | **Measured problem:** the M2 handler made one `UpdateItem` *and* one `Query` per failed record — two synchronous DynamoDB round trips each, serially. On a 1,000 rec/s replay CloudWatch showed `Duration` ~2,200 ms per 100-record batch (~22 ms/record), `ConcurrentExecutions` maxing at 2, and `GetRecords.IteratorAgeMilliseconds` peaking at **138,000 ms** — the consumer fell 138 s behind. Effective throughput ~90 rec/s, an order of magnitude *below* the single-shard Kinesis limit, so **the speed layer was the bottleneck, not the shard**. This corrects §7's prediction that Experiment 2's knee sits near 1,000 rec/s. **Fix:** aggregate each batch in memory into `(source_ip, bucket) -> n`, then one `UpdateItem` per group using `ADD :n`, then one window `Query` per *distinct IP* (phase 3 must follow phase 2 so the sum sees the new counts). Calls drop from `2N` to `len(groups) + len(distinct_ips)`; measured **5.7×–20.7× fewer** DynamoDB calls, and the gain grows with key skew — which brute-force traffic has in abundance. **Verified** by running old and new handlers over identical synthetic batches: counts byte-identical, alert identity (IP + window) identical. One deliberate semantic change: `window_count` on alert rows now records the *true* window size at detection instead of always reading exactly the threshold, because the check happens once per batch rather than after every record. Strictly more informative, and it fixes the "every alert says 50" problem. **MEASURED RESULT on an identical 30,000-record / 1,000 rec/s replay: mean `Duration` 2,200 ms → 89.6 ms (24× faster), max 2,600 ms → 566 ms, and peak iterator age 138,000 ms → 0 ms — the consumer never fell behind at all. Spiking IPs detected in the same window rose from 2–3 to 13, because the backlog no longer hid them. Zero errors, zero throttles.** | 29 Jul |
| D25 | **Merge = split the work, join in the dashboard** (closes the last CONTRACTS §6 open item) | Athena does the batch-side heavy lifting in SQL (rank offenders, threshold-filter, return a small result set); DynamoDB is queried for live `kind="alert"` rows; the dashboard performs only the final small join on `source_ip`. Each store does what it is good at, the merged view is genuinely live — no export lag, which matters because the whole claim is "spiking **right now**" — and it adds no new AWS resources inside a two-day milestone. Rejected: Athena over a DynamoDB snapshot, which would make the merge one SQL join but requires enabling PITR, costs minutes per export, and produces a *stale* view that undercuts the freshness argument the speed layer exists to make. **Stated limitation for the report:** the final join executes in the presentation layer, not in a distributed engine. | 29 Jul |
| D24 | **Both engines need an explicit recursion flag for Firehose's nested layout** | D15 recorded this for Spark (`recursiveFileLookup=true`). Hadoop Streaming has the *same* defect and an equally unhelpful error: pointing `-input` at `raw/2026/07/29/` fails with `Error Launching job : Not a file`, because `-input` does not descend into `15/`. Fix: `-D mapreduce.input.fileinputformat.input.dir.recursive=true` as the **first** argument (generic options precede streaming options), or point `-input` at the leaf hour directory. Worth a report sentence — the same nested-partition assumption breaks two different engines. | 29 Jul |
| D23 | **Batch reconciliation runs read `raw/2026/07/29/`, not all of `raw/`** | `raw/` is append-only and still holds **2,300 records** from the M1/M2 smoke tests (200 on 23 Jul; 100 + 2,000 on 28 Jul). Reading everything returns 316,066 records instead of 313,766, and per-IP counts are contaminated because the 2k fixture reuses the same offender IPs. Scoping to the replay partition keeps the reconciliation exact without mutating the immutable landing zone. | 29 Jul |
| D22 | **Reconcile the batch layer against `parse_line()` output, NOT the §5.1 `grep` pipeline** | The two disagree on the top-offender *ranking*: `message repeated N times: [ Failed password ... ]` lines match grep's pattern but are skipped by the parser (D17), roughly doubling `59.63.188.30` (28,766 vs 14,384) and `58.242.83.25` (14,383 vs 7,192) and giving grep a spurious #1. The parser's #1 (`183.63.110.206`) is the same IP that owns the worst 5-min burst, so the parser view is the self-consistent one. A Spark job that matches grep is wrong. | 29 Jul |

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
tar -xzf SSH.tar.gz && ls -lh          # extracted log is SSH.log (~70 MB)

# sanity checks — file-integrity only, NOT the batch-job reconciliation target (see warning below)
wc -l SSH.log
grep -c "Failed password" SSH.log
grep "Failed password" SSH.log | grep -oE 'from [0-9.]+' | awk '{print $2}' \
  | sort | uniq -c | sort -rn | head        # top offender IPs — grep's ranking, not the parser's
```

> ⚠️ **Do not reconcile the Spark job against that last command (D22).** `grep` and
> `parse_line()` produce **different top-offender rankings**, because `message repeated N times:
> [ Failed password for root from ... ]` lines match grep's pattern but are skipped by the parser
> (D17). It roughly doubles the heavy-repeat IPs — `59.63.188.30` reads 28,766 by grep but
> **14,384** by the parser, enough to hand it a spurious #1 — while most other IPs are unaffected.
> Use `wc -l` and `grep -c` as **file-integrity checks** (they must return 655,146 and 197,587),
> and reconcile the batch layer against the **parser** figures in §5.4 and `CONTRACTS.md` §3.

The verified local copy is `SSH.log` at the repo root (gitignored), MD5
`fc38b9b464eae746aaed8ceb044bd743`.

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

Computed with the canonical `parse_line()` + Option-A skip over the full 655,146-line `SSH.log`.
**Re-verified locally 29 Jul 2026** — every figure below reproduced exactly; the full status
breakdown is new.

| Metric | Value |
|---|---|
| Total lines | 655,146 |
| `status: failed` (skip applied) | **160,616** |
| `status: invalid_user` | 14,581 |
| `status: accepted` | 182 |
| `status: other` | 138,387 |
| Lines dropped (skip + no-IP) | 341,381 |
| Distinct (IP, 5-min window) pairs | 7,026 |
| **Worst single 5-min window** | **157** (`183.63.110.206`, a Jan-03 burst) |
| Top-IP peak-window band | **126–157** (tightly clustered) |

Top five failed-auth IPs **by the parser** — the batch layer's reconciliation target (D22):
`183.63.110.206` (17,340) · `183.238.178.195` (14,519) · `59.63.188.30` (14,384) ·
then `139.219.191.138` and `183.62.140.253` **tied on 10,852**. Full ten-row table with the grep
comparison is in `CONTRACTS.md` §3. The log yields **313,766 records across 1,238 distinct IPs**
after the D17 skip.

> **Sort tied rows by `source_ip` ascending** in both the reference and the Spark job. Those two
> 10,852 rows will otherwise land in arbitrary order and a diff will report a mismatch where the
> data agrees.

> **Line-count note.** `wc -l` says 655,146 because the file has no trailing newline, so a Python
> `for line in fh` loop iterates 655,147 times. Same off-by-one on the 2k fixture (1,999 vs
> 2,000). Expected, not a parser bug.

Raw `grep -c "Failed password"` on the full log returns **197,587** — higher than 160,616
because grep counts the ~37k `message repeated` lines the producer skips. This gap is expected
and is the D17 undercount, stated in the report.

> The 126–157 band is the justification for the **threshold = 50** decision (D18): well below any
> real attacker's peak (no false negatives), well above any legitimate user's 5-min failures
> (near-zero false positives).
>
> **Independently re-verified 29 Jul 2026.** 7,026 distinct (IP, 5-min window) pairs; worst window
> **157** (`183.63.110.206`, Jan 03 01:50); top-15 peak band exactly **126–157**. Every figure
> matched. Note the analysis script itself (`benchmarks/find_threshold.py`, specified in
> `CONTRACTS.md` §7.3) has **not been written yet** — the numbers are sound, but the script needs
> committing before the report claims a reproducible method.

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

 Auto-scaling boundary:  EMR custom auto scaling (CloudWatch rules, stated triggers)  ·  Lambda concurrency (native, by shard)
```

---

## 7. Benchmark Plan (summary — detailed method finalised at M4)

**Experiment 1 — Batch, sequential vs parallel.** Hold input fixed, vary parallelism, measure
wall-clock runtime. Baselines: (a) plain single-process Python / single-node MapReduce; (b) Spark
on 1 vs N EMR core nodes across **1 / 3 / 5 / 7**. Median of 3 runs each, warm-up discarded, timed
driver-side (exclude cluster bootstrap). Plots: runtime vs nodes, speedup S(N)=T(1)/T(N) vs nodes
(with ideal line), efficiency S(N)/N. Analysis anchored on Amdahl (shuffle + startup are the ceiling).

> **Experiment 2 has a before/after story now (D26).** The original prediction — "the knee sits
> near the single-shard 1,000 rec/s limit" — was wrong for the M2 handler: the per-record
> DynamoDB pattern saturated at ~90 rec/s, a tenth of the shard limit, and CloudWatch proved it
> (138 s iterator age). After batching, the same 1,000 rec/s load runs at 89.6 ms/batch with
> **zero** iterator age. So the sweep should now push to the higher tiers (2,500 / 5,000 rec/s)
> to find where the *shard* becomes the constraint. Report both curves: the optimisation is the
> analysis, not a footnote.

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
| M2 | Build the two layers | Jul 24–**29** (slipped 1 day) | ✅ **Complete** |
| M3 | Serving merge + dashboard | Jul 29–30 | ✅ **Complete** |
| M4 | Auto-scaling + benchmarks | Jul 30–31 | ✅ **Complete** (a day early) |
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
- [x] Download full `SSH.log` + run §5.1 sanity checks on it — **done 29 Jul**; 655,146 lines and
      `grep -c` 197,587 both match, and `parse_line()` reproduces the §5.4 figures exactly

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

### M2 — Build the Two Layers  ✅ **COMPLETE**  (Jul 24–29)  *the biggest block*
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
- [x] IAM role widened: added `dynamodb:Query` + `dynamodb:PutItem` (M1 had only `UpdateItem`) — recorded in `infra-notes/resources.md` §3 (29 Jul)
- [ ] **Write `benchmarks/find_threshold.py`** — the D18 threshold-derivation script. Designed in `CONTRACTS.md` §7.3 and its numbers verified 29 Jul, but the file itself doesn't exist yet. Needed so the report's threshold justification is reproducible, not just asserted

*Batch layer (Spark on EMR)* — ✅ **DONE (29 Jul)**
- [x] Full `SSH.log` local and verified against §5.4 (29 Jul)
- [x] Full log replayed to S3: **313,766 records** landed in `raw/2026/07/29/15/` (43 objects)
- [x] `batch/reference_counts.py` — single-process Python reference for the same aggregates;
      reconciles to §5.4 and emits the expected-output file the Spark job is diffed against.
      **Also serves as the Experiment 1 sequential baseline** (D21), so it is not throwaway
- [x] PySpark job `batch/spark_batch.py` -> parquet batch view (D20) + optional CSV validation copy
- [x] Hadoop Streaming **MapReduce** variant (`batch/mapper.py` / `batch/reducer.py`), tested
      locally with no Hadoop (`cat … | mapper.py | sort | reducer.py`) and then on EMR
- [x] **Parser reuse (D12)** — satisfied structurally: the producer is the single parsing
      boundary, so neither Spark nor the MapReduce job parses anything. They read the locked
      JSON schema straight from `raw/`
- [x] **Validated: all four implementations agree exactly.** Reference oracle == local
      mapper/reducer == EMR Spark == EMR Hadoop Streaming, byte-for-byte across all
      **1,238 IPs**: 160,616 failed / 182 accepted / 14,581 invalid_user / 138,387 other,
      313,766 records total
- [x] Auto-scaling configured (29 Jul as EMR-managed; **superseded 30 Jul by custom automatic
      scaling, D29** — explicit CloudWatch triggers make it defensible in the report). Bounds
      unchanged at min 1 / max 7 core nodes, deliberately matching the 1/3/5/7 Experiment 1 sweep.
      Full policy in `infra-notes/resources.md` §2. **Must be disabled during the sweep (D28).**
- [x] *(tidy)* re-ran the Spark step with `.coalesce(1)` before the parquet write. **Measured
      before/after: 1,001 objects / 2.5 MiB → 2 objects / 33.3 KiB for identical data (~75×).**
      The `orderBy` shuffles to `spark.sql.shuffle.partitions`, so ~1,000 tasks each wrote a
      near-empty parquet file whose footer/metadata dwarfed its ~1 row. Concrete small-files
      illustration for the report's critical analysis — measured, not asserted. Output
      re-validated against the reference after the change: still byte-identical

> **Build strategy for the batch layer (revised 29 Jul — D21).** The original plan said "develop
> the PySpark job locally first", but the dev machine has no JRE and a Python 3.14 venv that
> PySpark 3.5 doesn't support, so local Spark is an install detour we're skipping. Instead:
>
> 1. Write `batch/reference_counts.py` (plain Python, imports `parse_line()`) and reconcile it to
>    §5.4. This pins the expected numbers with zero AWS cost.
> 2. Write the PySpark job to produce the *same* aggregates, and the `mapper.py`/`reducer.py`
>    variant — the latter is fully testable locally with no Hadoop
>    (`cat SSH.log | mapper.py | sort | reducer.py`).
> 3. Only then run the PySpark job on EMR against `s3://<bucket>/raw/` with
>    `recursiveFileLookup=true` (D15), and **diff its output against the reference file**.
>
> The point of the original advice still holds: don't debug Spark logic and EMR config at the same
> time. This just moves the "is the logic right?" check into a Python script instead of local Spark.
>
> **Scope — settled 29 Jul (D23).** The full log was replayed into `raw/`, so the batch layer now
> reads a genuine full-history master dataset. But `raw/` *also* still holds **2,300 records** from
> the M1/M2 smoke tests (200 on 23 Jul, 100 + 2,000 on 28 Jul). Reading all of `raw/` therefore
> returns 316,066 records, not 313,766, and the per-IP counts are contaminated because the 2k
> fixture reuses the same offender IPs. **Reconciliation runs point at `raw/2026/07/29/`.**

---

### M3 — Serving Merge + Dashboard  ✅ **COMPLETE**  (Jul 29–30)
**DoD:** system behaves as one — merged priority view queryable and visualised. — *met*

- [x] Athena: external table `scp_siem.batch_view` over the parquet batch view (`serving/athena_ddl.sql`); validated at 1,238 rows reconciling to ground truth. **Athena runs ONE statement per execution** — the DDL and each query must be run separately
- [x] Merge logic `serving/merge.py` (D25): Athena ranks + threshold-filters in SQL, DynamoDB supplies live alerts, dashboard joins on `source_ip`. Classifies PRIORITY / SPIKING NEW / HISTORIC
- [x] Streamlit dashboard `dashboard/app.py` — all four panels; `st.fragment(run_every=…)` refreshes the live half while the Athena half is cached 60 s
- [x] End-to-end smoke test passed: producer running -> alerts appear -> dashboard updates live
- [x] **Speed layer optimised mid-milestone (D26)** after CloudWatch showed a 138 s iterator age; now keeps pace with 1,000 rec/s at zero lag
- [x] `speed/test_speed_layer.py` added — 8 dependency-free tests (counting, bucketing, windowing, threshold, redelivery idempotency). Replaces the `moto` suite the docs claimed but that never existed

> **Two behaviours to explain in the demo, not apologise for.** (1) The speed view is empty
> between producer runs — state rows live 6 min, alerts 1 h — so "nothing spiking" is correct,
> not broken. (2) `first_seen`/`last_seen` come from the batch view and only change when the
> Spark job reruns; that staleness is precisely what the speed layer compensates for, and it is
> the Lambda-architecture argument made visible.
>
> **Fixed during the smoke test:** `batch_offenders(limit=100)` truncated below the 132 IPs that
> meet the threshold, so alerting IPs ranked 101+ were misclassified as SPIKING NEW (i.e. as
> having no history). The limit must exceed the number of IPs above the threshold — raised to 1,000.

---

### M4 — Auto-scaling + Benchmarks  ✅ **COMPLETE**  (Jul 30–31)
**DoD:** both experiments run; raw metrics captured as CSV; figures generated; analysis notes drafted.

*Experiment 1 — batch seq-vs-parallel*
- [x] Input sized on evidence (D27): the 29 Jul replay replicated 100× → **4,300 objects / 4.44 GB / 31,376,600 records** in `s3://<bucket>/benchmark-input/`. One replica alone ran in 0.34 s, which would have measured Spark's scheduling overhead and nothing else
- [x] Driver written: `benchmarks/run_experiment1.py` — resizes one cluster through 1/3/5/7, runs 1 warm-up + 3 measured per size, parses the job's own `ELAPSED_SECONDS`, writes CSV incrementally, prints medians + speedup + efficiency
- [ ] **Disable auto-scaling before the sweep (D28)** — the runner refuses to start otherwise
- [ ] Run the sweep; every run must report `records=31376600` or the input is wrong, not the timing
- [x] **Spark sweep complete** (30 Jul): 47 usable measurements recovered and node-count-verified via `benchmarks/recover_experiment1.py`; 8–9 measured runs per size after discarding warm-ups. **T(1)=155.4 s → T(7)=30.5 s, speedup 5.09×, efficiency 72.8%**
- [x] **MapReduce at 7 nodes** (D31): 3,732 s vs Spark's 30.5 s on identical input — **122×** — with output verified byte-identical. 4,300 map tasks at ~10.5 s each is the cause
- [ ] **1-node MapReduce dropped deliberately**: at 7× the 62-min runtime it would need 4–5 hours, which the schedule cannot absorb and which would tell us nothing the 7-node comparison doesn't
- [x] **Sequential baseline complete** (D32): `benchmarks/sequential_baseline.py`, single process/thread over the same S3 input on a matching m5.xlarge primary. **612 s**, with runs agreeing to within 0.5 s. 1 warm-up + 2 measured — quote it as a 2-run mean and say so in the method
- [x] **Experiment 1 fully assembled**: sequential 612 s · Spark 155.4/56.3/37.0/30.5 s at 1/3/5/7 nodes · MapReduce 3,732 s at 7 nodes
- [ ] Metrics -> CSV in `benchmarks/`; generate the speedup and efficiency figures
- [ ] Metrics -> CSV in `benchmarks/`; compute speedup S(N)=T(1)/T(N) and efficiency S(N)/N

*Auto-scaling demonstration — separate from Experiment 1 (D28/D29)*
- [x] Custom auto-scaling policy attached and **verified firing in both directions** (D34): 1 → 2 → 3 → 5 → 4 core nodes, driven by 158 pending containers and released when YARN memory recovered
- [x] Exact metric / threshold / evaluation window / cooldown / increment recorded in `infra-notes/resources.md` §2 — the "stated triggers" requirement
- [x] **`CoreNodesRunning` captured** for 23:40–00:15 UTC (1 → 3 → 5 → 4), paired with the `ContainerPending` capture — trigger and response both evidenced

*Experiment 2 — speed-layer latency under load*
- [x] Baseline already captured (D26): the pre-optimisation handler saturated at ~90 rec/s with 138 s iterator age; the batched handler runs the same 1,000 rec/s at 89.6 ms/batch and **zero** lag. The before/after pair is the centrepiece
- [x] **Sweep complete** (D33) via `benchmarks/run_experiment2.py`: 5 rates x 180 s sustained, results in `benchmarks/experiment2_raw.csv`
- [x] Latency instrumented in the handler (`lag_ms_p50/p95/max` from `producer_ts`), distinct from iterator age. **Per batch, not per record** — say so in the report
- [x] **Shard knee found: ~1,240 rec/s**, flat across the 2,500 and 5,000 tiers
- [x] **D26's claim corrected**: sustained capacity is 500–1,000 rec/s, not 1,000. The zero-lag figure came from a 30 s burst
- [ ] *(optional, if time)* add a second shard and show recovery

*Both*
- [x] **Figures generated** via `benchmarks/make_figures.py` -> `report/figures/` (300 dpi, sized for IEEE two-column):
      `fig1_speedup_efficiency.png` (speedup vs the ideal line + efficiency decay) ·
      `fig2_runtime_vs_nodes.png` (median with min–max band and sample counts) ·
      `fig3_engine_comparison.png` (sequential vs Spark vs MapReduce, log scale) ·
      `fig4_throughput_knee.png` (offered vs achieved — the shard ceiling) ·
      `fig5_latency_vs_rate.png` (record age and consumer lag, shared ms axis).
      Every plotted value comes from the experiment CSVs; the two constants
      (sequential 612.1 s, MapReduce 3,732.4 s) carry provenance comments.
      **No dual-axis charts** — where measures share units they share an axis, otherwise separate figures
- [x] **All EMR clusters terminated** (31 Jul). Kinesis, DynamoDB, Lambda, Firehose and S3 remain — cheap and needed for the demo video
- [ ] Draft critical-analysis notes. The raw material, all measured rather than asserted:
      **Amdahl** — efficiency 100 → 92 → 84 → 73% as shuffle and scheduling take a growing share (D32) ·
      **Framework cost ≠ parallelism** — Hadoop Streaming on 7 nodes is 122× slower than Spark and **6× slower than one Python process**, because 4,300 map tasks each pay a JVM launch (D31) ·
      **Distribution wins before the second machine** — Spark on 1 node beats sequential 3.94×, since four cores fetch S3 concurrently against one thread (D32) ·
      **Bottleneck found and fixed** — 138 s iterator age traced by CloudWatch to per-record DynamoDB calls; batching gave 24× (D26) ·
      **Burst ≠ sustained** — our own 1,000 rec/s zero-lag claim held for 30 s and failed at 180 s; true capacity 500–1,000 (D33) ·
      **Elasticity is bounded by quota, not budget** — an 8 vCPU limit blocked a 7-node run the $80 ceiling could easily afford (D27/D28) ·
      **Spot economics** — reclamation destroyed a 2-hour benchmark; $1.34 bought reliability (D30)

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
- **Spot for exploration, On-Demand for measurement (D30).** Spot is ~70% cheaper but
  interruptible, and losing all core nodes terminates the cluster — which killed the first
  Experiment 1 attempt 26 minutes in. The full sweep costs ~$1.92 On-Demand vs ~$0.58 Spot; pay
  the $1.34 for any run you need to complete in one pass. Realistic total: **$30–50**.
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
auto-scaling config) in a 5-day window. **Retired 30 Jul:** M2 and M3 both landed complete,
including the MapReduce variant and the auto-scaling config.

**Current top risk (30 Jul):** M4 in the remaining window, with the report and video after it.
Experiment 1 is ~2 hours of cluster time and Experiment 2 needs a rate sweep; the auto-scaling
demonstration is a third, separate capture (D28). Budget the cluster time deliberately and tear
it down between runs.
