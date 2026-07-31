# Live AWS Resources & Runbook

**Account:** 009910375264 (shared) · **Region:** `us-east-1` · **Last updated:** 29 Jul 2026

> Shared account: check this file before creating anything (names are unique per
> account+region), and never tear down a resource you didn't create. Update this file in
> the same commit as any infrastructure change.

---

## 1. Persistent resources (leave running through M1–M4)

| Resource | Name / ID | Config | Status |
|---|---|---|---|
| Kinesis Data Stream | `scp-siem-stream` | 1 shard, provisioned. PK = `source_ip` | ACTIVE |
| DynamoDB table | `scp-siem-speed-state` | PK=`source_ip` (S), SK=`bucket` (N). On-demand. TTL on `ttl` | ACTIVE |
| S3 bucket | `scp-siem-data-009910375264` | `raw/`, `batch-views/`, `athena-results/`, `scripts/`, `emr-logs/` | ACTIVE |
| Firehose stream | `scp-siem-firehose` | Source: Kinesis `scp-siem-stream`. Dest: S3 `raw/`. Buffer 1 MiB / 60 s, uncompressed | ACTIVE |
| Lambda function | `scp-siem-speed` | Python 3.13, **256 MB** (D14), 30 s timeout. Env: `ALERT_THRESHOLD=50` (D18), `STATE_TABLE` defaults to `scp-siem-speed-state` | ACTIVE |
| Lambda event source | Kinesis → `scp-siem-speed` | Batch size 100, starting position LATEST | ENABLED |
| AWS Budget | `scp-siem-monthly` | $80 ceiling; alerts 50% / 100% / forecast-100% | ACTIVE |

**Approximate running cost:** Kinesis ~$0.36/day; DynamoDB + Lambda + Firehose ≈ negligible at
this volume. Safe to leave up.

---

## 2. Ephemeral resources (create on demand, ALWAYS terminate)

| Resource | Name | Config | Status |
|---|---|---|---|
| EMR cluster | `scp-siem-emr` | emr-7.13.0 (Spark 3.5.6, Hadoop 3.4.2, Hive 3.1.3, Livy 0.8.0). m5.xlarge. Auto-terminate on 1 h idle | see below |

**Cost:** ~$0.35/hour at 1+1; ~$1.54/hour at 1 primary + 7 core. **Terminate immediately after
every use.** Cluster history — all **TERMINATED** as of 31 Jul:

| Cluster | Date | Purpose | Outcome |
|---|---|---|---|
| `j-CS2V19449VW5` | 23 Jul | M1 EMR de-risk | ~1 h including the four §4 failures |
| `j-1SGHHVZLSRH8K` | 29 Jul | M2 batch layer | Spark batch view + Hadoop Streaming variant |
| `j-2JFD71W6KALYW` | 30 Jul | Experiment 1, attempt 1 | **Died at 26 min — Spot reclamation** (D30) |
| `j-1U8SPBNCV3U1P` | 30 Jul | Experiment 1 (On-Demand) | Spark sweep + MapReduce at 7 nodes |
| `j-0846384SERX5A5OL0U5` | 30 Jul | Sequential baseline | 1+1, ran alongside the sweep |
| `j-08215763I5JEY4IRNFK4` | 31 Jul | Auto-scaling demo | Scaled **1 → 3 → 5 → 4** (D34) |

### EMR auto-scaling — custom automatic scaling (D29, 30 Jul)

> **Supersedes the EMR-managed scaling configured on 29 Jul.** The two modes are mutually
> exclusive. Custom scaling was chosen because the rubric wants auto-scaling **with stated
> triggers**, and only custom scaling lets us state a metric and threshold we actually chose.

**Prerequisites** (both already satisfied — check before recreating a cluster):
- IAM role **`EMR_AutoScaling_DefaultRole`** must exist. It did *not* exist in this account
  before 30 Jul, and custom auto scaling is simply not selectable without it. Create with
  `aws emr create-default-roles`, or accept the console's offer when configuring the policy.
- Cluster must use **instance groups**, not instance fleets. `scp-siem-emr` uses instance groups.

Applied to the **core** instance group. Bounds: **min 1 / max 7** instances — deliberately the
same range as the 1/3/5/7 Experiment 1 sweep, so the policy and the benchmark exercise the same
hardware envelope.

| Rule | Metric | Condition | Action | Cooldown |
|---|---|---|---|---|
| Scale **out** | `ContainerPendingRatio` | ≥ 0.75 for 1 evaluation period of 300 s | **+2** instances | 300 s |
| Scale **in** | `YARNMemoryAvailablePercentage` | ≥ 75% for 1 evaluation period of 300 s | **−1** instance | 300 s |

**Why these choices — the wording the report needs:**

- `ContainerPendingRatio` for scale-out measures *unmet demand directly*: containers are queued
  that current capacity cannot run. A memory-percentage trigger only infers that indirectly.
- The rules are **deliberately asymmetric** (out by 2, in by 1). Removing a core node means
  decommissioning HDFS blocks and shuffle data, so growing fast and shrinking slowly is the safe
  direction under a scale-in mistake.
- **Stated tradeoff:** EMR publishes these metrics at roughly 5-minute granularity, so this policy
  reacts in *minutes*, where EMR-managed scaling reacts in 5–10 seconds. We accept slower reaction
  in exchange for explicit, defensible triggers, and say so rather than hiding it.

**Must be DISABLED during Experiment 1 (D28).** Any auto-scaling mode resizes the cluster mid-run,
which would make the core count an unknown variable and every timing meaningless.
`benchmarks/run_experiment1.py` refuses to start while a scaling policy is attached.

---

## 3. IAM roles

| Role | Purpose | Attached permissions |
|---|---|---|
| `scp-siem-lambda-speed-role` | Speed-layer Lambda execution | `AWSLambdaKinesisExecutionRole` (managed) + inline `scp-siem-ddb-write`: `dynamodb:UpdateItem`, **`dynamodb:Query`, `dynamodb:PutItem`** on the state table |
| `AmazonEMR-ServiceRole-20260723T165202` | EMR service role | `AmazonEMRServicePolicy_v2` + auto-generated customer-managed policy |
| `AmazonEMR-InstanceProfile-20260723T165145` | EMR EC2 instance profile | Auto-generated + inline `scp-siem-emr-s3-access` (see §4.2) |
| Firehose delivery role | Firehose → S3 | Console auto-created during stream setup |
| `AWSServiceRoleForEC2Spot` | Service-linked role for Spot | Account-level; created once (see §4.1) |
| `EMR_AutoScaling_DefaultRole` | Lets EMR resize instance groups from CloudWatch alarms | Required by custom auto scaling (D29). **Did not exist before 30 Jul** — `aws emr create-default-roles` |

**M2 update (28 Jul): `dynamodb:Query` + `dynamodb:PutItem` were added** to `scp-siem-ddb-write`.
`Query` backs the trailing-window sum and `PutItem` backs the conditional alert write; the M1
`UpdateItem`-only policy covered neither. If the window query ever throws
`AccessDeniedException`, this grant is the first thing to check.

**Note the two EMR roles have different timestamp suffixes** — service role `...T165202`, instance
profile `...T165145`. That looks like a typo but is correct; the console created them a few
seconds apart. Picking the wrong one is the §4.2 S3-403 failure mode, so copy from §7, don't
retype.

---

## 4. EMR runbook — the four things that broke, and their fixes

The first cluster took ~1 hour, almost all of it these four issues. Hitting any of them again at
M2/M4 should now cost minutes, not hours.

### 4.1 "Service-linked role 'AWSServiceRoleForEC2Spot' is required"

Cluster terminates ~1 minute after creation. The account had never launched a Spot instance, so
the service-linked role didn't exist and EMR couldn't create it. One-time, account-wide fix:

    aws iam create-service-linked-role --aws-service-name spot.amazonaws.com

(Already done for this account — should not recur. Alternative: use On-Demand core nodes.)

### 4.2 Step fails with S3 403 Forbidden

The EMR **instance profile** (not the service role — different role) lacked bucket access.
Inline policy `scp-siem-emr-s3-access` added to `AmazonEMR-InstanceProfile-*`:

    {
      "Version": "2012-10-17",
      "Statement": [
        { "Effect": "Allow",
          "Action": ["s3:GetObject","s3:PutObject","s3:DeleteObject"],
          "Resource": "arn:aws:s3:::scp-siem-data-009910375264/*" },
        { "Effect": "Allow",
          "Action": ["s3:ListBucket","s3:GetBucketLocation"],
          "Resource": "arn:aws:s3:::scp-siem-data-009910375264" }
      ]
    }

Both statements are required — the bucket-level `ListBucket` is what Spark uses to enumerate
files before reading. Missing it alone produces the 403.

### 4.3 "Failed to get main class in JAR ... (Is a directory)"

The step's **Application location** was a prefix, not the full object key. It must be the
complete path including the filename:

    s3://scp-siem-data-009910375264/scripts/smoke_test.py

Also: use **Deploy mode = Client** so driver `print()` output lands in the step's `stdout` log.
Cluster mode buries it in YARN container logs.

Reliable alternative that avoids console field ambiguity entirely — step type **Custom JAR**,
JAR location `command-runner.jar`, arguments:

    spark-submit --deploy-mode client s3://<bucket>/scripts/smoke_test.py s3://<bucket>/raw/

### 4.4 "[UNABLE_TO_INFER_SCHEMA] Unable to infer schema for JSON"

**Not a data problem.** Spark does not recurse into nested subdirectories by default, and
Firehose writes to `raw/yyyy/MM/dd/HH/`. Pointing Spark at `raw/` finds no files directly there.
Fix — required in every job that reads `raw/`:

    df = spark.read.option("recursiveFileLookup", "true").json(input_path)

---

## 5. Cluster relaunch procedure (M2 / M4)

1. EMR → Clusters → select the terminated `scp-siem-emr` → **Clone**. **Answer NO to "include
   steps"** — otherwise the whole historical step list (including the failures) replays.
2. Set core nodes to the starting tier (1 for the Experiment 1 sweep), and set the core group's
   purchasing option to **On-Demand** for any measurement run (D30). Spot reclamation terminated
   the first Experiment 1 attempt 26 minutes in; the whole sweep costs ~$1.92 On-Demand.
3. **Set the cluster scaling option deliberately.** For Experiment 1 turn scaling **off** (D28).
   For the auto-scaling demonstration select **custom automatic scaling** and apply the §2 policy.
   Scaling settings do **not** reliably survive a clone — always re-check this tab.
4. Verify the instance profile still carries `scp-siem-emr-s3-access`
5. Confirm auto-termination on idle is set
6. Submit step (Application location = full `.py` path, Deploy mode = Client). In the Arguments
   field, `spark-submit --deploy-mode client` must appear **exactly once** — cloning a step
   pre-fills it, and pasting a full command on top duplicates it, which fails with
   `File ... /spark-submit does not exist`.
7. **Terminate when done**

---

## 6. Teardown at end of project

Console: delete each resource in its own service page. Or:

    aws kinesis delete-stream --stream-name scp-siem-stream
    aws dynamodb delete-table --table-name scp-siem-speed-state
    aws firehose delete-delivery-stream --delivery-stream-name scp-siem-firehose
    aws lambda delete-function --function-name scp-siem-speed
    aws s3 rm s3://scp-siem-data-009910375264 --recursive
    aws s3 rb s3://scp-siem-data-009910375264

Then confirm in Billing → Cost Explorer (filter tag `Project=scp-siem`) that nothing is still
accruing, and check the EMR console shows no running clusters.



---

## 7. Exact identifiers (copy-paste reference)

Console-created names that are easy to mistype and slow to look up again.

| Thing | Value |
|---|---|
| S3 bucket | `scp-siem-data-009910375264` |
| Firehose stream | `scp-siem-firehose` |
| Firehose IAM role | `KinesisFirehoseServiceRole-scp-siem-fire-us-east-1-1784818029868` |
| EMR release | `emr-7.13.0` (Spark 3.5.6, Hadoop 3.4.2) |
| EMR service role | `AmazonEMR-ServiceRole-20260723T165202` |
| EMR instance profile | `AmazonEMR-InstanceProfile-20260723T165145` (confirmed 29 Jul) |
| — full ARN | `arn:aws:iam::009910375264:instance-profile/AmazonEMR-InstanceProfile-20260723T165145` |
| EMR subnet | `subnet-03ab5168599977eb2` (us-east-1c) — public subnet, no NAT gateway |