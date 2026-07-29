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

**Cost:** ~$0.35/hour. **Terminate immediately after every use.** First de-risk run
(23 Jul, cluster `j-CS2V19449VW5`) took ~1 hour including troubleshooting. Second cluster
(29 Jul, `j-1SGHHVZLSRH8K`) ran the M2 batch layer — Spark batch view + Hadoop Streaming variant.

### EMR managed scaling (configured 29 Jul — the M2 auto-scaling deliverable)

| Setting | Value |
|---|---|
| Cluster scaling option | **EMR-managed scaling** |
| Minimum cluster size | 1 instance |
| Maximum cluster size | 7 instances |
| Maximum core nodes | 7 instances |
| Maximum On-Demand instances | 1 (the primary; core nodes run Spot) |

Max of 7 deliberately matches the **1 / 3 / 5 / 7** core-node sweep Experiment 1 runs, so the
scaling policy and the benchmark exercise the same range.

**The trigger, stated for the report** (the rubric wants auto-scaling *with stated triggers*, not
merely enabled): EMR managed scaling does not use a hand-written CloudWatch alarm. It evaluates
**YARN resource pressure** on a ~5–10 second loop and scales **out** when pending container
demand exceeds what running nodes can satisfy, and **in** when nodes go idle — bounded by the
min/max unit limits above, with scale-in protection so nodes holding shuffle data are not
reclaimed mid-job.

---

## 3. IAM roles

| Role | Purpose | Attached permissions |
|---|---|---|
| `scp-siem-lambda-speed-role` | Speed-layer Lambda execution | `AWSLambdaKinesisExecutionRole` (managed) + inline `scp-siem-ddb-write`: `dynamodb:UpdateItem`, **`dynamodb:Query`, `dynamodb:PutItem`** on the state table |
| `AmazonEMR-ServiceRole-20260723T165202` | EMR service role | `AmazonEMRServicePolicy_v2` + auto-generated customer-managed policy |
| `AmazonEMR-InstanceProfile-20260723T165145` | EMR EC2 instance profile | Auto-generated + inline `scp-siem-emr-s3-access` (see §4.2) |
| Firehose delivery role | Firehose → S3 | Console auto-created during stream setup |
| `AWSServiceRoleForEC2Spot` | Service-linked role for Spot | Account-level; created once (see §4.1) |

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

1. EMR → Clusters → select the terminated `scp-siem-emr` → **Clone** (preserves all config)
2. Adjust instance counts for the benchmark tier (1 / 3 / 5 / 7 core nodes)
3. Verify the instance profile still carries `scp-siem-emr-s3-access`
4. Confirm auto-termination on idle is set
5. Submit step (Application location = full `.py` path, Deploy mode = Client)
6. **Terminate when done**

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