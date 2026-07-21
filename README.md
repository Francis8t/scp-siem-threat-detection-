# scp-siem — Real-Time SSH Threat-Detection Pipeline

A SIEM-lite real-time threat-detection system on a **Lambda architecture** (AWS),
built for the NCI *Scalable Cloud Programming* CA. Ingests a replayed stream of
OpenSSH auth events, detects brute-force / credential-stuffing behaviour live
(speed layer), reconciles it against each source IP's full-history reputation
(batch layer), and merges the two into a live priority-offender view (serving layer).

> **Real-time question:** *Which source IPs are exhibiting brute-force /
> credential-stuffing behaviour right now — exceeding an anomalous failed-auth rate
> over a 5-minute sliding window — and how does that reconcile against each IP's
> full-history reputation?*

## Repo layout
| Folder | Contents |
|---|---|
| `producer/`   | Paced replay producer (boto3 → Kinesis) |
| `speed/`      | Speed-layer Lambda (sliding-window count → DynamoDB) |
| `batch/`      | PySpark job + Hadoop Streaming MapReduce variant |
| `serving/`    | Athena DDL + merge logic (batch view ⋈ speed view) |
| `dashboard/`  | Streamlit dashboard |
| `benchmarks/` | Benchmark drivers, raw CSV results, figures |
| `report/`     | IEEE report sources |
| `infra-notes/`| Working IAM roles / policies / networking / commands |
| `fixtures/`   | OpenSSH_2k.log sample (committed; full log stays out of git) |

## Local setup
\`\`\`bash
python3 -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
\`\`\`

## Run instructions
_TODO: filled in as each component lands (producer → speed → batch → serving → dashboard)._

## Data
Loghub OpenSSH server log — 655,146 events, ~70 MiB, 28.4 days. The 2k sample lives
in \`fixtures/\`; the full \`OpenSSH.log\` is downloaded locally for S3 upload and kept
out of git. See \`PROJECT_PLAN.md\` §5.

**Citation:** Zhu, He, He, Liu, Lyu. *Loghub: A Large Collection of System Log Datasets
for AI-driven Log Analytics.* ISSRE 2023 (arXiv:2008.06448). github.com/logpai/loghub

## Cost guardrails
Personal AWS account, **\$80 ceiling** (budget alarm at \$40/\$80). Tear down EMR when
not benchmarking. No NAT gateway. Kinesis at 1 shard baseline. See \`PROJECT_PLAN.md\` §9.
