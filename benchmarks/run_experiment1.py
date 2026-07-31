"""
Experiment 1 driver — batch layer, sequential vs parallel (M4).

Sweeps the EMR core-node count, runs the Spark batch job several times at each
size, and writes a CSV of timings for the speedup and efficiency plots.

Why a driver script rather than console clicking: the sweep is 4 sizes x
(1 warm-up + 3 measured) = 16 step submissions. Doing that by hand is slow and
easy to get subtly wrong — a mistyped argument or a forgotten warm-up discard
silently corrupts the curve. This also records exactly what was run, which is
what makes the numbers defensible in the report.

It RESIZES one running cluster rather than launching a cluster per size. A
resize provisions instances in a couple of minutes; a fresh cluster costs a
full ~8-minute bootstrap each time.

--------------------------------------------------------------------------
BEFORE YOU RUN THIS — turn EMR auto-scaling OFF (D28)
--------------------------------------------------------------------------
This project uses custom automatic scaling (D29). Any scaling mode resizes the
cluster while the job runs, which makes the core count an unknown drifting
variable and every timing here meaningless. Detach the policy, run the sweep,
then re-attach it and capture the auto-scaling demonstration separately. The
script refuses to start while either scaling mechanism is active.

Timing: `elapsed_seconds` is what the Spark job itself reports — the read,
shuffle and aggregation, measured driver-side, excluding SparkSession creation
and the parquet write. That is the figure the plots use. `step_wall_seconds`
is the full step lifetime for reference; it includes JVM startup and the write,
so it is consistently larger.

Usage:
    python3 benchmarks/run_experiment1.py --cluster-id j-XXXXXXXX --dry-run
    python3 benchmarks/run_experiment1.py --cluster-id j-XXXXXXXX
"""

import argparse
import csv
import gzip
import io
import re
import statistics
import time

import boto3

REGION = "us-east-1"
BUCKET = "scp-siem-data-009910375264"
SCRIPT = f"s3://{BUCKET}/scripts/spark_batch.py"
INPUT = f"s3://{BUCKET}/benchmark-input/"
OUTPUT = f"s3://{BUCKET}/batch-views/bench-scratch/"

RE_ELAPSED = re.compile(r"ELAPSED_SECONDS:\s*([\d.]+)")
RE_RECORDS = re.compile(r"records aggregated\s*:\s*(\d+)")


def core_group(emr, cluster_id):
    """The CORE instance group — the one we resize. TASK/MASTER are left alone."""
    for g in emr.list_instance_groups(ClusterId=cluster_id)["InstanceGroups"]:
        if g["InstanceGroupType"] == "CORE":
            return g
    raise SystemExit("no CORE instance group found — is this the right cluster?")


def assert_no_autoscaling(emr, cluster_id):
    """D28: refuse to produce numbers that would be silently invalid.

    Checks BOTH scaling mechanisms. They are mutually exclusive, but either one
    resizes the cluster mid-run, which makes the core count an unknown variable
    and every timing here meaningless. This project uses custom automatic
    scaling (D29); the managed-scaling check remains because a cloned cluster
    can carry the older configuration forward.
    """
    try:
        policy = emr.get_managed_scaling_policy(ClusterId=cluster_id)
        if policy.get("ManagedScalingPolicy"):
            raise SystemExit(
                "EMR MANAGED scaling is enabled on this cluster (D28).\n"
                "Disable it: Instances (Hardware) -> Edit cluster scaling option.")
    except SystemExit:
        raise
    except Exception:
        pass                                     # no managed policy set

    core = core_group(emr, cluster_id)
    if core.get("AutoScalingPolicy"):
        state = core["AutoScalingPolicy"].get("Status", {}).get("State", "")
        if state not in ("DETACHED", "DETACHING", "FAILED"):
            raise SystemExit(
                f"CUSTOM auto scaling is attached to the core instance group "
                f"(state: {state or 'ATTACHED'}) — D28.\n"
                "It will resize the cluster mid-run and invalidate every timing.\n"
                "Remove the policy for the sweep, then re-attach it for the\n"
                "auto-scaling demonstration:\n"
                f"    aws emr remove-auto-scaling-policy --cluster-id {cluster_id} "
                f"--instance-group-id {core['Id']}")


def wait_for_size(emr, cluster_id, target, timeout=1800, poll=20):
    """Block until the CORE group actually has `target` instances running."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        g = core_group(emr, cluster_id)
        running = g.get("RunningInstanceCount", 0)
        state = g["Status"]["State"]
        if running == target and state == "RUNNING":
            return
        print(f"    ... core group {state}, {running}/{target} running", flush=True)
        time.sleep(poll)
    raise SystemExit(f"core group did not reach {target} instances in {timeout}s")


def resize(emr, cluster_id, target):
    g = core_group(emr, cluster_id)
    if g.get("RunningInstanceCount") == target and g["Status"]["State"] == "RUNNING":
        print(f"  core group already at {target}")
        return
    print(f"  resizing core group {g['RequestedInstanceCount']} -> {target}")
    emr.modify_instance_groups(
        ClusterId=cluster_id,
        InstanceGroups=[{"InstanceGroupId": g["Id"], "InstanceCount": target}])
    wait_for_size(emr, cluster_id, target)


def submit(emr, cluster_id, nodes, run_index, input_path, output_path):
    resp = emr.add_job_flow_steps(
        JobFlowId=cluster_id,
        Steps=[{
            "Name": f"exp1-n{nodes}-run{run_index}",
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": ["spark-submit", "--deploy-mode", "client",
                         SCRIPT, input_path, output_path],
            },
        }])
    return resp["StepIds"][0]


def wait_for_step(emr, cluster_id, step_id, poll=15):
    started = time.perf_counter()
    while True:
        s = emr.describe_step(ClusterId=cluster_id, StepId=step_id)["Step"]
        state = s["Status"]["State"]
        if state in ("COMPLETED", "FAILED", "CANCELLED", "INTERRUPTED"):
            return state, time.perf_counter() - started
        time.sleep(poll)


def step_stdout(s3, log_uri, cluster_id, step_id, retries=10, delay=15):
    """EMR uploads step logs to S3 asynchronously, so poll for the object."""
    prefix = log_uri.replace("s3n://", "").replace("s3://", "").rstrip("/")
    bucket, _, base = prefix.partition("/")
    key = f"{base}/{cluster_id}/steps/{step_id}/stdout.gz"
    for _ in range(retries):
        try:
            body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
            return gzip.GzipFile(fileobj=io.BytesIO(body)).read().decode("utf-8", "replace")
        except s3.exceptions.NoSuchKey:
            time.sleep(delay)
    return ""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cluster-id", required=True)
    ap.add_argument("--sizes", default="1,3,5,7",
                    help="core-node counts to sweep (default 1,3,5,7)")
    ap.add_argument("--runs", type=int, default=3, help="measured runs per size")
    ap.add_argument("--warmup", type=int, default=1,
                    help="discarded runs per size (default 1) — the first run pays "
                         "for cold JVMs and S3 metadata caching")
    ap.add_argument("--input", default=INPUT)
    ap.add_argument("--output", default=OUTPUT)
    ap.add_argument("--out", default="benchmarks/experiment1_raw.csv")
    ap.add_argument("--dry-run", action="store_true",
                    help="print the plan and exit without touching the cluster")
    args = ap.parse_args()

    sizes = [int(s) for s in args.sizes.split(",")]
    total = len(sizes) * (args.warmup + args.runs)

    print(f"Experiment 1 — {len(sizes)} sizes x ({args.warmup} warm-up + "
          f"{args.runs} measured) = {total} step submissions")
    print(f"  input : {args.input}")
    print(f"  script: {SCRIPT}")
    for n in sizes:
        print(f"    n={n}: resize core group, then {args.warmup + args.runs} runs")
    if args.dry_run:
        print("\n--dry-run: nothing submitted.")
        return

    emr = boto3.client("emr", region_name=REGION)
    s3 = boto3.client("s3", region_name=REGION)

    assert_no_autoscaling(emr, args.cluster_id)
    log_uri = emr.describe_cluster(ClusterId=args.cluster_id)["Cluster"]["LogUri"]

    rows = []
    for nodes in sizes:
        print(f"\n=== {nodes} core node(s) ===", flush=True)
        resize(emr, args.cluster_id, nodes)

        for i in range(args.warmup + args.runs):
            is_warmup = i < args.warmup
            tag = "warm-up" if is_warmup else f"run {i - args.warmup + 1}"
            print(f"  {tag} ...", end=" ", flush=True)

            step_id = submit(emr, args.cluster_id, nodes, i, args.input, args.output)
            state, wall = wait_for_step(emr, args.cluster_id, step_id)
            if state != "COMPLETED":
                print(f"FAILED ({state}) — see the step log")
                rows.append({"nodes": nodes, "run": i, "warmup": is_warmup,
                             "elapsed_seconds": "", "step_wall_seconds": round(wall, 1),
                             "records": "", "state": state, "step_id": step_id})
                continue

            out = step_stdout(s3, log_uri, args.cluster_id, step_id)
            m_elapsed = RE_ELAPSED.search(out)
            m_records = RE_RECORDS.search(out)
            elapsed = float(m_elapsed.group(1)) if m_elapsed else None
            records = int(m_records.group(1)) if m_records else None

            print(f"elapsed={elapsed}s wall={wall:.0f}s records={records}")
            rows.append({"nodes": nodes, "run": i, "warmup": is_warmup,
                         "elapsed_seconds": elapsed, "step_wall_seconds": round(wall, 1),
                         "records": records, "state": state, "step_id": step_id})

            with open(args.out, "w", newline="") as fh:      # write as we go
                w = csv.DictWriter(fh, fieldnames=list(rows[0]))
                w.writeheader()
                w.writerows(rows)

    # --- summary ------------------------------------------------------------
    print(f"\nraw results -> {args.out}\n")
    medians = {}
    for nodes in sizes:
        vals = [r["elapsed_seconds"] for r in rows
                if r["nodes"] == nodes and not r["warmup"] and r["elapsed_seconds"]]
        if vals:
            medians[nodes] = statistics.median(vals)

    if not medians:
        print("no successful measured runs — check the step logs")
        return

    baseline = medians.get(sizes[0])
    print(f"{'nodes':>6} {'median T(N)':>12} {'speedup':>9} {'efficiency':>11}")
    print("-" * 42)
    for nodes in sizes:
        if nodes not in medians:
            continue
        t = medians[nodes]
        speedup = baseline / t
        print(f"{nodes:>6} {t:>11.1f}s {speedup:>8.2f}x {speedup / nodes:>10.1%}")

    summary_path = args.out.replace("_raw.csv", "_summary.csv")
    with open(summary_path, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["nodes", "median_seconds", "speedup", "efficiency"])
        for nodes in sizes:
            if nodes in medians:
                sp = baseline / medians[nodes]
                w.writerow([nodes, round(medians[nodes], 2), round(sp, 3),
                            round(sp / nodes, 3)])
    print(f"\nsummary -> {summary_path}")
    print("Efficiency below 100% is expected and is the point: shuffle and "
          "scheduling do not parallelise (Amdahl).")


if __name__ == "__main__":
    main()
