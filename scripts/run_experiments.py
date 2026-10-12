import argparse, glob, json, os, random, signal, subprocess, sys, time
import pyarrow.dataset as ds
import pyarrow.parquet as pq

ALL = {"stream_only": "systems/stream_only.py", "batch_only": "systems/batch_only.py",
       "fixed_lambda": "systems/fixed_lambda.py", "rule_router": "router/rule_router.py",
       "ml_router": "router/ml_router.py"}
KAFKA_BIN = "/opt/kafka/bin/kafka-topics.sh"

ap = argparse.ArgumentParser()
ap.add_argument("--base", default="data/results")
ap.add_argument("--systems", default=",".join(ALL))
ap.add_argument("--repeats", type=int, default=5)
ap.add_argument("--sample", default="data/sampled_taxi_data.parquet")
ap.add_argument("--seed", type=int, default=42)
args = ap.parse_args()

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib"))
try:
    from resource_sampler import ResourceSampler
except ImportError:
    ResourceSampler = None

def kafka_container():
    out = subprocess.run(["docker", "ps", "--filter", "ancestor=apache/kafka:3.7.0", "-q"],
                         capture_output=True, text=True).stdout.split()
    if not out:
        sys.exit("Kafka container not running")
    return out[0]

def create_topic(cid, topic):
    subprocess.run(["docker", "exec", cid, KAFKA_BIN, "--bootstrap-server", "localhost:9092", "--create",
                    "--topic", topic, "--partitions", "1", "--replication-factor", "1"],
                   check=True, capture_output=True)

def count_rows(run_dir):
    n = 0
    for d in glob.glob(f"{run_dir}/*_out"):
        try:
            n += ds.dataset(d, format="parquet").count_rows()
        except Exception:
            pass
    return n

def drain(run_dir, min_wait=45, max_wait=240):
    t0, last, stable = time.time(), -1, 0
    while time.time() - t0 < max_wait:
        time.sleep(10)
        n = count_rows(run_dir)
        stable = stable + 1 if n == last else 0
        last = n
        if time.time() - t0 >= min_wait and stable >= 4:
            break
    return last

def run_one(cid, system, rep, expected):
    run_id = f"{system}_r{rep}"
    run_dir = f"{args.base}/{run_id}"
    if os.path.exists(f"{run_dir}/DONE"):
        print(f"skip {run_id}"); return
    os.makedirs(run_dir, exist_ok=True)
    topic = f"taxi_{run_id}_{int(time.time())}"
    create_topic(cid, topic)
    env = {**os.environ, "TOPIC": topic, "RUN_DIR": run_dir, "SAMPLE_PATH": args.sample,
           "SPARK_LOCAL_IP": "127.0.0.1"}
    print(f"\n=== {run_id} | topic {topic} ===", flush=True)
    proc = subprocess.Popen(["python3", ALL[system]], env=env)
    sampler = None
    if ResourceSampler:
        sampler = ResourceSampler(target_pid=proc.pid, interval=1.0, output_csv=f"{run_dir}/resources.csv", label=run_id)
        sampler.start()
    time.sleep(15)
    rep_out = subprocess.run(["python3", "replayer/replayer.py"], env=env, capture_output=True, text=True)
    open(f"{run_dir}/replayer.log", "w").write(rep_out.stdout + rep_out.stderr)
    rows = drain(run_dir)
    if sampler:
        res_stats = sampler.stop()
        json.dump(res_stats, open(f"{run_dir}/resource_summary.json", "w"), indent=2)
        try: sampler.plot(f"{run_dir}/resources.png")
        except Exception: pass
    proc.send_signal(signal.SIGINT)
    try: proc.wait(timeout=20)
    except subprocess.TimeoutExpired: proc.kill()
    json.dump({"system": system, "rep": rep, "topic": topic, "rows": rows}, open(f"{run_dir}/meta.json", "w"))
    open(f"{run_dir}/DONE", "w").close()
    flag = "" if (abs(rows - expected) <= 0.01 * expected or abs(rows - 2 * expected) <= 0.02 * expected) else "   <-- WARNING: unexpected row count"
    print(f"done {run_id}: {rows:,} rows (sample = {expected:,}){flag}", flush=True)

if __name__ == "__main__":
    systems = args.systems.split(",")
    if "ml_router" in systems and not os.path.exists("data/ml_sql_expr.txt"):
        sys.exit("Train the ML router first: python3 scripts/train_ml_model.py data/pilot")
    expected = pq.ParquetFile(args.sample).metadata.num_rows
    cid = kafka_container()
    jobs = [(s, r) for r in range(1, args.repeats + 1) for s in systems]
    random.Random(args.seed).shuffle(jobs)
    for s, r in jobs:
        run_one(cid, s, r, expected)
    print("ALL RUNS COMPLETE")
