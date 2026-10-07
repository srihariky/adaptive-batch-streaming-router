import subprocess, time, os, signal, random, json, sys, glob
import pyarrow.dataset as ds

REPEATS, SEED, BASE = 1, 42, "data/results"
SYSTEMS = {
    "stream_only": "systems/stream_only.py",
    "batch_only": "systems/batch_only.py",
    "fixed_lambda": "systems/fixed_lambda.py",
    "rule_router": "router/rule_router.py",
    "ml_router": "router/ml_router.py",
}
KAFKA_BIN = "/opt/kafka/bin/kafka-topics.sh"

def kafka_container():
    out = subprocess.run(["docker", "ps", "--filter", "ancestor=confluentinc/cp-kafka", "-q"],
                         capture_output=True, text=True).stdout.split()
    if not out:
        # Fallback if confluentinc image is not used
        out = subprocess.run(["docker", "ps", "--filter", "name=kafka", "-q"],
                             capture_output=True, text=True).stdout.split()
        if not out:
            sys.exit("Kafka container not running")
    return out[0]

def create_topic(cid, topic):
    # Determine the correct path to kafka-topics.sh based on the container
    check_bin = subprocess.run(["docker", "exec", cid, "which", "/opt/kafka/bin/kafka-topics.sh"], capture_output=True, text=True)
    bin_path = check_bin.stdout.strip() if check_bin.stdout.strip() else "/opt/kafka/bin/kafka-topics.sh"
    
    subprocess.run(["docker", "exec", cid, bin_path, "--bootstrap-server", "localhost:9092",
                    "--create", "--topic", topic, "--partitions", "1", "--replication-factor", "1"],
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
        if time.time() - t0 >= min_wait and stable >= 4:   # 40s unchanged > 30s batch trigger
            break
    return last

def run_one(cid, system, script, rep):
    run_id = f"{system}_r{rep}"
    run_dir = f"{BASE}/{run_id}"
    if os.path.exists(f"{run_dir}/DONE"):
        print(f"skip {run_id}"); return
    os.makedirs(run_dir, exist_ok=True)
    topic = f"taxi_{run_id}_{int(time.time())}"
    create_topic(cid, topic)
    env = {**os.environ, "TOPIC": topic, "RUN_DIR": run_dir}
    print(f"\n=== {run_id} | topic {topic} ===")
    proc = subprocess.Popen(["python3", script], env=env)
    time.sleep(15)                                   # Spark init
    rep_out = subprocess.run(["python3", "replayer/replayer.py"], env=env, capture_output=True, text=True)
    open(f"{run_dir}/replayer.log", "w").write(rep_out.stdout + rep_out.stderr)
    rows = drain(run_dir)
    proc.send_signal(signal.SIGINT)
    try: proc.wait(timeout=20)
    except subprocess.TimeoutExpired: proc.kill()
    json.dump({"system": system, "rep": rep, "topic": topic, "rows": rows}, open(f"{run_dir}/meta.json", "w"))
    open(f"{run_dir}/DONE", "w").close()
    print(f"done {run_id}: {rows:,} rows")

if __name__ == "__main__":
    subprocess.run(["python3", "scripts/train_ml_model.py"], check=True)
    cid = kafka_container()
    jobs = [(s, p, r) for r in range(1, REPEATS + 1) for s, p in SYSTEMS.items()]
    random.Random(SEED).shuffle(jobs)               # randomised order
    for s, p, r in jobs:
        run_one(cid, s, p, r)
    print("ALL RUNS COMPLETE")
