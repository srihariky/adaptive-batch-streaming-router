import subprocess
import time
import os
import signal
import pandas as pd
import requests

RESULTS_DIR = "data/results"
OS_SYSTEMS = {
    "stream_only": "systems/stream_only.py",
    "batch_only": "systems/batch_only.py",
    "fixed_lambda": "systems/fixed_lambda.py",
    "rule_router": "router/rule_router.py"
}

def clear_checkpoints():
    print("🧹 Cleaning results directory and checkpoints...")
    subprocess.run(["rm", "-rf", f"{RESULTS_DIR}/*"], check=False)
    os.makedirs(RESULTS_DIR, exist_ok=True)

def run_pipeline(system_name, script_path):
    print(f"\n==================================================")
    print(f"🚀 STARTING BENCHMARK RUN: {system_name.upper()}")
    print(f"==================================================")
    clear_checkpoints()
    
    # 1. Start System Listener Process
    sys_proc = subprocess.Popen(["python3", script_path])
    print(f"Waiting 10s for {system_name} Spark Session to initialize...")
    time.sleep(10)
    
    # 2. Run Replayer Stream
    print(f"▶️ Launching 3-Phase Replayer...")
    replayer_proc = subprocess.run(["python3", "replayer/replayer.py"], capture_output=True, text=True)
    print(replayer_proc.stdout)
    
    # 3. Allow streaming buffers to drain into Parquet sinks
    print("⏳ Draining micro-batch buffers (20s)...")
    time.sleep(20)
    
    # 4. Gracefully terminate system process
    print(f"🛑 Stopping {system_name} process...")
    sys_proc.send_signal(signal.SIGINT)
    try:
        sys_proc.wait(timeout=15)
    except subprocess.TimeoutExpired:
        sys_proc.kill()
        
    print(f"✅ Completed Run: {system_name}")

if __name__ == "__main__":
    print("Starting Automated Experiment Suite...")
    for sys_name, script in OS_SYSTEMS.items():
        run_pipeline(sys_name, script)
    print("\n🎉 ALL PILOT BENCHMARK RUNS COMPLETED SUCCESSFULLY!")
