import subprocess
import time
import os

def get_kafka_container():
    # Dynamically find running kafka container name
    res = subprocess.run(
        ["docker", "ps", "--filter", "ancestor=confluentinc/cp-kafka", "--format", "{{.Names}}"],
        capture_output=True, text=True
    )
    names = res.stdout.strip().split('\n')
    if names and names[0]:
        return names[0]
    
    # Fallback search by string match
    res = subprocess.run(
        ["docker", "ps", "--format", "{{.Names}}"],
        capture_output=True, text=True
    )
    for name in res.stdout.strip().split('\n'):
        if "kafka" in name.lower():
            return name
    return None

def reset_all():
    print("🧹 [1/3] Clearing old Parquet files and checkpoints...")
    subprocess.run("rm -rf data/results/* checkpoint* data/results/.* 2>/dev/null", shell=True, check=False)
    os.makedirs("data/results", exist_ok=True)
    with open("data/results/.gitkeep", "w") as f:
        f.write("")
    
    kafka_container = get_kafka_container()
    if not kafka_container:
        print("⚠️ No running Kafka container found. Starting docker compose stack...")
        subprocess.run(["docker", "compose", "-f", "compose/docker-compose.yml", "up", "-d"], check=False)
        time.sleep(5)
        kafka_container = get_kafka_container()

    if kafka_container:
        print(f"🗑️ [2/3] Recreating Kafka topic 'raw-taxi-events' on container '{kafka_container}'...")
        # Delete topic if exists
        subprocess.run([
            "docker", "exec", kafka_container, "kafka-topics.sh",
            "--bootstrap-server", "localhost:9092",
            "--delete", "--topic", "raw-taxi-events"
        ], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        
        time.sleep(2)
        
        # Create fresh topic
        subprocess.run([
            "docker", "exec", kafka_container, "kafka-topics.sh",
            "--bootstrap-server", "localhost:9092",
            "--create", "--topic", "raw-taxi-events",
            "--partitions", "1", "--replication-factor", "1"
        ], check=False, stdout=subprocess.DEVNULL)
        print("✅ [3/3] Kafka topic reset and environment completely cleaned!")
    else:
        print("❌ Could not locate or start Kafka container. Please run 'docker compose up -d' manually.")

if __name__ == "__main__":
    reset_all()
