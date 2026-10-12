import os, time, json, random
from collections import deque
import pandas as pd
from kafka import KafkaProducer

TOPIC = os.environ["TOPIC"]
SAMPLE = os.environ.get("SAMPLE_PATH", "data/sampled_taxi_data.parquet")
BOOTSTRAP = os.environ.get("BOOTSTRAP", "localhost:9092")
rng = random.Random(42)                      # seeded: identical ground truth in every run

print(f"Replayer: {SAMPLE} -> topic {TOPIC}")
records = pd.read_parquet(SAMPLE).to_dict(orient="records")
total = len(records)
producer = KafkaProducer(bootstrap_servers=BOOTSTRAP, acks=0,
                         value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"))

SCALE = float(os.environ.get("DEMO_SCALE", "1.0"))
phases = [("STEADY", 1000, 90 * SCALE), ("BURST", 5000, 20 * SCALE), ("CALM", 500, 120 * SCALE)]
window = deque()                             # (time, count) of sends in the last second

def trailing_rate(now):
    while window and now - window[0][0] > 1.0:
        window.popleft()
    return float(sum(n for _, n in window))

idx, t0 = 0, time.time()
for name, rate, dur in phases:
    print(f"--- {name}: target {rate} rec/s for {dur}s")
    p0, sent = time.time(), 0
    while time.time() - p0 < dur and idx < total:
        b0 = time.time()
        obs_rate = trailing_rate(b0)
        n = int(rate * 0.1)
        for _ in range(n):
            if idx >= total:
                break
            row = records[idx]; idx += 1; sent += 1
            fare = float(row.get("fare_amount", 0) or 0)
            dist = float(row.get("trip_distance", 0) or 0)
            fpm = fare / max(dist, 0.1)
            priority = 1 if (fare > 50.0 or dist > 15.0 or (fpm > 15.0 and dist > 1.0)) else 0
            producer.send(TOPIC, value={
                "record_id": idx, "vendor_id": row.get("VendorID"),
                "pickup_datetime": str(row.get("tpep_pickup_datetime")),
                "passenger_count": row.get("passenger_count"), "trip_distance": dist,
                "pickup_location_id": row.get("PULocationID"), "dropoff_location_id": row.get("DOLocationID"),
                "fare_amount": fare, "priority": priority, "arrival_rate": obs_rate,
                "event_creation_time": time.time()})
        window.append((b0, n))
        left = 0.1 - (time.time() - b0)
        if left > 0:
            time.sleep(left)
    el = time.time() - p0
    print(f"Phase {name} Complete: Sent {sent:,} records in {el:.2f}s (Actual Rate: {sent/el:.0f} rec/s)")

producer.flush()
print(f"Replayer finished: {idx:,} records in {time.time()-t0:.1f}s")
