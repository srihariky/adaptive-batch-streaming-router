import os
import time
import json
import random
import pandas as pd
from kafka import KafkaProducer

print("Starting NYC Taxi Data Replayer with 3-Phase Load Profile...")

# Load sampled data
df = pd.read_parquet('data/sampled_taxi_data.parquet')
records = df.to_dict(orient='records')
total_records = len(records)

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
    acks=0 # High-throughput streaming
)

# Load profile definitions (target rates in rec/sec and duration in sec)
phases = [
    {"name": "STEADY", "rate": 1000, "duration": 90},
    {"name": "BURST",  "rate": 5000, "duration": 20},
    {"name": "CALM",   "rate": 500,  "duration": 120}
]

idx = 0
global_start = time.time()

for phase in phases:
    p_name = phase["name"]
    target_rate = phase["rate"]
    duration = phase["duration"]
    
    print(f"\n--- Entering Phase: {p_name} | Target: {target_rate} rec/s | Duration: {duration}s ---")
    
    phase_start = time.time()
    sent_in_phase = 0
    
    while time.time() - phase_start < duration and idx < total_records:
        batch_start = time.time()
        # Emit in small 100ms micro-batches to keep pace
        batch_target = int(target_rate * 0.1) 
        
        for _ in range(batch_target):
            if idx >= total_records:
                break
                
            row = records[idx]
            idx += 1
            sent_in_phase += 1
            
            # Compute operational urgency tag (1 = Urgent, 0 = Routine)
            # High fare (> $50) or long distance (> 15 miles) = Urgent
            fare = float(row.get('fare_amount', 0) or 0)
            dist = float(row.get('trip_distance', 0) or 0)
            priority = 1 if (fare > 50.0 or dist > 15.0 or random.random() < 0.1) else 0

            payload = {
                "record_id": idx,
                "vendor_id": row.get('VendorID'),
                "pickup_datetime": str(row.get('tpep_pickup_datetime')),
                "passenger_count": row.get('passenger_count'),
                "trip_distance": dist,
                "pickup_location_id": row.get('PULocationID'),
                "dropoff_location_id": row.get('DOLocationID'),
                "fare_amount": fare,
                "priority": priority,
                "event_creation_time": time.time()  # Timestamp 1
            }
            
            producer.send(os.environ['TOPIC'], value=payload)
        
        elapsed = time.time() - batch_start
        sleep_time = 0.1 - elapsed
        if sleep_time > 0:
            time.sleep(sleep_time)
            
    actual_phase_time = time.time() - phase_start
    print(f"Phase {p_name} Complete: Sent {sent_in_phase:,} records in {actual_phase_time:.2f}s (Actual Rate: {sent_in_phase/actual_phase_time:.0f} rec/s)")

producer.flush()
print(f"\nReplayer Finished! Total {idx:,} records streamed to Kafka os.environ['TOPIC'] in {time.time()-global_start:.2f}s.")
