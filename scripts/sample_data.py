import pandas as pd
import numpy as np

print("1. Loading raw Yellow Taxi Parquet file...")
df = pd.read_parquet('data/yellow_tripdata_2026-01.parquet')
print(f"   -> Raw total records: {len(df):,}")

# Take a deterministic 250k sample using fixed seed 42
SEED = 42
SAMPLE_SIZE = min(250000, len(df))
df_sampled = df.sample(n=SAMPLE_SIZE, random_state=SEED).reset_index(drop=True)

print(f"2. Sampled {len(df_sampled):,} records using seed {SEED}.")

# Ensure required columns exist for urgency calculation
# Urgency signals: high fare, long distance, or high surge zone
df_sampled.to_parquet('data/sampled_taxi_data.parquet', index=False)
print("   -> Saved to data/sampled_taxi_data.parquet successfully!")
