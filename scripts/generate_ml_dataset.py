import pandas as pd
import glob

print("📊 Generating ML Router Training Dataset from Pilot Run Outputs...")

# Read routed outputs
stream_files = glob.glob("data/results/router_stream_out/*.parquet") + glob.glob("data/results/stream_only_out/*.parquet")
batch_files = glob.glob("data/results/router_batch_out/*.parquet") + glob.glob("data/results/batch_only_out/*.parquet")

records = []

for f in stream_files:
    df = pd.read_parquet(f)
    if not df.empty:
        df['optimal_route'] = 1  # Oracle decision: Stream
        records.append(df)

for f in batch_files:
    df = pd.read_parquet(f)
    if not df.empty:
        df['optimal_route'] = 0  # Oracle decision: Batch
        records.append(df)

if records:
    full_df = pd.concat(records, ignore_index=True)
    # Extract features for ML model training
    feature_cols = ['trip_distance', 'fare_amount', 'passenger_count', 'priority', 'optimal_route']
    ml_df = full_df[[c for c in feature_cols if c in full_df.columns]].dropna()
    
    ml_df.to_csv("data/ml_training_dataset.csv", index=False)
    print(f"✅ Dataset generated with {len(ml_df):,} records saved to 'data/ml_training_dataset.csv'!")
else:
    print("⚠️ No data files found to build ML dataset.")
