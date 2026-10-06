import glob
import pandas as pd
import numpy as np

print("==================================================")
print("📊 FINAL 5-SYSTEM EXPERIMENTAL EVALUATION SUMMARY")
print("==================================================")

paths = {
    "Stream-Only": "data/results/stream_only_out/*.parquet",
    "Batch-Only": "data/results/batch_only_out/*.parquet",
    "Fixed-Lambda (Stream)": "data/results/lambda_stream_out/*.parquet",
    "Fixed-Lambda (Batch)": "data/results/lambda_batch_out/*.parquet",
    "Rule-Router (Stream)": "data/results/router_stream_out/*.parquet",
    "Rule-Router (Batch)": "data/results/router_batch_out/*.parquet",
    "ML-Router (Stream)": "data/results/ml_router_stream_out/*.parquet",
    "ML-Router (Batch)": "data/results/checkpoint_ml_batch_out/*.parquet"
}

summary = []

for name, pattern in paths.items():
    files = glob.glob(pattern)
    if files:
        dfs = [pd.read_parquet(f) for f in files]
        df = pd.concat(dfs, ignore_index=True)
        count = len(df)
        
        # Determine processing timestamp column
        time_col = 'spark_process_time' if 'spark_process_time' in df.columns else 'router_decision_time'
        
        if time_col in df.columns and 'event_creation_time' in df.columns:
            latencies = (df[time_col] - df['event_creation_time']) * 1000
            p50 = np.percentile(latencies, 50)
            p95 = np.percentile(latencies, 95)
            p99 = np.percentile(latencies, 99)
        else:
            p50, p95, p99 = np.nan, np.nan, np.nan
            
        summary.append({
            "System Architecture": name,
            "Total Records": count,
            "p50 Latency (ms)": round(p50, 2),
            "p95 Latency (ms)": round(p95, 2),
            "p99 Latency (ms)": round(p99, 2)
        })

df_sum = pd.DataFrame(summary)
print(df_sum.to_string(index=False))
