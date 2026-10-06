import pandas as pd
import glob
import os

print("=== ADAPTIVE ROUTER VALIDATION REPORT ===")

stream_files = glob.glob("data/results/router_stream_out/*.parquet")
batch_files = glob.glob("data/results/router_batch_out/*.parquet")

if not stream_files and not batch_files:
    print("❌ No routed Parquet data found in data/results/. Run rule_router.py and replayer.py first.")
    exit()

# Load Stream Sink (Fast Path - Urgent)
df_stream = pd.concat([pd.read_parquet(f) for f in stream_files]) if stream_files else pd.DataFrame()

# Load Batch Sink (Thorough Path - Routine)
df_batch = pd.concat([pd.read_parquet(f) for f in batch_files]) if batch_files else pd.DataFrame()

total_stream = len(df_stream)
total_batch = len(df_batch)
total_routed = total_stream + total_batch

print(f"\n1. Volume Distribution:")
print(f"   - Stream Sink (Urgent) : {total_stream:,} records ({total_stream/max(1,total_routed)*100:.2f}%)")
print(f"   - Batch Sink  (Routine): {total_batch:,} records ({total_batch/max(1,total_routed)*100:.2f}%)")
print(f"   - Total Processed     : {total_routed:,} / 249,850 records")

print(f"\n2. Rule Accuracy Verification:")
# Urgent rules: priority == 1
if not df_stream.empty:
    stream_violations = (df_stream['priority'] != 1).sum()
    print(f"   - Stream Sink Rule Accuracy : {((total_stream - stream_violations)/total_stream)*100:.2f}% (Violations: {stream_violations})")

# Routine rules: priority == 0
if not df_batch.empty:
    batch_violations = (df_batch['priority'] != 0).sum()
    print(f"   - Batch Sink Rule Accuracy  : {((total_batch - batch_violations)/total_batch)*100:.2f}% (Violations: {batch_violations})")

print(f"\n3. Overhead & Latency Analysis:")
if not df_stream.empty and 'router_overhead_ms' in df_stream.columns:
    p50 = df_stream['router_overhead_ms'].median()
    p95 = df_stream['router_overhead_ms'].quantile(0.95)
    print(f"   - Router Decision Latency (p50): {p50:.2f} ms")
    print(f"   - Router Decision Latency (p95): {p95:.2f} ms")

if stream_violations == 0 and batch_violations == 0 and total_routed > 0:
    print("\n✅ VALIDATION SUCCESS: 100% Routing Accuracy across Stream and Batch Sinks!")
else:
    print("\n⚠️ VALIDATION FAILED: Misrouted records or missing records detected.")
