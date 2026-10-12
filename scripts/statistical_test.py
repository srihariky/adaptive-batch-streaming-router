import glob
import os
import sys
import pandas as pd
import numpy as np
from scipy.stats import mannwhitneyu

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

print("==========================================================================")
print("[STATISTICAL SIGNIFICANCE TESTING: MANN-WHITNEY U TEST]")
print("==========================================================================")

import sys
base = sys.argv[1] if len(sys.argv) > 1 else ("data/final_run" if os.path.exists("data/final_run") else "data/results")

paths = {
    "Stream-Only": [f"{base}/*_r*/stream_only_out/*.parquet", f"{base}/stream_only_out/*.parquet"],
    "Batch-Only": [f"{base}/*_r*/batch_only_out/*.parquet", f"{base}/batch_only_out/*.parquet"],
    "Fixed-Lambda": [f"{base}/*_r*/lambda_stream_out/*.parquet", f"{base}/lambda_stream_out/*.parquet"],
    "Rule-Router": [f"{base}/*_r*/router_stream_out/*.parquet", f"{base}/router_stream_out/*.parquet"],
    "ML-Router": [f"{base}/*_r*/ml_router_stream_out/*.parquet", f"{base}/ml_router_stream_out/*.parquet"],
}

datasets = {}

for name, pattern_list in paths.items():
    files = []
    for pat in pattern_list:
        files.extend(glob.glob(pat))
    if files:
        dfs = [pd.read_parquet(f) for f in files]
        df = pd.concat(dfs, ignore_index=True)
        time_col = 'spark_process_time' if 'spark_process_time' in df.columns else 'router_decision_time'
        if time_col in df.columns and 'event_creation_time' in df.columns:
            latencies = (df[time_col] - df['event_creation_time']) * 1000
            datasets[name] = latencies.dropna().values

baseline_name = "ML-Router" if "ML-Router" in datasets else ("Rule-Router" if "Rule-Router" in datasets else None)
if not baseline_name:
    print(f"Error: Could not find ML-Router or Rule-Router results in {base}.")
    exit()

baseline_data = datasets[baseline_name]

print(f"Comparing Baseline: [{baseline_name}] (N = {len(baseline_data):,}) against other architectures:\n")

results = []

for name, data in datasets.items():
    if name == baseline_name:
        continue
    
    # Perform two-sided Mann-Whitney U test
    stat, p_value = mannwhitneyu(baseline_data, data, alternative='two-sided')
    
    # Calculate effect size (Rank-Biserial Correlation)
    n1, n2 = len(baseline_data), len(data)
    u_stat = stat
    r_rb = 1 - (2 * u_stat) / (n1 * n2)
    
    significant = "YES (p < 0.001)" if p_value < 0.001 else ("YES (p < 0.05)" if p_value < 0.05 else "NO")
    
    results.append({
        "Comparison": f"{baseline_name} vs {name}",
        "U Statistic": f"{stat:.2e}",
        "p-value": f"{p_value:.2e}" if p_value > 0 else "< 1.0e-300",
        "Statistically Significant": significant,
        "Effect Size (r_rb)": round(r_rb, 4)
    })

df_res = pd.DataFrame(results)
print(df_res.to_string(index=False))

print("\n==========================================================================")
print("[DISSERTATION INTERPRETATION NOTE]:")
print("p < 0.001 indicates that the latency differences between the ML/Rule Router")
print("and the baselines are statistically significant and not due to random sample noise.")
print("==========================================================================")
