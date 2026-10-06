import glob
import pandas as pd
import numpy as np
from scipy.stats import mannwhitneyu

print("==========================================================================")
print("🧪 STATISTICAL SIGNIFICANCE TESTING (MANN-WHITNEY U TEST)")
print("==========================================================================")

paths = {
    "Stream-Only": "data/results/stream_only_out/*.parquet",
    "Batch-Only": "data/results/batch_only_out/*.parquet",
    "Fixed-Lambda": "data/results/lambda_stream_out/*.parquet",
    "Rule-Router": "data/results/router_stream_out/*.parquet",
    "ML-Router": "data/results/ml_router_stream_out/*.parquet"
}

datasets = {}

for name, pattern in paths.items():
    files = glob.glob(pattern)
    if files:
        dfs = [pd.read_parquet(f) for f in files]
        df = pd.concat(dfs, ignore_index=True)
        time_col = 'spark_process_time' if 'spark_process_time' in df.columns else 'router_decision_time'
        if time_col in df.columns and 'event_creation_time' in df.columns:
            latencies = (df[time_col] - df['event_creation_time']) * 1000
            datasets[name] = latencies.dropna().values

if "Rule-Router" not in datasets:
    print("❌ Error: Could not find Rule-Router results for comparison.")
    exit()

baseline_name = "Rule-Router"
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
print("📌 DISSERTATION INTERPRETATION NOTE:")
print("p < 0.001 indicates that the latency differences between the Rule-Based Router")
print("and the baselines are statistically significant and not due to random sample noise.")
print("==========================================================================")
