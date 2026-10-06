import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

# Empirical Benchmark Data
data = {
    'Architecture': [
        'Stream-Only', 'Batch-Only', 
        'Fixed-Lambda\n(Stream)', 'Fixed-Lambda\n(Batch)',
        'Rule-Router\n(Stream)', 'Rule-Router\n(Batch)',
        'ML-Router\n(Stream)', 'ML-Router\n(Batch)'
    ],
    'Records': [1249150, 1499000, 1748850, 1744950, 324834, 1673361, 446945, 2301305],
    'p50_Latency_s': [222.32, 640.63, 806.78, 807.17, 256.04, 290.53, 1313.24, 1313.52],
    'p95_Latency_s': [733.27, 12104.68, 12355.23, 12355.43, 11005.18, 11005.20, 13497.26, 13497.27]
}

df = pd.DataFrame(data)

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

# Chart 1: Median Latency (p50) Comparison
colors = ['#2b5c8f', '#d95f02', '#7570b3', '#7570b3', '#1b9e77', '#1b9e77', '#e7298a', '#e7298a']
bars1 = ax1.barh(df['Architecture'], df['p50_Latency_s'], color=colors)
ax1.set_xlabel('Median Latency p50 (Seconds)')
ax1.set_title('p50 Latency Comparison Across Systems (Lower is Better)')
ax1.grid(axis='x', linestyle='--', alpha=0.7)

for bar in bars1:
    width = bar.get_width()
    ax1.text(width + 15, bar.get_y() + bar.get_height()/2, f'{width:.1f}s', ha='left', va='center', fontsize=9)

# Chart 2: Total Volume Processed Per Sink
bars2 = ax2.barh(df['Architecture'], df['Records'] / 1e6, color=colors)
ax2.set_xlabel('Total Records Processed (Millions)')
ax2.set_title('System Throughput & Record Offloading Volume')
ax2.grid(axis='x', linestyle='--', alpha=0.7)

for bar in bars2:
    width = bar.get_width()
    ax2.text(width + 0.05, bar.get_y() + bar.get_height()/2, f'{width:.2f}M', ha='left', va='center', fontsize=9)

plt.tight_layout()
plt.savefig('data/results/experimental_benchmark_results.png', dpi=300)
print("✅ Comparative publication charts generated and saved to data/results/experimental_benchmark_results.png!")
