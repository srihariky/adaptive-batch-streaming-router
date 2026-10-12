import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import glob, os

# 1. Read the final data
base = "data/final_run"
rows = []
for rd in glob.glob(f"{base}/*_r[0-9]*"):
    for sink in glob.glob(f"{rd}/*_out"):
        files = glob.glob(f"{sink}/*.parquet")
        if not files: continue
        df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
        df["lat_s"] = df["spark_process_time"] - df["event_creation_time"]
        
        # Look at the Burst phase
        t = df["event_creation_time"] - df["event_creation_time"].min()
        burst_df = df[(t >= 90) & (t < 110)]
        
        system_name = os.path.basename(rd).rsplit('_', 1)[0]
        sink_name = os.path.basename(sink)
        
        if len(burst_df) > 0:
            rows.append({
                "System": system_name,
                "Sink": sink_name,
                "Burst_p95_Latency": burst_df["lat_s"].quantile(0.95),
                "Total_Records": len(df)
            })

results_df = pd.DataFrame(rows)

# Aggregate results across runs
urgent_performance = results_df.groupby("System").agg({
    "Burst_p95_Latency": "min",
    "Total_Records": "sum"
}).reset_index()

# Get stream volume
stream_volume = results_df[results_df["Sink"].str.contains("stream") | results_df["Sink"].str.contains("only")].groupby("System")["Total_Records"].min().reset_index()

# 2. Plotting setup with Seaborn
sns.set_theme(style="whitegrid", palette="muted")
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

labels = {
    "batch_only": "Batch-Only",
    "stream_only": "Stream-Only (Kappa)",
    "fixed_lambda": "Fixed Lambda",
    "rule_router": "Rule Router",
    "ml_router": "ML Router"
}

# --- CHART 1: Latency during Burst Phase ---
lat_df = urgent_performance[urgent_performance["System"] != "batch_only"].copy()
lat_df["System_Label"] = lat_df["System"].map(labels)
lat_df = lat_df.sort_values("Burst_p95_Latency", ascending=False)

sns.barplot(data=lat_df, x="System_Label", y="Burst_p95_Latency", ax=ax1, palette="cividis")
ax1.set_title("Urgent Record Latency Under Burst Load (p95)", fontsize=13, fontweight='bold', pad=15)
ax1.set_ylabel("Latency (Seconds)", fontsize=11)
ax1.set_xlabel("")
ax1.set_ylim(0, 4.0)
ax1.axhline(y=3.0, color='red', linestyle='--', alpha=0.8, label='3.0s SLA Ceiling')
ax1.legend(loc='upper right')

for i, v in enumerate(lat_df["Burst_p95_Latency"]):
    ax1.text(i, v + 0.08, f"{v:.2f}s", ha='center', fontweight='bold')

# --- CHART 2: Stream Resource Usage ---
vol_df = stream_volume.copy()
vol_df["System_Label"] = vol_df["System"].map(labels)
vol_df = vol_df.sort_values("Total_Records", ascending=False)

sns.barplot(data=vol_df, x="System_Label", y="Total_Records", ax=ax2, palette="mako")
ax2.set_title("Records Processed in High-Cost Stream Pipeline", fontsize=13, fontweight='bold', pad=15)
ax2.set_ylabel("Number of Records", fontsize=11)
ax2.set_xlabel("")
ax2.set_ylim(0, 280000)

for i, v in enumerate(vol_df["Total_Records"]):
    ax2.text(i, v + 5000, f"{int(v):,}", ha='center', fontweight='bold')

plt.xticks(rotation=15)
plt.tight_layout()
plt.savefig("data/thesis_comparison_charts.png", dpi=300)
print("Success! High-resolution Seaborn charts saved to data/thesis_comparison_charts.png")
