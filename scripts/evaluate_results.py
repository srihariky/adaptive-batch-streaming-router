import glob, json, os
import numpy as np, pandas as pd
from scipy.stats import mannwhitneyu

SINK_SYSTEM = {
    "stream_only_out": "Stream-Only", "batch_only_out": "Batch-Only",
    "lambda_stream_out": "Lambda (stream)", "lambda_batch_out": "Lambda (batch)",
    "router_stream_out": "Rule (stream)", "router_batch_out": "Rule (batch)",
    "ml_router_stream_out": "ML (stream)", "ml_router_batch_out": "ML (batch)",
}
rows = []
for run_dir in sorted(glob.glob("data/results/*_r[0-9]*")):
    if not os.path.exists(f"{run_dir}/DONE"):
        continue
    for sink, label in SINK_SYSTEM.items():
        files = glob.glob(f"{run_dir}/{sink}/*.parquet")
        if not files:
            continue
        df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
        tcol = next((c for c in ("spark_process_time", "router_decision_time") if c in df.columns), None)
        if not tcol:
            continue
        df["lat"] = (df[tcol] - df["event_creation_time"]) * 1000
        t = df["event_creation_time"] - df["event_creation_time"].min()
        df["phase"] = np.where(t < 90, "steady", np.where(t < 110, "burst", "calm"))
        dup = len(df) - df["record_id"].nunique()
        for scope, d in [("all", df), ("urgent", df[df.priority == 1])] + \
                        [(f"urgent_{p}", df[(df.priority == 1) & (df.phase == p)]) for p in ("steady", "burst", "calm")]:
            if len(d):
                rows.append(dict(run=os.path.basename(run_dir), sink=label, scope=scope, n=len(d), dup=dup,
                                 p50=d.lat.quantile(.5), p95=d.lat.quantile(.95), p99=d.lat.quantile(.99)))
res = pd.DataFrame(rows)
res.to_csv("data/results/run_level_metrics.csv", index=False)

bad = res[(res.scope == "all") & ((res.p50 > 120_000) | (res.dup > 0))]
if len(bad):
    print("WARNING: contaminated or duplicated runs:\n", bad[["run", "sink", "p50", "dup"]])

print(res.groupby(["scope", "sink"])[["n", "p50", "p95", "p99"]].agg(["mean", "std"]).round(1).to_string())

def cliffs(a, b):
    return (np.sum(np.array(a)[:, None] > np.array(b)) - np.sum(np.array(a)[:, None] < np.array(b))) / (len(a) * len(b))

try:
    base = res[(res.scope == "urgent") & (res.sink == "Rule (stream)")].p95
    for sink in res[res.scope == "urgent"].sink.unique():
        other = res[(res.scope == "urgent") & (res.sink == sink)].p95
        if sink != "Rule (stream)" and len(other) > 1 and len(base) > 1:
            print(f"urgent p95 | Rule(stream) vs {sink}: p={mannwhitneyu(base, other).pvalue:.4f}, Cliff's d={cliffs(base, other):.2f}")
except Exception as e:
    pass
