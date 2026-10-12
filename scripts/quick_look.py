import glob, os, sys
import numpy as np, pandas as pd

base = sys.argv[1]
rows = []
for rd in sorted(glob.glob(f"{base}/*_r[0-9]*")):
    for sink in sorted(glob.glob(f"{rd}/*_out")):
        files = glob.glob(f"{sink}/*.parquet")
        if not files:
            continue
        df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
        df["lat_s"] = df["spark_process_time"] - df["event_creation_time"]
        t = df["event_creation_time"] - df["event_creation_time"].min()
        df["phase"] = np.select([t < 90, t < 110], ["steady", "burst"], "calm")
        for ph, d in [("all", df)] + list(df.groupby("phase")):
            rows.append(dict(run=os.path.basename(rd), sink=os.path.basename(sink), phase=ph, n=len(d),
                             p50=d.lat_s.quantile(.5), p95=d.lat_s.quantile(.95), max=d.lat_s.max()))
r = pd.DataFrame(rows)
print("p95 latency (seconds) by phase:")
print(r.pivot_table(index=["run", "sink"], columns="phase", values="p95").round(1).to_string())
print("\nrows per sink:"); print(r[r.phase == "all"][["run", "sink", "n"]].to_string(index=False))
