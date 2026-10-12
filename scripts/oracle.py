import glob, os
import numpy as np, pandas as pd

URGENT_TARGET_MS, ROUTINE_TARGET_MS, STREAM_COST = 5_000, 60_000, 0.1
PHASES = ["steady", "burst", "calm"]

def read_run(run_dir, sinks):
    parts = []
    for s in sinks:
        files = glob.glob(f"{run_dir}/{s}/*.parquet")
        if files:
            d = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
            d["sink"] = s
            parts.append(d)
    if not parts:
        return pd.DataFrame()
    df = pd.concat(parts, ignore_index=True)
    tcol = "spark_process_time" if "spark_process_time" in df.columns else "router_decision_time"
    df["lat"] = (df[tcol] - df["event_creation_time"]) * 1000
    t = df["event_creation_time"] - df["event_creation_time"].min()
    df["phase"] = np.select([t < 90, t < 110], ["steady", "burst"], "calm")
    return df

def read_runs(base, system, sinks):
    out = []
    for rd in sorted(glob.glob(f"{base}/{system}_r[0-9]*")):
        d = read_run(rd, sinks)
        if len(d):
            d["run"] = os.path.basename(rd)
            out.append(d)
    return pd.concat(out, ignore_index=True) if out else pd.DataFrame()

def build_table(base):
    table = {}
    for path, system, sink in [("stream", "stream_only", "stream_only_out"), ("batch", "batch_only", "batch_only_out")]:
        d = read_runs(base, system, [sink])
        if d.empty:
            raise SystemExit(f"No pilot data for {system} in {base}")
        table[path] = d.groupby("phase").lat.quantile(0.95).round(1).to_dict()
        assert set(table[path]) == set(PHASES), f"missing phases for {path}: {table[path]}"
    return table

def oracle_route(urgent, phase, table):
    """1 = fast path is best, 0 = batch path. Utility = target met - capacity cost of fast path."""
    urgent = np.asarray(urgent)
    tgt = np.where(urgent == 1, URGENT_TARGET_MS, ROUTINE_TARGET_MS)
    ps = phase.map(table["stream"]).values.astype(float)
    pb = phase.map(table["batch"]).values.astype(float)
    ms, mb = ps <= tgt, pb <= tgt
    us, ub = ms.astype(float) - STREAM_COST, mb.astype(float)
    route = np.where(us > ub, 1, np.where(us < ub, 0, (ps < pb).astype(int)))
    return np.where(~ms & ~mb, (ps < pb).astype(int), route)
