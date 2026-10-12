import glob, sys, pandas as pd
rd = sys.argv[1] if len(sys.argv) > 1 else "data/demo/ml_router_r1"
def load(s):
    f = glob.glob(f"{rd}/{s}/*.parquet")
    return pd.concat([pd.read_parquet(x) for x in f], ignore_index=True) if f else pd.DataFrame()
fast, slow = load("ml_router_stream_out"), load("ml_router_batch_out")
for name, d in [("FAST path (2 s)", fast), ("THOROUGH path (30 s)", slow)]:
    lat = d.spark_process_time - d.event_creation_time
    print(f"{name:22s} rows={len(d):7,d}  p50={lat.median():5.1f}s  p95={lat.quantile(.95):5.1f}s")
al = pd.concat([fast, slow]); urg = al[al.priority == 1]
print(f"\nrouted to fast path : {len(fast)/len(al):.1%} of {len(al):,} records")
print(f"urgent caught       : {(urg.route == 1).mean():.1%} ({int((urg.route == 1).sum()):,} of {len(urg):,})")
print(f"urgent p95 latency  : {(urg.spark_process_time - urg.event_creation_time).quantile(.95):.1f}s (all urgent records)")
