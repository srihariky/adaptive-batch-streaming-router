import glob, json, os, re, sys
import numpy as np, pandas as pd
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import oracle

BASE = sys.argv[1] if len(sys.argv) > 1 else "data/results"
table = json.load(open("data/oracle_table.json"))
report = json.load(open("data/ml_training_report.json"))
PH = oracle.PHASES

ROUTERS = {"Rule router": ("rule_router", ["router_stream_out", "router_batch_out"], "router_stream_out"),
           "ML router": ("ml_router", ["ml_router_stream_out", "ml_router_batch_out"], "ml_router_stream_out")}
LAT = {"Stream-only": ("stream_only", ["stream_only_out"]), "Batch-only": ("batch_only", ["batch_only_out"]),
       "Lambda (fast result)": ("fixed_lambda", ["lambda_stream_out"]),
       "Rule router": ROUTERS["Rule router"][:2], "ML router": ROUTERS["ML router"][:2]}

def done_runs(system):
    return [r for r in sorted(glob.glob(f"{BASE}/{system}_r[0-9]*")) if os.path.exists(f"{r}/DONE")]

# ---- routing quality (routers only) ----
rr, ref_stream = [], []
for label, (system, sinks, fast) in ROUTERS.items():
    for rd in done_runs(system):
        df = oracle.read_run(rd, sinks)
        if df.empty: continue
        df["route"] = (df["sink"] == fast).astype(int)
        df["oracle"] = oracle.oracle_route(df["priority"].values, df["phase"], table)
        ref_stream.append((df.oracle == 1).mean())
        for ph, d in [("all", df)] + list(df.groupby("phase")):
            urg = d.priority == 1
            fastsel = d.route == 1
            rr.append(dict(system=label, run=os.path.basename(rd), phase=ph, n=len(d),
                           dup=int(df.record_id.duplicated().sum()) if ph == "all" else 0,
                           acc_oracle=(d.route == d.oracle).mean(), urgent_recall=(d.route[urg] == 1).mean(),
                           precision=(d.priority[fastsel] == 1).mean() if fastsel.any() else np.nan,
                           stream_share=d.route.mean()))
rr = pd.DataFrame(rr)
rr.to_csv(f"{BASE}/routing_validation.csv", index=False)

# ---- urgent-record latency, all systems ----
ll = []
for label, (system, sinks) in LAT.items():
    for rd in done_runs(system):
        df = oracle.read_run(rd, sinks)
        if df.empty: continue
        u = df[df.priority == 1]
        for ph, d in [("all", u)] + list(u.groupby("phase")):
            ll.append(dict(system=label, run=os.path.basename(rd), phase=ph, p50=d.lat.quantile(.5), p95=d.lat.quantile(.95)))
ll = pd.DataFrame(ll)
ll.to_csv(f"{BASE}/urgent_latency_validation.csv", index=False)

# ---- data-integrity checks ----
print("=== DATA INTEGRITY ===")
if len(rr):
    print(f"duplicate record_ids across router runs: {int(rr[rr.phase=='all'].dup.sum())}")
    print("rows per router run:", rr[rr.phase == "all"].groupby("system").n.agg(["min", "max"]).to_dict("index"))
tgt = {"STEADY": 1000, "BURST": 5000, "CALM": 500}
ach = {k: [] for k in tgt}
for lg in glob.glob(f"{BASE}/*_r*/replayer.log"):
    for m in re.finditer(r"Phase (\w+) Complete: Sent ([\d,]+) records in ([\d.]+)s \(Actual Rate: (\d+)", open(lg).read()):
        ach[m.group(1)].append(int(m.group(4)))
for k, v in ach.items():
    if v:
        print(f"replayer {k}: target {tgt[k]} rec/s, achieved mean {np.mean(v):.0f} (min {min(v)}, max {max(v)})"
              + ("" if np.mean(v) >= 0.95 * tgt[k] else "   <-- BELOW 95% OF TARGET"))
if len(ll):
    bad = ll[(ll.phase == "all") & (ll.system != "Batch-only") & (ll.p95 > 60_000)]
    print("latency sanity:", "OK" if bad.empty else f"SUSPICIOUS runs:\n{bad}")

# ---- console summary ----
if len(rr):
    print("\n=== ROUTING VS ORACLE (mean over runs) ===")
    print(rr.groupby(["system", "phase"])[["acc_oracle", "urgent_recall", "precision", "stream_share"]].mean().round(3).to_string())
if len(ll):
    print("\n=== URGENT-RECORD LATENCY p95 ms (mean over runs) ===")
    print(ll.pivot_table(index="system", columns="phase", values="p95", aggfunc="mean").round(0).to_string())

# ---- chart ----
def gbar(ax, cats, series):
    n, x = len(series), np.arange(len(cats)); w = 0.8 / n
    for i, (lab, (m, s)) in enumerate(series.items()):
        ax.bar(x + (i - (n - 1) / 2) * w, m, w, yerr=np.nan_to_num(s), capsize=3, label=lab)
    ax.set_xticks(x); ax.set_xticklabels(cats)

fig, ax = plt.subplots(2, 2, figsize=(13, 9))
if len(rr):
    g = rr.groupby(["system", "phase"]).acc_oracle.agg(["mean", "std"])
    gbar(ax[0, 0], PH, {s: ([g.loc[(s, p), "mean"] for p in PH], [g.loc[(s, p), "std"] for p in PH]) for s in ROUTERS if (s, "steady") in g.index})
    rs = float(np.mean(ref_stream))
    ax[0, 0].axhline(rs, ls="--", c="gray", label=f"always-fast ({rs:.2f})")
    ax[0, 0].axhline(1 - rs, ls=":", c="gray", label=f"always-batch ({1-rs:.2f})")
    ax[0, 0].set_ylim(0, 1.05); ax[0, 0].set_title("A. Routing accuracy vs oracle, by load phase"); ax[0, 0].legend(fontsize=8)
    a = rr[rr.phase == "all"].groupby("system")[["urgent_recall", "precision", "stream_share"]].agg(["mean", "std"])
    gbar(ax[0, 1], ["Urgent recall", "Precision", "Fast-path share"],
         {s: ([a.loc[s, (m, "mean")] for m in ("urgent_recall", "precision", "stream_share")],
              [a.loc[s, (m, "std")] for m in ("urgent_recall", "precision", "stream_share")]) for s in a.index})
    ax[0, 1].set_ylim(0, 1.05); ax[0, 1].set_title("B. Urgent recall, precision and fast-path load"); ax[0, 1].legend(fontsize=8)
if len(ll):
    l = ll.groupby(["system", "phase"]).p95.agg(["mean", "std"])
    gbar(ax[1, 0], PH, {s: ([l.loc[(s, p), "mean"] for p in PH], [l.loc[(s, p), "std"] for p in PH]) for s in LAT if (s, "steady") in l.index})
    ax[1, 0].set_yscale("log"); ax[1, 0].set_ylabel("p95 latency (ms, log)")
    ax[1, 0].set_title("C. Urgent-record p95 latency, all systems"); ax[1, 0].legend(fontsize=7)
if len(rr):
    vals = {"ML\n(pilot held-out)": report["val_ml"]["accuracy"], "ML\n(final runs)": rr[(rr.system == "ML router") & (rr.phase == "all")].acc_oracle.mean(),
            "Rule\n(pilot held-out)": report["val_rule"]["accuracy"], "Rule\n(final runs)": rr[(rr.system == "Rule router") & (rr.phase == "all")].acc_oracle.mean(),
            "Majority\nbaseline": report["majority_baseline"]}
    ax[1, 1].bar(list(vals), list(vals.values()), color=["C0", "C0", "C1", "C1", "gray"])
    for i, v in enumerate(vals.values()): ax[1, 1].text(i, v + 0.01, f"{v:.3f}", ha="center", fontsize=9)
    ax[1, 1].set_ylim(0, 1.05); ax[1, 1].set_title("D. Generalisation: pilot validation vs final runs")
fig.suptitle("Validation of adaptive routing (mean over repeats, error bars = std)", fontsize=13)
fig.tight_layout(); fig.savefig(f"{BASE}/validation_chart.png", dpi=200)
print(f"\nSaved {BASE}/validation_chart.png, routing_validation.csv, urgent_latency_validation.csv")
