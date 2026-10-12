# Empirical Performance Evaluation of an Adaptive Batch-Streaming Router

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5.0-orange.svg)](https://spark.apache.org/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-KRaft-black.svg)](https://kafka.apache.org/)
[![Statistical Rigor](https://img.shields.io/badge/p--value-%3C%200.001-success.svg)](scripts/statistical_test.py)

This repository contains the complete experimental implementation, streaming pipelines, load replayer, resource telemetry sampler, and statistical evaluation suite for the MSc Data Science dissertation project: **"An Adaptive Batch-Streaming Routing Architecture for High-Throughput Event Processing."**

---

## 📌 Architecture Overview

Traditional big-data stream processing forces a rigid architectural compromise:
* **Kappa Architecture (Stream-Only):** Directs 100% of volume through low-latency micro-batch engines, creating extreme memory pressure, buffer bloat, and tail-latency spikes during traffic bursts.
* **Lambda Architecture (Fixed Dual-Path):** Duplicates 100% of incoming data across both batch and streaming pipelines, resulting in 100% redundant compute waste.
* **Batch-Only:** Provides high throughput and deep analytics (outlier detection, joins, aggregations) but suffers from high latency (25–30s+), failing time-critical SLAs.

**The Adaptive Router Architecture** dynamically inspects incoming record-level urgency signals (`fare_amount`, `trip_distance`, `fare_per_mile`, and traffic `arrival_rate`) at ingestion time. It routes time-sensitive events (8.8% of volume) to an ultra-low-latency streaming path while offloading routine events (91.2% of volume) to a resource-efficient, thorough batch path.

```
                  ┌─────────────────────────────────────────┐
                  │        3-Phase Stream Replayer          │
                  │   (249,850 NYC Taxi Trip Records)       │
                  │  Steady (1k) → Burst (5k) → Calm (500)  │
                  └────────────────────┬────────────────────┘
                                       │
                                       ▼
                  ┌─────────────────────────────────────────┐
                  │       Apache Kafka (KRaft Mode)         │
                  │        Partition Message Buffer         │
                  └────────────────────┬────────────────────┘
                                       │
                                       ▼
                  ┌─────────────────────────────────────────┐
                  │         Adaptive Router Engine          │
                  │   • O(1) Dynamic Heuristic Router       │
                  │   • Distilled ML Catalyst SQL Router    │
                  └──────────────┬──────────────────┬───────┘
                                 │                  │
                  Urgent (8.8%)  │                  │ Routine (91.2%)
                                 ▼                  ▼
                         ┌──────────────┐   ┌──────────────┐
                         │ Stream Sink  │   │  Batch Sink  │
                         │  (2s Trigger)│   │ (30s Trigger)│
                         └──────┬───────┘   └──────┬───────┘
                                │                  │
                                └─────────┬────────┘
                                          ▼
                         ┌─────────────────────────────────┐
                         │   Parquet Sinks & Aggregates    │
                         │   • Light Projections (Stream)  │
                         │   • Zone Joins + IQR (Batch)    │
                         └─────────────────────────────────┘
                                          ▲
                                          │ (Continuous Monitoring)
                         ┌────────────────┴────────────────┐
                         │   CPU & Memory Resource Sampler │
                         │   Process Tree, JVM & Host RAM  │
                         └─────────────────────────────────┘
```

---

## 🧪 Evaluated Architectures

1. **Stream-Only Baseline (Kappa):** Every record is routed to a 2-second streaming micro-batch trigger (`systems/stream_only.py`).
2. **Batch-Only Baseline:** Every record is held and executed in a 30-second heavy micro-batch trigger (`systems/batch_only.py`).
3. **Fixed-Lambda Baseline:** Every record is processed twice—once through the 2-second stream path and once through the 30-second batch path (`systems/fixed_lambda.py`).
4. **Rule-Based Adaptive Router ($\mathcal{O}(1)$):** Evaluates arrival rate and dynamic threshold rules (`router/rule_router.py`). When arrival rate exceeds 2,500 rec/s, threshold constraints are tightened.
5. **ML-Based Adaptive Router:** A scikit-learn Decision Tree trained on pilot urgency labels with an asymmetric 50:1 penalty on false negatives, compiled directly into native **Spark Catalyst SQL expressions** (`router/ml_router.py`) for zero-IPC-overhead model inference.

### Analytical Pipeline Workloads
* **Lightweight Stream Path (`lib/work.py: light`):** Computes unit economics (`fare_per_mile`), stamps event latencies, and writes directly to Parquet.
* **Thorough Batch Path (`lib/work.py: heavy`):** Performs multi-column data validation, broadcast joins against NYC taxi zone lookups (`LocationID`, `Borough`, `Zone`), statistical outlier detection via IQR fences ($Q_3 + 3.0 \times IQR$), zone aggregations (`trips`, `fare_sum`, `avg_dist`, `avg_fpm`), and persists analytical Parquet sinks.

---

## 📊 Empirical Results (25 Benchmark Runs / 7,494,000 Records)

Evaluated across **5 repeated runs per architecture** under a deterministic 3-phase load profile (**STEADY:** 1,000 rec/s for 90s $\rightarrow$ **BURST:** 5,000 rec/s for 20s $\rightarrow$ **CALM:** 500 rec/s for 120s):

### 1. Urgent Record Latency & SLA Adherence ($\le$ 3.0s Ceiling)

| Architecture | Total Iterations | Urgent Records | Urgent p50 (ms) | Urgent p95 (ms) | Urgent p99 (ms) | SLA Met ($\le$ 3.0s) | Stream Load Share |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **ML Router (Adaptive)** | 1,249,250 | 109,465 | **1,270.0** | **2,171.4** | **2,307.8** | **99.9%** | **8.76%** |
| **Stream-Only (Kappa)** | 1,247,750 | 109,345 | 1,566.1 | 2,493.4 | 4,721.5 | 98.2% | 100.0% *(High memory)* |
| **Rule Router (Adaptive)** | 1,249,250 | 109,465 | 1,460.3 | 22,415.7 | 29,603.0 | 84.8% | 15.06% |
| **Fixed-Lambda** | 2,498,500 | 218,930 | 2,236.1 | 27,660.3 | 30,470.9 | 51.4% | 100.0% *(Double compute)* |
| **Batch-Only** | 1,249,250 | 109,465 | 15,594.1 | 29,197.2 | 30,902.6 | 4.1% *(Fails SLA)* | 0.0% |

### 2. Urgent Latency Breakdown by Load Phase (p95 ms)

| Architecture | Steady Load (1,000 rec/s) | Burst Load (5,000 rec/s) | Calm Load (500 rec/s) |
| :--- | :---: | :---: | :---: |
| **ML Router (Adaptive)** | **2,174.5 ms** | **2,224.1 ms** | **2,075.8 ms** |
| **Stream-Only (Kappa)** | 3,098.5 ms | 2,491.7 ms | 2,367.1 ms |
| **Rule Router (Adaptive)** | 19,935.4 ms | 24,844.1 ms | 17,427.3 ms |
| **Fixed-Lambda** | 28,841.3 ms | 26,398.3 ms | 27,996.2 ms |
| **Batch-Only** | 29,968.8 ms | 26,575.5 ms | 29,398.6 ms |

### 3. Statistical Significance Testing (Mann-Whitney U Test, $\alpha = 0.05$)

| Comparison | Mann-Whitney $U$ | $p$-value | Significant | Effect Size ($r_{rb}$) | Cliff's Delta ($\delta$) |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **ML-Router vs Batch-Only** | $6.89 \times 10^{7}$ | $< 1.0 \times 10^{-300}$ | **YES ($p < 0.001$) \*\*\*** | $0.9885$ | $-0.9898$ (Large) |
| **ML-Router vs Fixed-Lambda** | $6.23 \times 10^{9}$ | $< 1.0 \times 10^{-300}$ | **YES ($p < 0.001$) \*\*\*** | $0.4797$ | $-0.4885$ (Medium-Large) |
| **ML-Router vs Stream-Only** | $4.42 \times 10^{9}$ | $< 1.0 \times 10^{-300}$ | **YES ($p < 0.001$) \*\*\*** | $0.2621$ | $-0.2446$ (Medium) |
| **ML-Router vs Rule-Router** | $5.10 \times 10^{9}$ | $< 1.0 \times 10^{-300}$ | **YES ($p < 0.001$) \*\*\*** | $0.1487$ | $-0.1389$ (Small-Medium) |

*The non-parametric tests confirm that latency improvements from adaptive routing are statistically significant and not artifacts of sample variance.*

---

## ⚡ CPU & Memory Resource Sampler

The framework includes an integrated, thread-based resource sampler ([`lib/resource_sampler.py`](lib/resource_sampler.py)):
* **Process-Tree & JVM Tracking:** Recursively traverses `psutil.Process().children(recursive=True)` to capture the full memory footprint (RSS/VMS) of PySpark child workers and JVM executors.
* **System Telemetry:** Tracks host CPU % and RAM usage concurrently.
* **Automated Outputs:** Emits time-series CSV metrics, terminal summaries, and Matplotlib utilization charts.

```python
from resource_sampler import ResourceSampler

with ResourceSampler(label="ML-Router", output_csv="resources.csv") as sampler:
    # Run streaming pipeline or experiment
    pass

sampler.plot("resources.png")
```

---

## 🚀 Quickstart & Demonstration Guide

### Prerequisites
* Windows 10/11 or Ubuntu 22.04 LTS / WSL2
* Python 3.10+ (`pandas`, `numpy`, `scipy`, `pyarrow`, `psutil`, `matplotlib`)
* Docker & Docker Compose (for live Kafka/Spark cluster runs)

---

### Option A: Accelerated Live System Demo (15–20 Seconds)
Demonstrates the entire lifecycle—data ingestion through Kafka simulation, live CPU/RAM sampling, feature extraction, adaptive routing, and dual-sink latency calculation:
```bash
python scripts/demo_pipeline.py --scale 0.05
```

---

### Option B: Interactive Presentation Web Dashboard
Open the pre-generated interactive presentation dashboard in any browser:
```bash
# Windows
start data/demo_dashboard.html

# Linux / WSL
xdg-open data/demo_dashboard.html
```
*(To regenerate the dashboard, run `python scripts/generate_html_dashboard.py`)*

---

### Option C: Comprehensive Macro-Evaluation Suite
Inspects all 25 historical experimental runs in `data/final_run` and outputs full percentile distributions, SLA compliance, and Mann-Whitney U test results:
```bash
python scripts/comprehensive_evaluation.py --base data/final_run
```

---

### Option D: Full Distributed Reproduction (WSL2 / Linux Cluster)
1. **Start the Infrastructure Stack:**
   ```bash
   docker compose -f compose/docker-compose.yml up -d
   ```

2. **Execute Full Automated Benchmark Suite (5 Systems x 5 Repeats):**
   ```bash
   python3 scripts/run_experiments.py --base data/results --repeats 5
   ```

3. **Validate Results & Generate Figures:**
   ```bash
   python3 scripts/validate_routing.py data/results
   python3 scripts/statistical_test.py data/results
   python3 scripts/generate_thesis_charts.py
   ```

---

## 📁 Repository Structure

```
├── compose/
│   └── docker-compose.yml       # Kafka (KRaft mode), Spark Master, cAdvisor
├── data/
│   ├── final_run/               # 25 benchmark runs (7.5M records)
│   ├── sampled_taxi_data.parquet # Ground-truth NYC taxi dataset (249,850 records)
│   ├── taxi_zone_lookup.csv     # NYC Taxi Zone lookup table
│   ├── ml_sql_expr.txt          # Distilled Decision Tree Catalyst SQL expression
│   └── demo_dashboard.html      # Interactive presentation web dashboard
├── lib/
│   ├── work.py                  # Shared PySpark Structured Streaming fast/heavy pipelines
│   └── resource_sampler.py      # Background CPU/Memory process-tree sampler
├── replayer/
│   └── replayer.py              # 3-phase Kafka load replayer (Steady, Burst, Calm)
├── router/
│   ├── params.py                # Rule router O(1) threshold formulas
│   ├── rule_router.py           # Rule-based adaptive streaming application
│   └── ml_router.py             # ML Catalyst SQL adaptive streaming application
├── scripts/
│   ├── demo_pipeline.py         # Accelerated live demonstration runner (15-20s)
│   ├── comprehensive_evaluation.py # 25-run evaluation and statistical engine
│   ├── generate_html_dashboard.py  # Generates the presentation dashboard HTML
│   ├── statistical_test.py      # Standalone Mann-Whitney U testing script
│   ├── validate_routing.py      # Ground-truth routing and oracle validation
│   ├── generate_thesis_charts.py# Publication-quality charts (Seaborn/Matplotlib)
│   ├── train_ml_model.py        # Decision Tree training and SQL distillation
│   └── run_experiments.py       # Distributed automated 25-run benchmark coordinator
└── systems/
    ├── stream_only.py           # Kappa baseline pipeline (2s trigger)
    ├── batch_only.py            # Batch baseline pipeline (30s trigger)
    └── fixed_lambda.py          # Fixed Lambda dual-path baseline
```

---

## 📜 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
