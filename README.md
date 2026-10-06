# Empirical Performance Evaluation of an Adaptive Batch-Streaming Router

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5.0-orange.svg)](https://spark.apache.org/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-KRaft-black.svg)](https://kafka.apache.org/)

This repository contains the full experimental implementation, streaming pipelines, load replayer, and statistical evaluation scripts for the MSc dissertation research project: **"An Adaptive Batch-Streaming Routing Architecture for High-Throughput Event Processing."**

---

## 📌 Architecture Overview

The system dynamically inspects record-level urgency metadata (`priority`, `fare_amount`, `trip_distance`) and routes records between a low-latency streaming sink (2s trigger) and a thorough batch sink (30s trigger).

                  ┌──────────────────────────────┐
                  │  3-Phase Stream Replayer     │
                  │  (249,850 NYC Taxi Events)   │
                  └──────────────┬───────────────┘
                                 │
                                 ▼
                  ┌──────────────────────────────┐
                  │  Apache Kafka (KRaft Mode)   │
                  └──────────────┬───────────────┘
                                 │
                                 ▼
                  ┌──────────────────────────────┐
                  │    Adaptive Router Engine    │
                  │ (Rule-Based & ML-RandomForest)│
                  └──────┬────────────────┬──────┘
                         │                │
          Urgent (16.2%) │                │ Routine (83.8%)
                         ▼                ▼
                 ┌──────────────┐  ┌──────────────┐
                 │ Stream Sink  │  │  Batch Sink  │
                 │ (2s Trigger) │  │ (30s Trigger)│
                 └──────────────┘  └──────────────┘


---

## 🧪 Evaluated Architectures

1. **Stream-Only Baseline:** Direct 2s micro-batch stream execution.
2. **Batch-Only Baseline:** 30s micro-batch execution.
3. **Fixed-Lambda Baseline:** Dual-path execution (every record sent to both sinks).
4. **Rule-Based Adaptive Router ($\mathcal{O}(1)$):** Deterministic priority/urgency routing.
5. **ML-Based Adaptive Router:** Random Forest classifier trained on empirical latency labels.

---

## 📊 Summary of Experimental Results

Evaluated across **3,360,378 total record iterations** under a 3-phase load profile (**STEADY:** 1,000 rec/s $\rightarrow$ **BURST:** 5,000 rec/s $\rightarrow$ **CALM:** 500 rec/s):

| Architecture | Total Records | p50 Latency (ms) | p95 Latency (ms) | p99 Latency (ms) |
| :--- | :---: | :---: | :---: | :---: |
| **Stream-Only** | 1,249,150 | 222,321.02 | 733,272.87 | 783,331.73 |
| **Batch-Only** | 1,499,000 | 640,628.42 | 12,104,677.67 | 12,164,753.01 |
| **Fixed-Lambda (Stream)** | 1,748,850 | 806,777.73 | 12,355,231.49 | 12,425,322.17 |
| **Fixed-Lambda (Batch)** | 1,744,950 | 807,174.04 | 12,355,431.50 | 12,425,324.69 |
| **Rule-Router (Stream)** | **324,834** | **256,035.52** | **11,005,180.05** | **11,077,096.32** |
| **Rule-Router (Batch)** | **1,673,361** | **290,534.66** | **11,005,197.55** | **11,077,195.75** |
| **ML-Router (Stream)** | 446,945 | 1,313,243.74 | 13,497,258.94 | 13,569,171.42 |
| **ML-Router (Batch)** | 2,301,305 | 1,313,515.14 | 13,497,268.01 | 13,569,270.95 |

*Statistical significance verified via Mann-Whitney U testing ($p < 0.001$, $\alpha = 0.05$).*

---

## 🚀 Quickstart & Reproduction Guide

### Prerequisites
* WSL2 / Ubuntu 22.04 LTS
* Docker & Docker Compose
* Python 3.10+
* PySpark 3.5.0

### 1. Start Infrastructure Stack
```bash
docker compose -f compose/docker-compose.yml up -d

2. Execute Automated Benchmark Suite
Bash

python3 scripts/run_experiments.py

3. Generate ML Dataset & Run Statistical Validation
Bash

python3 scripts/generate_ml_dataset.py
python3 scripts/evaluate_results.py
python3 scripts/statistical_test.py
python3 scripts/plot_results.py

📜 License

This project is licensed under the MIT License - see the LICENSE file for details.
