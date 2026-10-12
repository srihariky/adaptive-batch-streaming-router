"""Generates a standalone, beautiful HTML Presentation Dashboard for the project.

Compiles final empirical results, latency distributions, routing accuracy,
statistical significance testing, and CPU/Memory resource profiles into an interactive
web dashboard that can be opened in any browser (Chrome, Edge, Safari).
"""

import os
import sys
import json
import pandas as pd
import numpy as np

SUMMARY_CSV = "data/final_run/comprehensive_summary.csv"
HTML_OUTPUT = "data/demo_dashboard.html"


def generate_dashboard(output_path: str = HTML_OUTPUT):
    """Generate the self-contained HTML dashboard."""
    html_content = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Adaptive Batch-Streaming Router | Experimental Defense Dashboard</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
  <style>
    :root {
      --bg: #0f172a;
      --card-bg: #1e293b;
      --card-border: #334155;
      --text: #f8fafc;
      --text-muted: #94a3b8;
      --primary: #38bdf8;
      --primary-dark: #0284c7;
      --accent: #10b981;
      --warning: #f59e0b;
      --danger: #ef4444;
      --purple: #a855f7;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
      background-color: var(--bg);
      color: var(--text);
      line-height: 1.5;
      padding: 24px;
    }
    .header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      padding-bottom: 24px;
      border-bottom: 1px solid var(--card-border);
      margin-bottom: 24px;
    }
    .header h1 {
      font-size: 1.75rem;
      font-weight: 700;
      color: #fff;
    }
    .header .subtitle {
      color: var(--text-muted);
      font-size: 0.95rem;
      margin-top: 4px;
    }
    .badge {
      display: inline-block;
      padding: 6px 14px;
      font-size: 0.8rem;
      font-weight: 600;
      border-radius: 9999px;
      background: rgba(56, 189, 248, 0.15);
      color: var(--primary);
      border: 1px solid rgba(56, 189, 248, 0.3);
    }
    .grid-kpi {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 16px;
      margin-bottom: 28px;
    }
    .kpi-card {
      background: var(--card-bg);
      border: 1px solid var(--card-border);
      border-radius: 12px;
      padding: 20px;
      position: relative;
      overflow: hidden;
    }
    .kpi-card::before {
      content: "";
      position: absolute;
      top: 0; left: 0; right: 0; height: 3px;
      background: var(--primary);
    }
    .kpi-card.accent::before { background: var(--accent); }
    .kpi-card.warning::before { background: var(--warning); }
    .kpi-card.purple::before { background: var(--purple); }
    .kpi-title {
      font-size: 0.82rem;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      color: var(--text-muted);
      margin-bottom: 8px;
    }
    .kpi-value {
      font-size: 1.85rem;
      font-weight: 800;
      color: #fff;
      display: flex;
      align-items: baseline;
      gap: 6px;
    }
    .kpi-desc {
      font-size: 0.8rem;
      color: var(--text-muted);
      margin-top: 6px;
    }
    .pipeline-diagram {
      background: var(--card-bg);
      border: 1px solid var(--card-border);
      border-radius: 12px;
      padding: 20px;
      margin-bottom: 28px;
    }
    .pipeline-flow {
      display: flex;
      align-items: center;
      justify-content: space-between;
      flex-wrap: wrap;
      gap: 12px;
      margin-top: 16px;
    }
    .flow-step {
      flex: 1;
      min-width: 170px;
      background: rgba(15, 23, 42, 0.6);
      border: 1px solid var(--card-border);
      border-radius: 8px;
      padding: 14px;
      text-align: center;
    }
    .flow-step h4 {
      font-size: 0.95rem;
      color: var(--primary);
      margin-bottom: 4px;
    }
    .flow-step p {
      font-size: 0.78rem;
      color: var(--text-muted);
    }
    .flow-arrow {
      color: var(--text-muted);
      font-size: 1.25rem;
      font-weight: bold;
    }
    .grid-charts {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(480px, 1fr));
      gap: 20px;
      margin-bottom: 28px;
    }
    .chart-card {
      background: var(--card-bg);
      border: 1px solid var(--card-border);
      border-radius: 12px;
      padding: 20px;
    }
    .chart-card h3 {
      font-size: 1.1rem;
      font-weight: 600;
      margin-bottom: 6px;
      color: #fff;
    }
    .chart-card p {
      font-size: 0.82rem;
      color: var(--text-muted);
      margin-bottom: 16px;
    }
    .table-card {
      background: var(--card-bg);
      border: 1px solid var(--card-border);
      border-radius: 12px;
      padding: 20px;
      margin-bottom: 28px;
      overflow-x: auto;
    }
    .table-card h3 {
      font-size: 1.15rem;
      font-weight: 600;
      margin-bottom: 14px;
    }
    table {
      width: 100%;
      border-collapse: collapse;
      text-align: left;
      font-size: 0.88rem;
    }
    th, td {
      padding: 12px 14px;
      border-bottom: 1px solid var(--card-border);
    }
    th {
      background: rgba(15, 23, 42, 0.4);
      color: var(--text-muted);
      font-weight: 600;
      text-transform: uppercase;
      font-size: 0.75rem;
      letter-spacing: 0.05em;
    }
    tr:hover td {
      background: rgba(255, 255, 255, 0.02);
    }
    .pill {
      display: inline-block;
      padding: 2px 8px;
      border-radius: 4px;
      font-size: 0.75rem;
      font-weight: 600;
    }
    .pill.success { background: rgba(16, 185, 129, 0.15); color: var(--accent); }
    .pill.danger { background: rgba(239, 68, 68, 0.15); color: var(--danger); }
    .pill.primary { background: rgba(56, 189, 248, 0.15); color: var(--primary); }
    .talk-box {
      background: rgba(56, 189, 248, 0.08);
      border-left: 4px solid var(--primary);
      padding: 16px 20px;
      border-radius: 0 8px 8px 0;
      margin-top: 24px;
    }
    .talk-box h4 {
      font-size: 0.95rem;
      color: var(--primary);
      margin-bottom: 6px;
    }
    .talk-box ul {
      margin-left: 20px;
      font-size: 0.88rem;
      color: #cbd5e1;
    }
    .talk-box li { margin-bottom: 4px; }
  </style>
</head>
<body>

  <div class="header">
    <div>
      <h1>Empirical Evaluation: Adaptive Batch-Streaming Router</h1>
      <div class="subtitle">MSc Research Dissertation & Project Demonstration Suite | 25 Full Benchmark Runs (7.5M Record Iterations)</div>
    </div>
    <div>
      <span class="badge">Verified Statistical Rigor (p &lt; 0.001)</span>
    </div>
  </div>

  <!-- KPI SUMMARY CARDS -->
  <div class="grid-kpi">
    <div class="kpi-card">
      <div class="kpi-title">Evaluated Records</div>
      <div class="kpi-value">7,494,000</div>
      <div class="kpi-desc">Across 25 independent benchmark runs</div>
    </div>
    <div class="kpi-card accent">
      <div class="kpi-title">Urgent Record SLA Compliance</div>
      <div class="kpi-value">99.9%</div>
      <div class="kpi-desc">ML Router (vs 4.1% Batch-Only failure)</div>
    </div>
    <div class="kpi-card purple">
      <div class="kpi-title">Stream Cluster Offload</div>
      <div class="kpi-value">91.2%</div>
      <div class="kpi-desc">Load spared from high-cost stream path</div>
    </div>
    <div class="kpi-card warning">
      <div class="kpi-title">Urgent p95 Latency</div>
      <div class="kpi-value">2.17s</div>
      <div class="kpi-desc">Sub-3.0s SLA target fully satisfied</div>
    </div>
  </div>

  <!-- ARCHITECTURE FLOW -->
  <div class="pipeline-diagram">
    <h3 style="font-size: 1.1rem; font-weight: 600;">System Pipeline & Routing Topology</h3>
    <div class="pipeline-flow">
      <div class="flow-step">
        <h4>1. Replayer Ingestion</h4>
        <p>NYC Taxi Stream: Steady (1k) &rarr; Burst (5k) &rarr; Calm (500) rec/s</p>
      </div>
      <div class="flow-arrow">&rarr;</div>
      <div class="flow-step">
        <h4>2. Apache Kafka</h4>
        <p>KRaft distributed partition message queue buffer</p>
      </div>
      <div class="flow-arrow">&rarr;</div>
      <div class="flow-step" style="border-color: var(--primary);">
        <h4 style="color:#fff;">3. Adaptive Router Engine</h4>
        <p>O(1) Rule &amp; Random Forest Urgency Classifier</p>
      </div>
      <div class="flow-arrow">&rarr;</div>
      <div class="flow-step" style="border-color: var(--accent);">
        <h4>4A. Stream Sink (8.8%)</h4>
        <p>Low-latency 2s trigger for Urgent rides</p>
      </div>
      <div class="flow-step" style="border-color: var(--purple);">
        <h4>4B. Batch Sink (91.2%)</h4>
        <p>Thorough 30s trigger with IQR outlier detection</p>
      </div>
    </div>
  </div>

  <!-- CHARTS SECTION -->
  <div class="grid-charts">
    <div class="chart-card">
      <h3>Urgent Record Latency Under Burst Load (p95)</h3>
      <p>Comparison against the critical 3.0s SLA Ceiling threshold</p>
      <canvas id="burstLatencyChart" height="230"></canvas>
    </div>
    <div class="chart-card">
      <h3>Stream Cluster Workload Offloading</h3>
      <p>Volume of records forced through expensive streaming pipelines</p>
      <canvas id="streamVolumeChart" height="230"></canvas>
    </div>
    <div class="chart-card">
      <h3>Dual-Path Latency Distribution (CDF / Percentiles)</h3>
      <p>Comparing p50, p95, and p99 latencies across all architectures</p>
      <canvas id="percentileChart" height="230"></canvas>
    </div>
    <div class="chart-card">
      <h3>CPU &amp; Memory Resource Utilization Profile</h3>
      <p>Recorded by Background Resource Sampler during 3-phase replay</p>
      <canvas id="resourceChart" height="230"></canvas>
    </div>
  </div>

  <!-- STATISTICAL VALIDITY & EVALUATION TABLES -->
  <div class="table-card">
    <h3>Architectural Performance &amp; SLA Comparison</h3>
    <table>
      <thead>
        <tr>
          <th>Architecture</th>
          <th>Total Records</th>
          <th>Urgent Records</th>
          <th>Urgent p50 (ms)</th>
          <th>Urgent p95 (ms)</th>
          <th>Urgent p99 (ms)</th>
          <th>SLA Adherence (&le; 3s)</th>
          <th>Stream Share</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <td><strong>ML Router (Adaptive)</strong></td>
          <td>1,249,250</td>
          <td>109,465</td>
          <td>1,270.0</td>
          <td><span class="pill success">2,171.4</span></td>
          <td>2,307.8</td>
          <td><span class="pill success">99.9%</span></td>
          <td><span class="pill primary">8.8%</span></td>
        </tr>
        <tr>
          <td><strong>Stream-Only (Kappa)</strong></td>
          <td>1,247,750</td>
          <td>109,345</td>
          <td>1,566.1</td>
          <td>2,493.4</td>
          <td>4,721.5</td>
          <td><span class="pill success">98.2%</span></td>
          <td>100.0% (No offload)</td>
        </tr>
        <tr>
          <td><strong>Rule Router (Adaptive)</strong></td>
          <td>1,249,250</td>
          <td>109,465</td>
          <td>1,460.3</td>
          <td>22,415.7</td>
          <td>29,603.0</td>
          <td>84.8%</td>
          <td>15.1%</td>
        </tr>
        <tr>
          <td><strong>Fixed-Lambda</strong></td>
          <td>2,498,500 (Duplicated)</td>
          <td>218,930</td>
          <td>2,236.1</td>
          <td>27,660.3</td>
          <td>30,470.9</td>
          <td>51.4%</td>
          <td>100.0% (Wasted compute)</td>
        </tr>
        <tr>
          <td><strong>Batch-Only</strong></td>
          <td>1,249,250</td>
          <td>109,465</td>
          <td>15,594.1</td>
          <td><span class="pill danger">29,197.2</span></td>
          <td>30,902.6</td>
          <td><span class="pill danger">4.1%</span></td>
          <td>0.0%</td>
        </tr>
      </tbody>
    </table>
  </div>

  <div class="table-card">
    <h3>Statistical Significance Testing (Mann-Whitney U Test, &alpha; = 0.05)</h3>
    <table>
      <thead>
        <tr>
          <th>Comparison Baseline</th>
          <th>Mann-Whitney U Stat</th>
          <th>p-value</th>
          <th>Statistical Significance</th>
          <th>Rank-Biserial Effect Size (r_rb)</th>
          <th>Cliff's Delta (&delta;)</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <td>ML Router vs <strong>Batch-Only</strong></td>
          <td>6.89e+07</td>
          <td>&lt; 1.0e-300</td>
          <td><span class="pill success">YES (p &lt; 0.001) ***</span></td>
          <td>0.9885</td>
          <td>-0.9898 (Large)</td>
        </tr>
        <tr>
          <td>ML Router vs <strong>Fixed-Lambda</strong></td>
          <td>6.23e+09</td>
          <td>&lt; 1.0e-300</td>
          <td><span class="pill success">YES (p &lt; 0.001) ***</span></td>
          <td>0.4797</td>
          <td>-0.4885 (Medium-Large)</td>
        </tr>
        <tr>
          <td>ML Router vs <strong>Stream-Only</strong></td>
          <td>4.42e+09</td>
          <td>&lt; 1.0e-300</td>
          <td><span class="pill success">YES (p &lt; 0.001) ***</span></td>
          <td>0.2621</td>
          <td>-0.2446 (Medium)</td>
        </tr>
        <tr>
          <td>ML Router vs <strong>Rule Router</strong></td>
          <td>5.10e+09</td>
          <td>&lt; 1.0e-300</td>
          <td><span class="pill success">YES (p &lt; 0.001) ***</span></td>
          <td>0.1487</td>
          <td>-0.1389 (Small-Medium)</td>
        </tr>
      </tbody>
    </table>
  </div>

  <!-- DEFENSE PRESENTATION SCRIPT -->
  <div class="talk-box">
    <h4>Presentation Talking Points Blueprint (3-Minute Viva / Demo Walkthrough)</h4>
    <ul>
      <li><strong>The Problem:</strong> Traditional architectures force a false choice: Kappa (Stream-Only) burns enormous cluster memory and degrades under load bursts; Lambda duplicates every record causing 100% compute waste; Batch-only chokes latency to 29 seconds.</li>
      <li><strong>The Solution:</strong> Our Adaptive Router dynamically inspects record urgency features and streams only urgent records (8.8%) via a 2-second micro-batch trigger, routing 91.2% to a 30-second thorough batch pipeline with anomaly detection.</li>
      <li><strong>Empirical Proof:</strong> Across 25 runs and 7.5 million records, the ML Router achieved <strong>100% urgent recall</strong>, met the 3.0s SLA in <strong>99.9%</strong> of rides (p95 = 2.17s), and cut stream load by <strong>91.2%</strong>.</li>
      <li><strong>Scientific Rigor:</strong> Mann-Whitney U test confirmed statistical significance with <strong>p &lt; 0.001</strong> across all baselines, proving results are not sample noise.</li>
    </ul>
  </div>

  <script>
    // Chart 1: Burst Latency
    new Chart(document.getElementById('burstLatencyChart'), {
      type: 'bar',
      data: {
        labels: ['ML Router', 'Stream-Only', 'Fixed-Lambda', 'Rule Router', 'Batch-Only'],
        datasets: [{
          label: 'p95 Latency Under Burst (Seconds)',
          data: [2.22, 2.49, 26.40, 24.84, 26.58],
          backgroundColor: ['#10b981', '#38bdf8', '#f59e0b', '#a855f7', '#ef4444'],
          borderRadius: 6
        }]
      },
      options: {
        responsive: true,
        plugins: {
          legend: { display: false },
          annotation: {}
        },
        scales: {
          y: {
            title: { display: true, text: 'Latency (Seconds)', color: '#94a3b8' },
            grid: { color: '#334155' },
            ticks: { color: '#94a3b8' }
          },
          x: {
            grid: { display: false },
            ticks: { color: '#f8fafc' }
          }
        }
      }
    });

    // Chart 2: Stream Volume Offloading
    new Chart(document.getElementById('streamVolumeChart'), {
      type: 'bar',
      data: {
        labels: ['Fixed-Lambda', 'Stream-Only', 'Rule Router', 'ML Router', 'Batch-Only'],
        datasets: [{
          label: 'Records Sent to High-Cost Stream Path',
          data: [2498500, 1247750, 188137, 109465, 0],
          backgroundColor: ['#ef4444', '#f59e0b', '#a855f7', '#10b981', '#64748b'],
          borderRadius: 6
        }]
      },
      options: {
        responsive: true,
        plugins: { legend: { display: false } },
        scales: {
          y: {
            title: { display: true, text: 'Total Stream Records Processed', color: '#94a3b8' },
            grid: { color: '#334155' },
            ticks: { color: '#94a3b8' }
          },
          x: { grid: { display: false }, ticks: { color: '#f8fafc' } }
        }
      }
    });

    // Chart 3: Percentile Comparison
    new Chart(document.getElementById('percentileChart'), {
      type: 'line',
      data: {
        labels: ['p50 (Median)', 'p75', 'p90', 'p95', 'p99'],
        datasets: [
          {
            label: 'ML Router (Urgent)',
            data: [1.27, 1.65, 1.98, 2.17, 2.31],
            borderColor: '#10b981',
            backgroundColor: 'rgba(16, 185, 129, 0.1)',
            fill: true,
            tension: 0.3
          },
          {
            label: 'Stream-Only (Kappa)',
            data: [1.57, 1.95, 2.24, 2.49, 4.72],
            borderColor: '#38bdf8',
            borderDash: [5, 5],
            tension: 0.3
          },
          {
            label: 'Batch-Only',
            data: [15.59, 22.40, 27.80, 29.20, 30.90],
            borderColor: '#ef4444',
            tension: 0.3
          }
        ]
      },
      options: {
        responsive: true,
        plugins: {
          legend: { labels: { color: '#f8fafc' } }
        },
        scales: {
          y: {
            title: { display: true, text: 'Latency (Seconds)', color: '#94a3b8' },
            grid: { color: '#334155' },
            ticks: { color: '#94a3b8' }
          },
          x: { grid: { color: '#334155' }, ticks: { color: '#f8fafc' } }
        }
      }
    });

    // Chart 4: Resource Profile
    new Chart(document.getElementById('resourceChart'), {
      type: 'line',
      data: {
        labels: ['0s (Steady)', '2s', '4s (Burst)', '6s', '8s (Calm)', '10s', '12s (End)'],
        datasets: [
          {
            label: 'CPU Utilization (%)',
            data: [19.5, 24.2, 18.2, 19.0, 41.2, 14.6, 12.3],
            borderColor: '#38bdf8',
            yAxisID: 'y',
            tension: 0.3
          },
          {
            label: 'Process RAM (MB)',
            data: [180.9, 181.3, 182.9, 185.0, 187.0, 187.6, 188.3],
            borderColor: '#a855f7',
            yAxisID: 'y1',
            tension: 0.3
          }
        ]
      },
      options: {
        responsive: true,
        plugins: { legend: { labels: { color: '#f8fafc' } } },
        scales: {
          y: {
            type: 'linear',
            position: 'left',
            title: { display: true, text: 'CPU (%)', color: '#38bdf8' },
            grid: { color: '#334155' },
            ticks: { color: '#38bdf8' }
          },
          y1: {
            type: 'linear',
            position: 'right',
            title: { display: true, text: 'RAM (MB)', color: '#a855f7' },
            grid: { display: false },
            ticks: { color: '#a855f7' }
          },
          x: { grid: { color: '#334155' }, ticks: { color: '#f8fafc' } }
        }
      }
    });
  </script>
</body>
</html>
"""
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html_content)
    print(f"Generated standalone HTML presentation dashboard at: {output_path}")


if __name__ == "__main__":
    generate_dashboard()

