"""CPU and Memory Resource Sampler for Adaptive Batch-Streaming Router.

Monitors real-time CPU utilization (%) and Memory footprint (MB / %) at both
system-wide and process-tree levels (capturing Python host and Spark JVM child workers).
Supports context manager usage, thread-based background logging, CSV persistence,
summary statistics, and chart visualization.
"""

import os
import sys
import time
import threading
import argparse
from typing import Optional, Dict, Any, List
import psutil
import pandas as pd

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass


class ResourceSampler:
    """Lightweight background resource sampler measuring CPU and Memory utilization."""

    def __init__(
        self,
        target_pid: Optional[int] = None,
        interval: float = 0.5,
        output_csv: Optional[str] = None,
        label: str = "Pipeline",
        verbose: bool = False,
    ):
        """
        Initialize the resource sampler.

        :param target_pid: PID to monitor along with its recursive children. If None, monitors current process.
        :param interval: Sampling interval in seconds (default: 0.5s).
        :param output_csv: Optional path to save sampled metrics as a CSV file.
        :param label: Identifier name for the workload being sampled.
        :param verbose: Whether to print periodic live telemetry to stdout.
        """
        self.target_pid = target_pid if target_pid is not None else os.getpid()
        self.interval = max(0.1, float(interval))
        self.output_csv = output_csv
        self.label = label
        self.verbose = verbose

        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._samples: List[Dict[str, Any]] = []
        self._start_time: float = 0.0
        self._end_time: float = 0.0

        try:
            self._process = psutil.Process(self.target_pid)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            self._process = None

    def _collect_process_metrics(self) -> Dict[str, float]:
        """Aggregate CPU % and Memory RSS (MB) across target process and all child processes."""
        if not self._process:
            return {"proc_cpu_percent": 0.0, "proc_mem_rss_mb": 0.0, "proc_mem_vms_mb": 0.0}

        try:
            if not self._process.is_running():
                return {"proc_cpu_percent": 0.0, "proc_mem_rss_mb": 0.0, "proc_mem_vms_mb": 0.0}

            # Collect target process and all children (e.g. PySpark JVM worker processes)
            procs = [self._process]
            try:
                children = self._process.children(recursive=True)
                procs.extend(children)
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass

            total_cpu = 0.0
            total_rss = 0.0
            total_vms = 0.0

            for p in procs:
                try:
                    total_cpu += p.cpu_percent(interval=None)
                    m = p.memory_info()
                    total_rss += m.rss / (1024 * 1024)
                    total_vms += m.vms / (1024 * 1024)
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue

            return {
                "proc_cpu_percent": round(total_cpu, 2),
                "proc_mem_rss_mb": round(total_rss, 2),
                "proc_mem_vms_mb": round(total_vms, 2),
            }
        except Exception:
            return {"proc_cpu_percent": 0.0, "proc_mem_rss_mb": 0.0, "proc_mem_vms_mb": 0.0}

    def _sample_loop(self):
        """Background sampling loop running until stop event is signaled."""
        # Prime cpu_percent measurements
        psutil.cpu_percent(interval=None)
        if self._process:
            try:
                self._process.cpu_percent(interval=None)
            except Exception:
                pass

        step = 0
        while not self._stop_event.is_set():
            t_now = time.time()
            elapsed = round(t_now - self._start_time, 2)

            # System-wide metrics
            sys_cpu = psutil.cpu_percent(interval=None)
            sys_mem = psutil.virtual_memory()
            sys_mem_used_mb = round((sys_mem.total - sys_mem.available) / (1024 * 1024), 2)
            sys_mem_total_mb = round(sys_mem.total / (1024 * 1024), 2)
            sys_mem_pct = sys_mem.percent

            # Process tree metrics
            proc_metrics = self._collect_process_metrics()

            sample = {
                "timestamp": round(t_now, 3),
                "elapsed_sec": elapsed,
                "label": self.label,
                "system_cpu_pct": sys_cpu,
                "system_mem_used_mb": sys_mem_used_mb,
                "system_mem_total_mb": sys_mem_total_mb,
                "system_mem_pct": sys_mem_pct,
                "proc_cpu_pct": proc_metrics["proc_cpu_percent"],
                "proc_mem_rss_mb": proc_metrics["proc_mem_rss_mb"],
                "proc_mem_vms_mb": proc_metrics["proc_mem_vms_mb"],
            }
            self._samples.append(sample)

            if self.verbose and step % 4 == 0:
                print(
                    f"[{self.label}] t={elapsed:5.1f}s | "
                    f"CPU: {sys_cpu:5.1f}% (Proc: {proc_metrics['proc_cpu_percent']:5.1f}%) | "
                    f"RAM: {proc_metrics['proc_mem_rss_mb']:7.1f} MB (Sys: {sys_mem_pct:.1f}%)",
                    flush=True,
                )

            step += 1
            self._stop_event.wait(self.interval)

    def start(self):
        """Start the background sampling thread."""
        if self._thread is not None and self._thread.is_alive():
            return
        self._samples.clear()
        self._stop_event.clear()
        self._start_time = time.time()
        self._thread = threading.Thread(target=self._sample_loop, daemon=True)
        self._thread.start()

    def stop(self) -> Dict[str, Any]:
        """Stop background sampling and return summary statistics."""
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=3.0)
        self._end_time = time.time()

        if self.output_csv and self._samples:
            self.save_csv(self.output_csv)

        return self.get_summary()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    def to_dataframe(self) -> pd.DataFrame:
        """Convert collected samples to a pandas DataFrame."""
        if not self._samples:
            return pd.DataFrame()
        return pd.DataFrame(self._samples)

    def save_csv(self, filepath: str):
        """Save sample time-series to CSV file."""
        df = self.to_dataframe()
        if not df.empty:
            os.makedirs(os.path.dirname(os.path.abspath(filepath)), exist_ok=True)
            df.to_csv(filepath, index=False)

    def get_summary(self) -> Dict[str, Any]:
        """Calculate aggregate statistical metrics across all recorded samples."""
        df = self.to_dataframe()
        duration = round(self._end_time - self._start_time, 2) if self._end_time else 0.0

        if df.empty:
            return {
                "label": self.label,
                "duration_sec": duration,
                "samples_count": 0,
                "avg_system_cpu_pct": 0.0,
                "peak_system_cpu_pct": 0.0,
                "avg_proc_cpu_pct": 0.0,
                "peak_proc_cpu_pct": 0.0,
                "avg_proc_mem_rss_mb": 0.0,
                "peak_proc_mem_rss_mb": 0.0,
                "avg_system_mem_pct": 0.0,
                "peak_system_mem_pct": 0.0,
            }

        return {
            "label": self.label,
            "duration_sec": duration,
            "samples_count": len(df),
            "avg_system_cpu_pct": round(float(df["system_cpu_pct"].mean()), 2),
            "peak_system_cpu_pct": round(float(df["system_cpu_pct"].max()), 2),
            "avg_proc_cpu_pct": round(float(df["proc_cpu_pct"].mean()), 2),
            "peak_proc_cpu_pct": round(float(df["proc_cpu_pct"].max()), 2),
            "avg_proc_mem_rss_mb": round(float(df["proc_mem_rss_mb"].mean()), 2),
            "peak_proc_mem_rss_mb": round(float(df["proc_mem_rss_mb"].max()), 2),
            "avg_system_mem_pct": round(float(df["system_mem_pct"].mean()), 2),
            "peak_system_mem_pct": round(float(df["system_mem_pct"].max()), 2),
        }

    def print_summary(self):
        """Print a formatted presentation table of resource utilization."""
        s = self.get_summary()
        print("\n" + "=" * 62)
        print(f"[RESOURCE UTILIZATION SUMMARY] Workload: [{s['label']}]")
        print("=" * 62)
        print(f"Duration               : {s['duration_sec']}s ({s['samples_count']} samples)")
        print(f"Process Peak RAM (RSS) : {s['peak_proc_mem_rss_mb']:,.1f} MB (Avg: {s['avg_proc_mem_rss_mb']:,.1f} MB)")
        print(f"Process Peak CPU       : {s['peak_proc_cpu_pct']:.1f}% (Avg: {s['avg_proc_cpu_pct']:.1f}%)")
        print(f"System Peak RAM        : {s['peak_system_mem_pct']:.1f}% (Avg: {s['avg_system_mem_pct']:.1f}%)")
        print(f"System Peak CPU        : {s['peak_system_cpu_pct']:.1f}% (Avg: {s['avg_system_cpu_pct']:.1f}%)")
        print("=" * 62 + "\n")

    def plot(self, output_png: str):
        """Generate and save resource utilization time-series plots."""
        df = self.to_dataframe()
        if df.empty or len(df) < 2:
            return

        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 6), sharex=True)
        ax1.plot(df["elapsed_sec"], df["proc_cpu_pct"], label="Process + Workers CPU (%)", color="#1f77b4", lw=2)
        ax1.plot(df["elapsed_sec"], df["system_cpu_pct"], label="Total System CPU (%)", color="#aec7e8", ls="--")
        ax1.set_ylabel("CPU Utilization (%)")
        ax1.set_title(f"Resource Profiling: {self.label}", fontsize=12, fontweight="bold")
        ax1.grid(True, alpha=0.3)
        ax1.legend(loc="upper right")

        ax2.plot(df["elapsed_sec"], df["proc_mem_rss_mb"], label="Process Memory RSS (MB)", color="#2ca02c", lw=2)
        ax2.set_xlabel("Elapsed Time (seconds)")
        ax2.set_ylabel("Memory RSS (MB)")
        ax2.grid(True, alpha=0.3)
        ax2.legend(loc="upper right")

        plt.tight_layout()
        os.makedirs(os.path.dirname(os.path.abspath(output_png)), exist_ok=True)
        fig.savefig(output_png, dpi=200)
        plt.close(fig)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Standalone CPU/Memory Resource Sampler")
    parser.add_argument("--pid", type=int, default=None, help="Target process PID (default: self)")
    parser.add_argument("--duration", type=float, default=5.0, help="Sampling duration in seconds")
    parser.add_argument("--interval", type=float, default=0.5, help="Sampling interval in seconds")
    parser.add_argument("--output", type=str, default="data/demo/resource_sample.csv", help="Output CSV path")
    parser.add_argument("--plot", type=str, default="data/demo/resource_sample.png", help="Output PNG plot path")
    parser.add_argument("--label", type=str, default="Standalone Test", help="Label name")
    parser.add_argument("--verbose", action="store_true", help="Print live updates")
    args = parser.parse_args()

    print(f"Starting Resource Sampler for {args.duration}s (interval: {args.interval}s)...")
    sampler = ResourceSampler(
        target_pid=args.pid,
        interval=args.interval,
        output_csv=args.output,
        label=args.label,
        verbose=args.verbose,
    )
    sampler.start()
    time.sleep(args.duration)
    sampler.stop()
    sampler.print_summary()
    if args.plot:
        sampler.plot(args.plot)
        print(f"Saved resource plot to {args.plot}")
