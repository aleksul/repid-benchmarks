#!/usr/bin/env python3
"""
Benchmark orchestrator — runs all frameworks across multiple sleep times and
collects throughput (and optionally latency) statistics.

Latency variants (repid_latency, celery_latency, etc.) are first-class citizens
here. When a benchmark emits LATENCY_P* lines the orchestrator captures them
alongside throughput and writes them to a separate CSV file.

Usage:
    python run_all.py [--frameworks repid celery ...] [--sleep-times 0.01 0.1 ...]
                      [--runs N] [--messages N] [--resume]
"""

from __future__ import annotations

import argparse
import csv
import os
import re
import subprocess
import sys
import threading
from pathlib import Path
from statistics import mean, stdev

DIR = Path(__file__).parent
BENCHMARKS_DIR = Path(__file__).parent / "benchmarks"

ALL_FRAMEWORKS = [
    "repid",
    "celery",
    "celery_nogt",
    "dramatiq",
    "dramatiq_nogt",
    "faststream",
    "taskiq",
    # High-concurrency variants (CONCURRENCY_LIMIT=10000, 5x base)
    "repid_hc",
    "celery_hc",
    "dramatiq_hc",
    "faststream_hc",
    "taskiq_hc",
    # CPU-bound task variants (thread-offloaded from event loop)
    "repid_cpu",
    "celery_cpu",
    "dramatiq_cpu",
    "faststream_cpu",
    "taskiq_cpu",
    # Streaming variants (publish-while-consuming)
    "repid_streaming",
    "celery_streaming",
    "dramatiq_streaming",
    "faststream_streaming",
    "taskiq_streaming",
    # Bursty load variants
    "repid_burst",
    "celery_burst",
    "dramatiq_burst",
    "faststream_burst",
    "taskiq_burst",
    # Latency variants
    "repid_latency",
    "celery_latency",
    "dramatiq_latency",
    "faststream_latency",
    "taskiq_latency",
]

DEFAULT_SLEEP_TIMES = [0.01, 0.1, 0.5, 1.0, 5.0]
DEFAULT_RUNS = 5
DEFAULT_MESSAGES = 80000


DEFAULT_MESSAGES_PER_FRAMEWORK_SLEEP: dict[str, dict[float, int]] = {
    "repid": {
        0.01: 200_000,
        0.1: 200_000,
        0.5: 200_000,
        1.0: 200_000,
        5.0: 100_000,
    },
    "celery": {
        0.01: 30_000,
        0.1: 25_000,
        0.5: 25_000,
        1.0: 20_000,
        5.0: 20_000,
    },
    "celery_nogt": {
        0.01: 20_000,
        0.1: 5_000,
    },
    "dramatiq": {
        0.01: 5_000,
        0.1: 5_000,
        0.5: 5_000,
        1.0: 4_000,
        5.0: 2_000,
    },
    "dramatiq_nogt": {
        0.01: 30_000,
        0.1: 10_000,
        0.5: 5_000,
        1.0: 2_000,
    },
    "faststream": {
        0.01: 95_000,
        0.1: 95_000,
        0.5: 95_000,
        1.0: 95_000,
        5.0: 90_000,
    },
    "taskiq": {
        0.01: 10_000,
        0.1: 15_000,
        0.5: 5_000,
        1.0: 2_000,
        5.0: 2_000,
    },
    # High-concurrency variants — same order-of-magnitude as base
    "repid_hc": {0.01: 200_000, 0.1: 200_000, 0.5: 200_000, 1.0: 200_000, 5.0: 100_000},
    "celery_hc": {0.01: 30_000, 0.1: 25_000, 0.5: 25_000, 1.0: 20_000, 5.0: 20_000},
    "dramatiq_hc": {0.01: 5_000, 0.1: 5_000, 0.5: 5_000, 1.0: 4_000, 5.0: 2_000},
    "faststream_hc": {0.01: 95_000, 0.1: 95_000, 0.5: 95_000, 1.0: 95_000, 5.0: 90_000},
    "taskiq_hc": {0.01: 10_000, 0.1: 15_000, 0.5: 5_000, 1.0: 2_000},
    # CPU-bound variants — fewer sleep times (long CPU tasks are impractical)
    "repid_cpu": {0.01: 6_000, 0.1: 2_000},
    "celery_cpu": {0.01: 4_000, 0.1: 2_000},
    "dramatiq_cpu": {0.01: 3_000, 0.1: 2_000},
    "faststream_cpu": {0.01: 7_000, 0.1: 2_000},
    "taskiq_cpu": {0.01: 6_000, 0.1: 2_000},
    # Streaming variants — same message counts as base
    "repid_streaming": {0.01: 200_000, 0.1: 200_000, 0.5: 200_000, 1.0: 200_000, 5.0: 100_000},
    "celery_streaming": {0.01: 30_000, 0.1: 25_000, 0.5: 25_000, 1.0: 20_000, 5.0: 20_000},
    "dramatiq_streaming": {0.01: 5_000, 0.1: 5_000, 0.5: 5_000, 1.0: 4_000, 5.0: 2_000},
    "faststream_streaming": {0.01: 95_000, 0.1: 95_000, 0.5: 95_000, 1.0: 95_000, 5.0: 90_000},
    "taskiq_streaming": {0.01: 10_000, 0.1: 15_000, 0.5: 5_000, 1.0: 2_000},
    # Burst variants — same message counts as base
    "repid_burst": {0.01: 200_000, 0.1: 200_000, 0.5: 200_000, 1.0: 200_000, 5.0: 100_000},
    "celery_burst": {0.01: 30_000, 0.1: 25_000, 0.5: 25_000, 1.0: 20_000, 5.0: 20_000},
    "dramatiq_burst": {0.01: 5_000, 0.1: 5_000, 0.5: 5_000, 1.0: 4_000, 5.0: 2_000},
    "faststream_burst": {0.01: 95_000, 0.1: 95_000, 0.5: 95_000, 1.0: 95_000, 5.0: 90_000},
    "taskiq_burst": {0.01: 10_000, 0.1: 15_000, 0.5: 5_000, 1.0: 2_000},
    # Latency variants — enough messages for stable percentile measurement
    "repid_latency": {0.01: 20_000, 0.1: 20_000, 0.5: 20_000, 1.0: 20_000},
    "celery_latency": {0.01: 20_000, 0.1: 20_000, 0.5: 20_000, 1.0: 20_000},
    "dramatiq_latency": {0.01: 20_000, 0.1: 20_000, 0.5: 20_000, 1.0: 20_000},
    "faststream_latency": {0.01: 20_000, 0.1: 20_000, 0.5: 20_000, 1.0: 20_000},
    "taskiq_latency": {0.01: 20_000, 0.1: 20_000, 0.5: 20_000, 1.0: 20_000},
}

# Latency frameworks use fewer sleep times by default (no 5.0s).
LATENCY_DEFAULT_SLEEP_TIMES = [0.01, 0.1, 0.5, 1.0]


def _is_latency_fw(fw: str) -> bool:
    return fw.endswith("_latency")


def _is_cpu_fw(fw: str) -> bool:
    return fw.endswith("_cpu")


CPU_DEFAULT_SLEEP_TIMES = [0.01, 0.1]


def run_once(
    framework: str,
    sleep_time: float,
    messages: int,
    extra_env: dict[str, str] | None = None,
) -> dict[str, float] | None:
    """Run a single benchmark and return metrics.

    For latency frameworks returns {throughput, p50_ms, p95_ms, p99_ms}.
    For others returns {throughput}.
    """
    env = {
        **os.environ,
        "SLEEP_TIME": str(sleep_time),
        "MESSAGES_AMOUNT": str(messages),
        **(extra_env or {}),
    }

    script = f"bench_{framework}.py"

    try:
        proc = subprocess.Popen(
            [sys.executable, "-u", script],
            cwd=BENCHMARKS_DIR,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
    except Exception as e:
        print(f"\n[ERROR] Could not start {framework}: {e}")
        return None

    stdout_lines: list[str] = []
    stderr_data: list[str] = [""]

    def _read_stdout() -> None:
        assert proc.stdout is not None
        for raw in proc.stdout:
            line = raw.rstrip("\r\n")
            stdout_lines.append(line)
            if line.startswith("Enqueued:"):
                print(f"  {line}  ", end="\r", flush=True)

    def _read_stderr() -> None:
        assert proc.stderr is not None
        stderr_data[0] = proc.stderr.read()

    t_out = threading.Thread(target=_read_stdout, daemon=True)
    t_err = threading.Thread(target=_read_stderr, daemon=True)
    t_out.start()
    t_err.start()

    timed_out = False
    try:
        proc.wait(timeout=600)
    except subprocess.TimeoutExpired:
        proc.kill()
        timed_out = True

    t_out.join(timeout=30)
    t_err.join(timeout=30)

    print(" " * 60, end="\r", flush=True)

    if timed_out:
        print(f"\n[TIMEOUT] {framework} @ sleep={sleep_time}")
        return None

    if proc.returncode != 0:
        print(
            f"\n[ERROR] {framework} @ sleep={sleep_time} exited with code {proc.returncode}"
        )
        if stderr_data[0]:
            print(stderr_data[0][-2000:])
        return None

    stdout = "\n".join(stdout_lines)

    throughput_match = re.search(r"^THROUGHPUT:\s*([\d.]+)", stdout, re.MULTILINE)
    if not throughput_match:
        print(
            f"\n[WARN] No THROUGHPUT line found in output of {framework} @ sleep={sleep_time}"
        )
        return None

    result: dict[str, float] = {"throughput": float(throughput_match.group(1))}

    # Capture latency metrics if present (emitted by *_latency.py benchmarks).
    for tag in ("P50", "P95", "P99"):
        m = re.search(rf"^LATENCY_{tag}:\s*([\d.]+)", stdout, re.MULTILINE)
        if m:
            result[f"{tag.lower()}_ms"] = float(m.group(1))

    return result


def fmt_cell(values: list[float]) -> str:
    if not values:
        return "FAILED"
    m = mean(values)
    if len(values) > 1:
        s = stdev(values)
        return f"{m:.1f}±{s:.1f}"
    return f"{m:.1f}"


def print_table(
    frameworks: list[str],
    sleep_times: list[float],
    throughput_results: dict[str, dict[float, list[float]]],
    runs: int,
) -> None:
    fw_w = max(len(fw) for fw in frameworks) + 2
    col_w = 16
    header = f"{'Framework':<{fw_w}}" + "".join(
        f"{'sleep=' + str(s):>{col_w}}" for s in sleep_times
    )
    sep = "─" * len(header)
    print()
    print(sep)
    print(f"  Throughput (msg/sec) — mean ± std over {runs} runs")
    print(sep)
    print(header)
    print(sep)
    for fw in frameworks:
        row = f"{fw:<{fw_w}}"
        for st in sleep_times:
            if st in throughput_results[fw]:
                row += f"{fmt_cell(throughput_results[fw][st]):>{col_w}}"
            else:
                row += f"{'—':>{col_w}}"
        print(row)
    print(sep)


def print_latency_table(
    frameworks: list[str],
    sleep_times: list[float],
    latency_results: dict[str, dict[float, dict[str, list[float]]]],
    runs: int,
) -> None:
    fw_w = max(len(fw) for fw in frameworks) + 2
    col_w = 16
    for metric, label in [
        ("p50_ms", "Latency p50 (ms)"),
        ("p95_ms", "Latency p95 (ms)"),
        ("p99_ms", "Latency p99 (ms)"),
    ]:
        header = f"{'Framework':<{fw_w}}" + "".join(
            f"{'sleep=' + str(s):>{col_w}}" for s in sleep_times
        )
        sep = "─" * len(header)
        print()
        print(sep)
        print(f"  {label} — mean ± std over {runs} runs")
        print(sep)
        print(header)
        print(sep)
        for fw in frameworks:
            row = f"{fw:<{fw_w}}"
            for st in sleep_times:
                vals = latency_results[fw][st].get(metric, [])
                row += f"{fmt_cell(vals):>{col_w}}"
            print(row)
        print(sep)


def save_csv(
    frameworks: list[str],
    sleep_times: list[float],
    throughput_results: dict[str, dict[float, list[float]]],
    latency_results: dict[str, dict[float, dict[str, list[float]]]] | None = None,
) -> Path:
    csv_path = DIR / "benchmarks_results.csv"
    with csv_path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["framework", "sleep_time", "run", "throughput_msg_per_sec"])
        for fw in frameworks:
            for st in sleep_times:
                if st not in throughput_results[fw]:
                    continue
                for i, val in enumerate(throughput_results[fw][st], 1):
                    writer.writerow([fw, st, i, f"{val:.2f}"])
    return csv_path


def save_latency_csv(
    frameworks: list[str],
    sleep_times: list[float],
    latency_results: dict[str, dict[float, dict[str, list[float]]]],
) -> Path:
    csv_path = DIR / "latency_results.csv"
    with csv_path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            ["framework", "sleep_time", "run", "throughput_msg_per_sec", "p50_ms", "p95_ms", "p99_ms"]
        )
        for fw in frameworks:
            for st in sleep_times:
                r = latency_results[fw][st]
                n = len(r.get("throughput", []))
                for i in range(n):
                    writer.writerow([
                        fw,
                        st,
                        i + 1,
                        f"{r['throughput'][i]:.2f}",
                        f"{r['p50_ms'][i]:.3f}",
                        f"{r['p95_ms'][i]:.3f}",
                        f"{r['p99_ms'][i]:.3f}",
                    ])
    return csv_path


def suggest_message_counts(
    frameworks: list[str],
    sleep_times: list[float],
    results: dict[str, dict[float, list[float]]],
    target_seconds: float = 45.0,
) -> None:
    print()
    print("─" * 72)
    print(
        f"  Suggested DEFAULT_MESSAGES_PER_FRAMEWORK_SLEEP  (target ≈ {target_seconds:.0f}s/run)"
    )
    print("─" * 72)
    print("DEFAULT_MESSAGES_PER_FRAMEWORK_SLEEP: dict[str, dict[float, int]] = {")
    any_missing = False
    for fw in frameworks:
        print(f'    "{fw}": {{')
        fw_sts = [st for st in sleep_times if st in results[fw]]
        if not fw_sts:
            print("    },")
            continue
        for st in fw_sts:
            vals = results[fw][st]
            if not vals:
                print(f"        {st}: ???,  # no data")
                any_missing = True
                continue
            avg_t = mean(vals)
            n = max(1_000, round(avg_t * target_seconds / 1_000) * 1_000)
            estimated_dur = n / avg_t
            print(
                f"        {st}: {n:>10_},  # avg {avg_t:>8.0f} msg/s -> {estimated_dur:.0f}s"
            )
        print("    },")
    print("}")
    print("─" * 72)
    if any_missing:
        print("  NOTE: some (fw, sleep_time) combinations had no successful runs.")
        print("  Replace ??? entries manually before using these values.")
        print("─" * 72)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run all benchmarks and compare.")
    parser.add_argument(
        "--frameworks",
        nargs="+",
        default=ALL_FRAMEWORKS,
        choices=ALL_FRAMEWORKS,
        metavar="FW",
        help=f"Frameworks to benchmark (default: all). Choices: {ALL_FRAMEWORKS}",
    )
    parser.add_argument(
        "--sleep-times",
        nargs="+",
        type=float,
        default=None,
        metavar="S",
        help="Sleep durations (seconds) to test (default: [0.01, 0.1, 0.5, 1.0, 5.0]; latency variants use [0.01, 0.1, 0.5, 1.0]).",
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=DEFAULT_RUNS,
        metavar="N",
        help=f"Number of repeated runs per combination (default: {DEFAULT_RUNS})",
    )
    parser.add_argument(
        "--messages",
        type=int,
        default=None,
        metavar="N",
        help="Messages per run for all frameworks/sleep-times (overrides all defaults).",
    )
    parser.add_argument(
        "--messages-per-framework",
        nargs="+",
        default=[],
        metavar="FW:N",
        help="Override message count for a specific framework across all its sleep times, e.g. celery_nogt:5000. Takes precedence over --messages and per-(fw,sleep) defaults.",
    )
    parser.add_argument(
        "--amqp-url",
        default="amqp://user:testtest@localhost:5672",
        metavar="URL",
        help="AMQP broker URL passed to every benchmark (default: amqp://user:testtest@localhost:5672)",
    )
    parser.add_argument(
        "--rabbitmq-mgmt-url",
        default=None,
        metavar="URL",
        help="RabbitMQ management HTTP URL for queue purging. Derived from --amqp-url hostname by default.",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Load existing CSV files and skip already-completed (framework, sleep_time, run) triples.",
    )
    args = parser.parse_args()

    frameworks: list[str] = args.frameworks

    # Default sleep times: latency variants use a shorter list (no 5.0s).
    if args.sleep_times is not None:
        sleep_times = sorted(args.sleep_times)
    else:
        sleep_times_for_fw: dict[str, list[float]] = {}
        for fw in frameworks:
            if _is_latency_fw(fw):
                sleep_times_for_fw[fw] = sorted(LATENCY_DEFAULT_SLEEP_TIMES)
            else:
                sleep_times_for_fw[fw] = sorted(DEFAULT_SLEEP_TIMES)
        # Use the union of all unique sleep times.
        all_st = set()
        for st_list in sleep_times_for_fw.values():
            all_st.update(st_list)
        sleep_times = sorted(all_st)

    runs: int = args.runs

    # CLI overrides: --messages-per-framework FW:N
    fw_overrides: dict[str, int] = {}
    for override in args.messages_per_framework:
        fw_name, n_str = override.split(":", 1)
        if fw_name not in args.frameworks:
            print(
                f"[WARN] --messages-per-framework: {fw_name!r} not in selected frameworks, ignoring."
            )
            continue
        fw_overrides[fw_name] = int(n_str)

    def get_messages(fw: str, st: float) -> int | None:
        """Return message count for (fw, st) respecting CLI overrides then defaults."""
        if fw in fw_overrides:
            return fw_overrides[fw]
        if args.messages is not None:
            return args.messages
        fw_table = DEFAULT_MESSAGES_PER_FRAMEWORK_SLEEP.get(fw, {})
        if st in fw_table:
            return fw_table[st]
        return None

    def get_sleep_times_for_fw(fw: str) -> list[float]:
        """Return the sleep times applicable for a given framework."""
        if args.sleep_times is not None:
            return sleep_times
        if _is_latency_fw(fw):
            return sorted(LATENCY_DEFAULT_SLEEP_TIMES)
        if _is_cpu_fw(fw):
            return sorted(CPU_DEFAULT_SLEEP_TIMES)
        return sorted(DEFAULT_SLEEP_TIMES)

    venv_bin = str(DIR / ".venv" / "bin")
    existing_path = os.environ.get("PATH", "")
    if venv_bin not in existing_path:
        existing_path = f"{venv_bin}:{existing_path}"

    extra_env: dict[str, str] = {
        "AMQP_URL": args.amqp_url,
        "PATH": existing_path,
    }
    if args.rabbitmq_mgmt_url is not None:
        extra_env["RABBITMQ_MGMT_URL"] = args.rabbitmq_mgmt_url

    # Build per-framework sleep time lists.
    fw_sleep_times = {fw: get_sleep_times_for_fw(fw) for fw in frameworks}
    # Total number of runs.
    total = sum(len(sts) * runs for fw, sts in fw_sleep_times.items())

    throughput_results: dict[str, dict[float, list[float]]] = {
        fw: {st: [] for st in fw_sleep_times[fw]} for fw in frameworks
    }
    # Latency results only populated for latency frameworks.
    latency_results: dict[str, dict[float, dict[str, list[float]]]] = {
        fw: {st: {"throughput": [], "p50_ms": [], "p95_ms": [], "p99_ms": []} for st in fw_sleep_times[fw]}
        for fw in frameworks if _is_latency_fw(fw)
    }

    throughput_csv_path = DIR / "benchmarks_results.csv"
    latency_csv_path = DIR / "latency_results.csv"

    # ── Resume support ──────────────────────────────────────────────────────
    resume_skip: set[tuple[str, float, int]] = set()

    if args.resume:
        if throughput_csv_path.exists():
            with throughput_csv_path.open(newline="") as f:
                for row in csv.DictReader(f):
                    fw, st, val = (
                        row["framework"],
                        float(row["sleep_time"]),
                        float(row["throughput_msg_per_sec"]),
                    )
                    if fw in throughput_results and st in throughput_results[fw]:
                        throughput_results[fw][st].append(val)

        if latency_csv_path.exists():
            with latency_csv_path.open(newline="") as f:
                for row in csv.DictReader(f):
                    fw, st = row["framework"], float(row["sleep_time"])
                    if fw in latency_results and st in latency_results[fw]:
                        latency_results[fw][st]["throughput"].append(float(row["throughput_msg_per_sec"]))
                        latency_results[fw][st]["p50_ms"].append(float(row["p50_ms"]))
                        latency_results[fw][st]["p95_ms"].append(float(row["p95_ms"]))
                        latency_results[fw][st]["p99_ms"].append(float(row["p99_ms"]))

        total_loaded = sum(len(v) for r in throughput_results.values() for v in r.values())
        print(f"[RESUME] Loaded {total_loaded} existing throughput results from {throughput_csv_path}")

        throughput_file = throughput_csv_path.open("a", newline="")
        throughput_writer = csv.writer(throughput_file)
        latency_file = latency_csv_path.open("a", newline="")
        latency_writer = csv.writer(latency_file)
    else:
        throughput_file = throughput_csv_path.open("w", newline="")
        throughput_writer = csv.writer(throughput_file)
        throughput_writer.writerow(["framework", "sleep_time", "run", "throughput_msg_per_sec"])

        latency_file = latency_csv_path.open("w", newline="")
        latency_writer = csv.writer(latency_file)
        latency_writer.writerow(
            ["framework", "sleep_time", "run", "throughput_msg_per_sec", "p50_ms", "p95_ms", "p99_ms"]
        )

    done = 0

    try:
        for framework in frameworks:
            for sleep_time in fw_sleep_times[framework]:
                for run_idx in range(1, runs + 1):
                    done += 1

                    # Skip already-completed runs when resuming.
                    existing = len(throughput_results[framework][sleep_time])
                    if run_idx <= existing:
                        print(
                            f"[DONE]  {framework:<10}  sleep={sleep_time:<5}  run={run_idx}/{runs}  (already recorded)"
                        )
                        continue

                    messages = get_messages(framework, sleep_time)
                    if messages is None:
                        done += runs - run_idx
                        print(
                            f"[SKIP]  {framework:<10}  sleep={sleep_time:<5}  (no default message count)"
                        )
                        break

                    label = f"[{done}/{total}] {framework:<10}  sleep={sleep_time:<5}  run={run_idx}/{runs}"
                    print(label, "...", flush=True)

                    result = run_once(framework, sleep_time, messages, extra_env)
                    if result is not None:
                        throughput = result["throughput"]
                        throughput_results[framework][sleep_time].append(throughput)
                        throughput_writer.writerow(
                            [framework, sleep_time, run_idx, f"{throughput:.2f}"]
                        )
                        throughput_file.flush()

                        # Latency data.
                        is_latency = _is_latency_fw(framework)
                        latency_parts = []
                        if "p50_ms" in result:
                            latency_parts.append(f"p50={result['p50_ms']:.1f}ms")
                        if "p95_ms" in result:
                            latency_parts.append(f"p95={result['p95_ms']:.1f}ms")
                        if "p99_ms" in result:
                            latency_parts.append(f"p99={result['p99_ms']:.1f}ms")

                        suffix = "  " + "  ".join(latency_parts) if latency_parts else ""
                        print(f"{'':>{len(label)}}  → {throughput:.1f} msg/s{suffix}")

                        if is_latency and framework in latency_results:
                            for metric in ("throughput", "p50_ms", "p95_ms", "p99_ms"):
                                if metric in result:
                                    latency_results[framework][sleep_time][metric].append(result[metric])
                            latency_writer.writerow([
                                framework,
                                sleep_time,
                                run_idx,
                                f"{result['throughput']:.2f}",
                                f"{result.get('p50_ms', 0):.3f}",
                                f"{result.get('p95_ms', 0):.3f}",
                                f"{result.get('p99_ms', 0):.3f}",
                            ])
                            latency_file.flush()
    finally:
        throughput_file.close()
        latency_file.close()

    # ── Print tables ────────────────────────────────────────────────────────
    print_table(frameworks, sleep_times, throughput_results, runs)

    latency_fws = [fw for fw in frameworks if _is_latency_fw(fw)]
    if latency_fws:
        latency_sts = sorted(
            {st for fw in latency_fws for st in fw_sleep_times[fw]}
        )
        latency_filtered: dict[str, dict[float, dict[str, list[float]]]] = {
            fw: latency_results[fw] for fw in latency_fws
        }
        print_latency_table(latency_fws, latency_sts, latency_filtered, runs)

    csv_saved = save_csv(frameworks, sleep_times, throughput_results)
    print(f"\nThroughput results saved to {csv_saved}")

    if latency_fws:
        latency_csv_saved = save_latency_csv(latency_fws, latency_sts, latency_filtered)
        print(f"Latency results saved to {latency_csv_saved}")

    suggest_message_counts(frameworks, sleep_times, throughput_results)


if __name__ == "__main__":
    main()
