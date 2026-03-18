#!/usr/bin/env python3
"""
Benchmark orchestrator — runs all frameworks across multiple sleep times and
collects throughput statistics.

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
]
DEFAULT_SLEEP_TIMES = [0.01, 0.1, 0.5, 1.0, 5.0]
DEFAULT_RUNS = 5
DEFAULT_MESSAGES = 80000


DEFAULT_MESSAGES_PER_FRAMEWORK_SLEEP: dict[str, dict[float, int]] = {
    "repid": {
        0.01:  2_151_000,  # avg    47790 msg/s -> 45s
        0.1:  1_902_000,  # avg    42256 msg/s -> 45s
        0.5:  1_361_000,  # avg    30240 msg/s -> 45s
        1.0:    713_000,  # avg    15850 msg/s -> 45s
        5.0:    151_000,  # avg     3355 msg/s -> 45s
    },
    "celery": {
        0.01:     92_000,  # avg     2054 msg/s -> 45s
        0.1:     88_000,  # avg     1946 msg/s -> 45s
        0.5:     84_000,  # avg     1873 msg/s -> 45s
        1.0:     70_000,  # avg     1561 msg/s -> 45s
        5.0:     64_000,  # avg     1433 msg/s -> 45s
    },
    "celery_nogt": {
        0.01:     34_000,  # avg      748 msg/s -> 45s
        0.1:      4_000,  # avg       81 msg/s -> 50s
    },
    "dramatiq": {
        0.01:    357_000,  # avg     7942 msg/s -> 45s
        0.1:    300_000,  # avg     6672 msg/s -> 45s
        0.5:    404_000,  # avg     8967 msg/s -> 45s
        1.0:    385_000,  # avg     8563 msg/s -> 45s
        5.0:    148_000,  # avg     3282 msg/s -> 45s
    },
    "dramatiq_nogt": {
        0.01:    279_000,  # avg     6209 msg/s -> 45s
        0.1:     29_000,  # avg      638 msg/s -> 45s
        0.5:      6_000,  # avg      128 msg/s -> 47s
        1.0:      3_000,  # avg       64 msg/s -> 47s
    },
    "faststream": {
        0.01:    860_000,  # avg    19119 msg/s -> 45s
        0.1:    818_000,  # avg    18170 msg/s -> 45s
        0.5:    716_000,  # avg    15910 msg/s -> 45s
        1.0:    628_000,  # avg    13956 msg/s -> 45s
        5.0:    150_000,  # avg     3324 msg/s -> 45s
    },
    "taskiq": {
        0.01:    260_000,  # avg     5786 msg/s -> 45s
        0.1:     35_000,  # avg      783 msg/s -> 45s
        0.5:      7_000,  # avg      159 msg/s -> 44s
        1.0:      4_000,  # avg       80 msg/s -> 50s
    },
}


def run_once(
    framework: str,
    sleep_time: float,
    messages: int,
    extra_env: dict[str, str] | None = None,
) -> float | None:
    env = {
        **os.environ,
        "SLEEP_TIME": str(sleep_time),
        "MESSAGES_AMOUNT": str(messages),
        **(extra_env or {}),
    }
    try:
        proc = subprocess.Popen(
            [sys.executable, "-u", f"bench_{framework}.py"],
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

    # Clear the publishing progress line
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
    match = re.search(r"^THROUGHPUT:\s*([\d.]+)", stdout, re.MULTILINE)
    if match:
        return float(match.group(1))

    print(
        f"\n[WARN] No THROUGHPUT line found in output of {framework} @ sleep={sleep_time}"
    )
    return None


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
    results: dict[str, dict[float, list[float]]],
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
            row += f"{fmt_cell(results[fw][st]):>{col_w}}"
        print(row)
    print(sep)


def save_csv(
    frameworks: list[str],
    sleep_times: list[float],
    results: dict[str, dict[float, list[float]]],
) -> Path:
    csv_path = DIR / "benchmarks_results.csv"
    with csv_path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["framework", "sleep_time", "run", "throughput_msg_per_sec"])
        for fw in frameworks:
            for st in sleep_times:
                for i, val in enumerate(results[fw][st], 1):
                    writer.writerow([fw, st, i, f"{val:.2f}"])
    return csv_path


def suggest_message_counts(
    frameworks: list[str],
    sleep_times: list[float],
    results: dict[str, dict[float, list[float]]],
    target_seconds: float = 45.0,
) -> None:
    """Print a ready-to-paste DEFAULT_MESSAGES_PER_FRAMEWORK_SLEEP dict
    derived from the measured throughputs, targeting *target_seconds* per run.
    Values are rounded to the nearest 1 000; minimum 1 000.
    """
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
        for st in sleep_times:
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
        default=DEFAULT_SLEEP_TIMES,
        metavar="S",
        help=f"Sleep durations (seconds) to test (default: {DEFAULT_SLEEP_TIMES})",
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
        help="Load existing benchmarks_results.csv and skip already-completed (framework, sleep_time, run) triples.",
    )
    args = parser.parse_args()

    frameworks: list[str] = args.frameworks
    sleep_times: list[float] = sorted(args.sleep_times)
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

    extra_env: dict[str, str] = {
        "AMQP_URL": args.amqp_url,
    }
    if args.rabbitmq_mgmt_url is not None:
        extra_env["RABBITMQ_MGMT_URL"] = args.rabbitmq_mgmt_url

    results: dict[str, dict[float, list[float]]] = {
        fw: {st: [] for st in sleep_times} for fw in frameworks
    }

    csv_path = DIR / "benchmarks_results.csv"

    # Pre-populate results from an existing CSV when resuming.
    if args.resume and csv_path.exists():
        with csv_path.open(newline="") as f:
            for row in csv.DictReader(f):
                fw, st, val = (
                    row["framework"],
                    float(row["sleep_time"]),
                    float(row["throughput_msg_per_sec"]),
                )
                if fw in results and st in results[fw]:
                    results[fw][st].append(val)
        total_loaded = sum(len(v) for r in results.values() for v in r.values())
        print(f"[RESUME] Loaded {total_loaded} existing results from {csv_path}")
        csv_file = csv_path.open("a", newline="")
        csv_writer = csv.writer(csv_file)
    else:
        csv_file = csv_path.open("w", newline="")
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(
            ["framework", "sleep_time", "run", "throughput_msg_per_sec"]
        )

    total = len(frameworks) * len(sleep_times) * runs
    done = 0

    try:
        for sleep_time in sleep_times:
            for framework in frameworks:
                for run_idx in range(1, runs + 1):
                    done += 1
                    # Skip runs already present from a previous interrupted session.
                    if run_idx <= len(results[framework][sleep_time]):
                        print(
                            f"[DONE]  {framework:<10}  sleep={sleep_time:<5}  run={run_idx}/{runs}  (already recorded)"
                        )
                        continue

                    messages = get_messages(framework, sleep_time)
                    if messages is None:
                        done += runs - run_idx  # skip remaining runs for this combo
                        print(
                            f"[SKIP]  {framework:<10}  sleep={sleep_time:<5}  (no default message count)"
                        )
                        break

                    label = f"[{done}/{total}] {framework:<10}  sleep={sleep_time:<5}  run={run_idx}/{runs}"
                    print(label, "...", flush=True)

                    throughput = run_once(
                        framework,
                        sleep_time,
                        messages,
                        extra_env,
                    )
                    if throughput is not None:
                        results[framework][sleep_time].append(throughput)
                        csv_writer.writerow(
                            [framework, sleep_time, run_idx, f"{throughput:.2f}"]
                        )
                        csv_file.flush()
                        print(f"{'':>{len(label)}}  → {throughput:.1f} msg/s")
    finally:
        csv_file.close()

    print_table(frameworks, sleep_times, results, runs)
    print(f"\nDetailed results saved to {csv_path}")

    suggest_message_counts(frameworks, sleep_times, results)


if __name__ == "__main__":
    main()
