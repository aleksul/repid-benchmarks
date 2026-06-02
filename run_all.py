#!/usr/bin/env python3
"""Benchmark orchestrator.

All benchmark variants are defined in ``benchmarks._specs`` and executed through
``benchmarks._runner``. Direct per-variant benchmark entry points are not part of
the public workflow anymore.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import platform
import random
import re
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path
from statistics import mean, stdev

from benchmarks._rabbitmq import management_url_from_amqp
from benchmarks._runtime import BenchmarkConfig, write_config
from benchmarks._specs import ALL_BENCHMARKS, SPECS, get_spec
from benchmarks._work import calibrate_cpu_work_iterations

ROOT = Path(__file__).parent
DEFAULT_RUNS = 5
DEFAULT_TIME_LIMIT = 300.0
DEFAULT_BURST_SIZE = 5000
DEFAULT_BURST_INTERVAL = 1.0


def _is_latency(name: str) -> bool:
    return get_spec(name).mode == "latency"


def _is_cpu(name: str) -> bool:
    return get_spec(name).task_kind == "cpu"


def fmt_cell(values: list[float]) -> str:
    if not values:
        return "FAILED"
    avg = mean(values)
    return f"{avg:.1f}±{stdev(values):.1f}" if len(values) > 1 else f"{avg:.1f}"


def run_once(config: BenchmarkConfig) -> dict[str, float] | None:
    config_path = write_config(config)
    env = os.environ.copy()
    venv_bin = str(ROOT / ".venv" / "bin")
    if venv_bin not in env.get("PATH", ""):
        env["PATH"] = f"{venv_bin}:{env.get('PATH', '')}"
    env["BENCHMARK_CONFIG_PATH"] = str(config_path)

    child_python = ROOT / ".venv" / "bin" / "python"
    executable = str(child_python) if child_python.exists() else sys.executable

    try:
        proc = subprocess.Popen(
            [executable, "-u", "-m", "benchmarks._runner", "--config", str(config_path)],
            cwd=ROOT,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
    except Exception as e:
        print(f"\n[ERROR] Could not start {config.name}: {e}")
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
        proc.wait(timeout=config.time_limit + 180)
    except subprocess.TimeoutExpired:
        proc.kill()
        timed_out = True

    t_out.join(timeout=30)
    t_err.join(timeout=30)
    print(" " * 80, end="\r", flush=True)

    try:
        config_path.unlink()
    except FileNotFoundError:
        pass

    if timed_out:
        print(f"\n[TIMEOUT] {config.name} @ sleep={config.sleep_time}")
        return None
    if proc.returncode != 0:
        print(f"\n[ERROR] {config.name} @ sleep={config.sleep_time} exited with code {proc.returncode}")
        if stderr_data[0]:
            print(stderr_data[0][-4000:])
        return None

    stdout = "\n".join(stdout_lines)
    throughput_match = re.search(r"^THROUGHPUT:\s*([\d.]+)", stdout, re.MULTILINE)
    if not throughput_match:
        print(f"\n[WARN] No THROUGHPUT line found for {config.name} @ sleep={config.sleep_time}")
        return None

    result: dict[str, float] = {"throughput": float(throughput_match.group(1))}
    for tag in ("P50", "P95", "P99"):
        match = re.search(rf"^LATENCY_{tag}:\s*([\d.]+)", stdout, re.MULTILINE)
        if match:
            result[f"{tag.lower()}_ms"] = float(match.group(1))
    return result


def print_table(frameworks: list[str], sleep_times: list[float], results: dict[str, dict[float, list[float]]], runs: int) -> None:
    fw_w = max(len(fw) for fw in frameworks) + 2
    col_w = 16
    header = f"{'Framework':<{fw_w}}" + "".join(f"{'sleep=' + str(s):>{col_w}}" for s in sleep_times)
    sep = "-" * len(header)
    print("\n" + sep)
    print(f"  Throughput (msg/sec) - mean +/- std over {runs} runs")
    print(sep)
    print(header)
    print(sep)
    for fw in frameworks:
        row = f"{fw:<{fw_w}}"
        for st in sleep_times:
            row += f"{fmt_cell(results.get(fw, {}).get(st, [])):>{col_w}}" if st in results.get(fw, {}) else f"{'-':>{col_w}}"
        print(row)
    print(sep)


def print_latency_table(frameworks: list[str], sleep_times: list[float], results: dict[str, dict[float, dict[str, list[float]]]], runs: int) -> None:
    fw_w = max(len(fw) for fw in frameworks) + 2
    col_w = 16
    for metric, label in (("p50_ms", "Latency p50"), ("p95_ms", "Latency p95"), ("p99_ms", "Latency p99")):
        header = f"{'Framework':<{fw_w}}" + "".join(f"{'sleep=' + str(s):>{col_w}}" for s in sleep_times)
        sep = "-" * len(header)
        print("\n" + sep)
        print(f"  {label} (ms) - mean +/- std over {runs} runs")
        print(sep)
        print(header)
        print(sep)
        for fw in frameworks:
            row = f"{fw:<{fw_w}}"
            for st in sleep_times:
                row += f"{fmt_cell(results[fw][st].get(metric, [])):>{col_w}}"
            print(row)
        print(sep)


def suggest_message_counts(frameworks: list[str], sleep_times: list[float], results: dict[str, dict[float, list[float]]], target_seconds: float = 45.0) -> None:
    print("\n" + "-" * 72)
    print(f"  Suggested message counts (target ~= {target_seconds:.0f}s/run)")
    print("-" * 72)
    for fw in frameworks:
        print(f"{fw}:")
        for st in sleep_times:
            vals = results.get(fw, {}).get(st, [])
            if not vals:
                continue
            avg = mean(vals)
            n = max(1_000, round(avg * target_seconds / 1_000) * 1_000)
            print(f"  {st}: {n:_}  # avg {avg:.0f} msg/s")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run benchmark specs and compare throughput.")
    parser.add_argument("--frameworks", nargs="+", default=ALL_BENCHMARKS, choices=ALL_BENCHMARKS, metavar="FW")
    parser.add_argument("--sleep-times", nargs="+", type=float, default=None, metavar="S")
    parser.add_argument("--runs", type=int, default=DEFAULT_RUNS, metavar="N")
    parser.add_argument("--warmup-runs", type=int, default=0, metavar="N")
    parser.add_argument("--messages", type=int, default=None, metavar="N")
    parser.add_argument("--messages-per-framework", nargs="+", default=[], metavar="FW:N")
    parser.add_argument("--cpu-work-iterations", type=int, default=None, metavar="N")
    parser.add_argument("--amqp-url", default="amqp://user:testtest@localhost:5672", metavar="URL")
    parser.add_argument("--rabbitmq-mgmt-url", default=None, metavar="URL")
    parser.add_argument("--time-limit", type=float, default=DEFAULT_TIME_LIMIT)
    parser.add_argument("--burst-size", type=int, default=DEFAULT_BURST_SIZE)
    parser.add_argument("--burst-interval", type=float, default=DEFAULT_BURST_INTERVAL)
    parser.add_argument("--randomize-order", action="store_true")
    parser.add_argument("--keep-queues", action="store_true")
    parser.add_argument("--worker-log-dir", default=None)
    parser.add_argument("--resume", action="store_true")
    args = parser.parse_args()

    frameworks: list[str] = args.frameworks
    mgmt_url = args.rabbitmq_mgmt_url or management_url_from_amqp(args.amqp_url)

    fw_overrides: dict[str, int] = {}
    for override in args.messages_per_framework:
        fw_name, n_str = override.split(":", 1)
        if fw_name not in frameworks:
            print(f"[WARN] --messages-per-framework: {fw_name!r} not selected, ignoring.")
            continue
        fw_overrides[fw_name] = int(n_str)

    def sleep_times_for(name: str) -> list[float]:
        return sorted(args.sleep_times if args.sleep_times is not None else get_spec(name).sleep_times)

    def messages_for(name: str, sleep_time: float) -> int | None:
        if name in fw_overrides:
            return fw_overrides[name]
        if args.messages is not None:
            return args.messages
        table = get_spec(name).messages or {}
        return table.get(sleep_time)

    fw_sleep_times = {fw: sleep_times_for(fw) for fw in frameworks}
    table_sleep_times = sorted({st for sts in fw_sleep_times.values() for st in sts})

    cpu_iterations_by_sleep: dict[float, int] = {}
    for fw in frameworks:
        if not _is_cpu(fw):
            continue
        for st in fw_sleep_times[fw]:
            iterations = args.cpu_work_iterations if args.cpu_work_iterations is not None else calibrate_cpu_work_iterations(st)
            cpu_iterations_by_sleep.setdefault(st, iterations)
    for st, iterations in sorted(cpu_iterations_by_sleep.items()):
        print(f"[CPU] sleep={st:<5} fixed work={iterations} hash iterations/task")

    throughput_results: dict[str, dict[float, list[float]]] = {fw: {st: [] for st in fw_sleep_times[fw]} for fw in frameworks}
    latency_results: dict[str, dict[float, dict[str, list[float]]]] = {
        fw: {st: {"throughput": [], "p50_ms": [], "p95_ms": [], "p99_ms": []} for st in fw_sleep_times[fw]}
        for fw in frameworks if _is_latency(fw)
    }

    throughput_csv_path = ROOT / "benchmarks_results.csv"
    latency_csv_path = ROOT / "latency_results.csv"
    metadata_path = ROOT / "benchmarks_metadata.jsonl"

    if args.resume and throughput_csv_path.exists():
        with throughput_csv_path.open(newline="") as f:
            for row in csv.DictReader(f):
                fw, st = row["framework"], float(row["sleep_time"])
                if fw in throughput_results and st in throughput_results[fw]:
                    throughput_results[fw][st].append(float(row["throughput_msg_per_sec"]))
    if args.resume and latency_csv_path.exists():
        with latency_csv_path.open(newline="") as f:
            for row in csv.DictReader(f):
                fw, st = row["framework"], float(row["sleep_time"])
                if fw in latency_results and st in latency_results[fw]:
                    latency_results[fw][st]["throughput"].append(float(row["throughput_msg_per_sec"]))
                    latency_results[fw][st]["p50_ms"].append(float(row["p50_ms"]))
                    latency_results[fw][st]["p95_ms"].append(float(row["p95_ms"]))
                    latency_results[fw][st]["p99_ms"].append(float(row["p99_ms"]))

    throughput_file = throughput_csv_path.open("a" if args.resume else "w", newline="")
    latency_file = latency_csv_path.open("a" if args.resume else "w", newline="")
    metadata_file = metadata_path.open("a" if args.resume else "w")
    throughput_writer = csv.writer(throughput_file)
    latency_writer = csv.writer(latency_file)
    if not args.resume:
        throughput_writer.writerow(["framework", "sleep_time", "run", "throughput_msg_per_sec"])
        latency_writer.writerow(["framework", "sleep_time", "run", "throughput_msg_per_sec", "p50_ms", "p95_ms", "p99_ms"])

    tasks: list[tuple[str, float, int, bool]] = []
    for fw in frameworks:
        for st in fw_sleep_times[fw]:
            if messages_for(fw, st) is None:
                print(f"[SKIP] {fw:<18} sleep={st:<5} (no message count)")
                continue
            for run_idx in range(1, args.warmup_runs + 1):
                tasks.append((fw, st, run_idx, True))
            existing = len(throughput_results[fw][st]) if args.resume else 0
            for run_idx in range(existing + 1, args.runs + 1):
                tasks.append((fw, st, run_idx, False))
    if args.randomize_order:
        random.shuffle(tasks)

    try:
        for idx, (fw, st, run_idx, warmup) in enumerate(tasks, 1):
            spec = get_spec(fw)
            messages = messages_for(fw, st)
            assert messages is not None
            queue_name = f"{spec.queue_base}_{uuid.uuid4().hex[:10]}"
            config = BenchmarkConfig(
                name=fw,
                framework=spec.framework,
                mode=spec.mode,
                task_kind=spec.task_kind,
                queue_name=queue_name,
                messages=messages,
                sleep_time=st,
                time_limit=args.time_limit,
                amqp_url=args.amqp_url,
                rabbitmq_mgmt_url=mgmt_url,
                processes=spec.processes,
                concurrency=spec.concurrency,
                publish_concurrency=spec.publish_concurrency,
                publish_workers=spec.publish_workers,
                enqueue_batch_size=spec.enqueue_batch_size,
                green_threads=spec.green_threads,
                burst_size=args.burst_size,
                burst_interval=args.burst_interval,
                cpu_work_iterations=cpu_iterations_by_sleep.get(st),
                keep_queue=args.keep_queues,
                worker_log_dir=args.worker_log_dir,
            )
            label = f"[{idx}/{len(tasks)}] {fw:<18} sleep={st:<5} run={run_idx}/{args.runs}"
            if warmup:
                label += " warmup"
            print(label, "...", flush=True)
            result = run_once(config)
            if result is None:
                continue
            throughput = result["throughput"]
            suffix_parts = [f"p50={result['p50_ms']:.1f}ms" for _ in [0] if "p50_ms" in result]
            suffix_parts += [f"p95={result['p95_ms']:.1f}ms" for _ in [0] if "p95_ms" in result]
            suffix_parts += [f"p99={result['p99_ms']:.1f}ms" for _ in [0] if "p99_ms" in result]
            print(f"{'':>{len(label)}}  -> {throughput:.1f} msg/s" + ("  " + "  ".join(suffix_parts) if suffix_parts else ""))
            if warmup:
                continue
            metadata_file.write(json.dumps({
                "benchmark": fw,
                "framework": spec.framework,
                "mode": spec.mode,
                "task_kind": spec.task_kind,
                "sleep_time": st,
                "messages": messages,
                "run": run_idx,
                "amqp_host": config.amqp_url.split("@")[-1].split("/")[0],
                "processes": config.processes,
                "concurrency": config.concurrency,
                "publish_concurrency": config.publish_concurrency,
                "publish_workers": config.publish_workers,
                "green_threads": config.green_threads,
                "python": platform.python_version(),
                "timestamp": time.time(),
            }) + "\n")
            metadata_file.flush()
            throughput_results[fw][st].append(throughput)
            throughput_writer.writerow([fw, st, run_idx, f"{throughput:.2f}"])
            throughput_file.flush()
            if _is_latency(fw) and fw in latency_results:
                for metric in ("throughput", "p50_ms", "p95_ms", "p99_ms"):
                    if metric in result:
                        latency_results[fw][st][metric].append(result[metric])
                latency_writer.writerow([
                    fw,
                    st,
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
        metadata_file.close()

    print_table(frameworks, table_sleep_times, throughput_results, args.runs)
    latency_fws = [fw for fw in frameworks if _is_latency(fw)]
    if latency_fws:
        latency_sts = sorted({st for fw in latency_fws for st in fw_sleep_times[fw]})
        print_latency_table(latency_fws, latency_sts, latency_results, args.runs)
    print(f"\nThroughput results saved to {throughput_csv_path}")
    print(f"Run metadata saved to {metadata_path}")
    if latency_fws:
        print(f"Latency results saved to {latency_csv_path}")
    suggest_message_counts(frameworks, table_sleep_times, throughput_results)


if __name__ == "__main__":
    main()
