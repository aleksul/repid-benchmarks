#!/usr/bin/env python3
"""Benchmark orchestrator.

All benchmark variants are defined in ``benchmarks._specs`` and executed through
``benchmarks._runner``. Direct per-variant benchmark entry points are not part of
the public workflow anymore.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import random
import re
import signal
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path
from statistics import mean, stdev
import platform

from benchmarks._rabbitmq import management_url_from_amqp
from benchmarks._runtime import BenchmarkConfig, write_config
from benchmarks._specs import ALL_BENCHMARKS, DEFAULT_TARGET_DURATION, SPECS, get_spec
from benchmarks._work import calibrate_cpu_work_iterations

ROOT = Path(__file__).parent
DEFAULT_RUNS = 5
DEFAULT_TIME_LIMIT = 300.0
DEFAULT_BURST_SIZE = 5000
DEFAULT_BURST_INTERVAL = 1.0

THROUGHPUT_CSV_HEADER = ["framework", "sleep_time", "run", "status", "throughput_msg_per_sec", "tasks_done", "total_messages", "duration_seconds"]
LATENCY_CSV_HEADER = ["framework", "sleep_time", "run", "status", "throughput_msg_per_sec", "p50_ms", "p95_ms", "p99_ms", "latency_samples", "duration_seconds"]


def _is_latency(name: str) -> bool:
    return get_spec(name).mode == "latency"


def _is_cpu(name: str) -> bool:
    return get_spec(name).task_kind == "cpu"


def config_hash(config: BenchmarkConfig) -> str:
    raw = json.dumps({
        "framework": config.framework,
        "mode": config.mode,
        "task_kind": config.task_kind,
        "processes": config.processes,
        "concurrency": config.concurrency,
        "publish_concurrency": config.publish_concurrency,
        "publish_workers": config.publish_workers,
        "green_threads": config.green_threads,
    }, sort_keys=True)
    return hashlib.sha256(raw.encode()).hexdigest()[:12]


def fmt_cell(values: list[float]) -> str:
    if not values:
        return "FAILED"
    avg = mean(values)
    return f"{avg:.1f}±{stdev(values):.1f}" if len(values) > 1 else f"{avg:.1f}"


def parse_runner_output(stdout: str) -> dict[str, float | int | str] | None:
    m = re.search(r"^STATUS:\s*(\S+)", stdout, re.MULTILINE)
    if not m:
        return None
    result: dict[str, float | int | str] = {"status": m.group(1)}
    m_thr = re.search(r"^THROUGHPUT:\s*([\d.]+)", stdout, re.MULTILINE)
    if m_thr:
        result["throughput"] = float(m_thr.group(1))
    m_td = re.search(r"^TASKS_DONE:\s*(\d+)", stdout, re.MULTILINE)
    if m_td:
        result["tasks_done"] = int(m_td.group(1))
    m_tm = re.search(r"^TOTAL_MESSAGES:\s*(\d+)", stdout, re.MULTILINE)
    if m_tm:
        result["total_messages"] = int(m_tm.group(1))
    m_dur = re.search(r"^DURATION_SECONDS:\s*([\d.]+)", stdout, re.MULTILINE)
    if m_dur:
        result["duration_seconds"] = float(m_dur.group(1))
    for tag, key in (("LATENCY_P50", "p50_ms"), ("LATENCY_P95", "p95_ms"), ("LATENCY_P99", "p99_ms")):
        m_lat = re.search(rf"^{tag}:\s*([\d.]+)", stdout, re.MULTILINE)
        if m_lat:
            result[key] = float(m_lat.group(1))
    m_ls = re.search(r"^LATENCY_SAMPLES:\s*(\d+)", stdout, re.MULTILINE)
    if m_ls:
        result["latency_samples"] = int(m_ls.group(1))
    return result


def load_counts_file(path: Path) -> dict[str, dict[float, int]]:
    with path.open() as f:
        raw = json.load(f)
    return {
        framework: {float(sleep_time): int(messages) for sleep_time, messages in by_sleep.items()}
        for framework, by_sleep in raw.items()
    }


def run_once(config: BenchmarkConfig) -> dict[str, float | int | str] | None:
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
            start_new_session=True,
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
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except (ProcessLookupError, OSError):
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
        print(f"\n[TIMEOUT] {config.name} @ sleep={config.sleep_time} (killed process group)")
        return {"status": "timeout"}

    if proc.returncode != 0:
        print(f"\n[ERROR] {config.name} @ sleep={config.sleep_time} exited with code {proc.returncode}")
        if stderr_data[0]:
            print(stderr_data[0][-4000:])
        return {"status": "error"}

    stdout = "\n".join(stdout_lines)
    parsed = parse_runner_output(stdout)
    if parsed is None:
        print(f"\n[WARN] No STATUS line found for {config.name} @ sleep={config.sleep_time}")
        return {"status": "error"}
    return parsed


def calibration_messages(sleep_time: float) -> int:
    if sleep_time <= 0.1:
        return 500
    elif sleep_time <= 1.0:
        return 100
    else:
        return 30


def calibrate_message_counts(
    frameworks: list[str],
    sleep_times_by_fw: dict[str, list[float]],
    target_duration: float,
    amqp_url: str = "amqp://user:testtest@localhost:5672",
    mgmt_url: str | None = None,
) -> dict[str, dict[float, int]]:
    if mgmt_url is None:
        mgmt_url = management_url_from_amqp(amqp_url)
    counts: dict[str, dict[float, int]] = {}
    for fw in frameworks:
        spec = get_spec(fw)
        counts[fw] = {}
        for st in sleep_times_by_fw.get(fw, []):
            cal_messages = calibration_messages(st)
            queue_name = f"cal_{spec.framework}_{uuid.uuid4().hex[:6]}"
            cfg = BenchmarkConfig(
                name=fw,
                framework=spec.framework,
                mode=spec.mode,
                task_kind=spec.task_kind,
                queue_name=queue_name,
                messages=cal_messages,
                sleep_time=st,
                time_limit=max(60, cal_messages * st * 2),
                amqp_url=amqp_url,
                rabbitmq_mgmt_url=mgmt_url,
                processes=spec.processes,
                concurrency=spec.concurrency,
                publish_concurrency=spec.publish_concurrency,
                publish_workers=spec.publish_workers,
                enqueue_batch_size=spec.enqueue_batch_size,
                green_threads=spec.green_threads,
                burst_size=DEFAULT_BURST_SIZE,
                burst_interval=DEFAULT_BURST_INTERVAL,
            )
            print(f"  Calibrating {fw} @ sleep={st} with {cal_messages} messages...")
            result = run_once(cfg)
            if result and result.get("status") == "ok" and "throughput" in result:
                throughput = result["throughput"]
                recommended = max(500, round(throughput * target_duration / 500) * 500)
                counts[fw][st] = recommended
                print(f"    -> {throughput:.0f} msg/s, recommending {recommended:,} messages")
            else:
                fallback = (spec.messages or {}).get(st)
                if fallback:
                    counts[fw][st] = fallback
                    print(f"    -> calibration failed, using spec default {fallback:,}")
                else:
                    print(f"    -> calibration failed, no fallback available for sleep={st}")
    return counts


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


def suggest_message_counts(frameworks: list[str], sleep_times: list[float], results: dict[str, dict[float, list[float]]], target_seconds: float = DEFAULT_TARGET_DURATION) -> None:
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
    parser.add_argument("--randomize-order", action="store_true", default=True, help="Randomize run order (default: True)")
    parser.add_argument("--no-randomize", action="store_true", help="Disable run order randomization")
    parser.add_argument("--keep-queues", action="store_true")
    parser.add_argument("--worker-log-dir", default=None)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--calibrate", action="store_true", help="Run calibration to determine message counts")
    parser.add_argument("--target-duration", type=float, default=DEFAULT_TARGET_DURATION, metavar="SECONDS")
    parser.add_argument("--counts-file", type=Path, default=None, metavar="FILE")
    parser.add_argument("--seed", type=int, default=None, metavar="N", help="Random seed for run order")
    args = parser.parse_args()

    randomize = not args.no_randomize
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

    calibrated_counts: dict[str, dict[float, int]] | None = None
    if args.counts_file and args.counts_file.exists():
        calibrated_counts = load_counts_file(args.counts_file)
        print(f"Loaded calibrated message counts from {args.counts_file}")

    elif args.calibrate:
        all_sleep_times = {fw: sleep_times_for(fw) for fw in frameworks}
        print("\n=== Calibration Phase ===\n")
        calibrated_counts = calibrate_message_counts(
            frameworks, all_sleep_times,
            target_duration=args.target_duration,
            amqp_url=args.amqp_url, mgmt_url=mgmt_url,
        )
        print("\n=== Calibration Results ===\n")
        for fw in frameworks:
            for st in sorted(calibrated_counts.get(fw, {})):
                print(f"  {fw} @ sleep={st}: {calibrated_counts[fw][st]:,} messages")
        if args.counts_file:
            with args.counts_file.open("w") as f:
                json.dump(calibrated_counts, f, indent=2)
            print(f"\nCalibrated counts saved to {args.counts_file}")

    def messages_for(name: str, sleep_time: float) -> int | None:
        if name in fw_overrides:
            return fw_overrides[name]
        if args.messages is not None:
            return args.messages
        if calibrated_counts and name in calibrated_counts and sleep_time in calibrated_counts[name]:
            return calibrated_counts[name][sleep_time]
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
    run_status: dict[tuple[str, float, int], str] = {}

    throughput_csv_path = ROOT / "benchmarks_results.csv"
    latency_csv_path = ROOT / "latency_results.csv"
    metadata_path = ROOT / "benchmarks_metadata.jsonl"

    skip_set: set[tuple[str, float, int]] = set()
    if args.resume and throughput_csv_path.exists():
        with throughput_csv_path.open(newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                fw = row["framework"]
                st = float(row["sleep_time"])
                run_num = int(row["run"])
                status = row.get("status", "ok")
                if status == "ok" and row.get("throughput_msg_per_sec"):
                    skip_set.add((fw, st, run_num))
                    if fw in throughput_results and st in throughput_results[fw]:
                        throughput_results[fw][st].append(float(row["throughput_msg_per_sec"]))

    write_throughput_header = not args.resume or not throughput_csv_path.exists()
    write_latency_header = not args.resume or not latency_csv_path.exists()
    throughput_file = throughput_csv_path.open("a" if args.resume else "w", newline="")
    latency_file = latency_csv_path.open("a" if (args.resume and latency_csv_path.exists()) else "w", newline="")
    metadata_file = metadata_path.open("a" if args.resume else "w")
    throughput_writer = csv.writer(throughput_file)
    latency_writer = csv.writer(latency_file)
    if write_throughput_header:
        throughput_writer.writerow(THROUGHPUT_CSV_HEADER)
    if write_latency_header:
        latency_writer.writerow(LATENCY_CSV_HEADER)

    tasks: list[tuple[str, float, int, bool]] = []

    for fw in frameworks:
        for st in fw_sleep_times[fw]:
            if messages_for(fw, st) is None:
                print(f"[SKIP] {fw:<18} sleep={st:<5} (no message count)")
                continue
            for run_idx in range(1, args.warmup_runs + 1):
                tasks.append((fw, st, run_idx, True))
            run_num_start = 1
            for run_idx in range(run_num_start, args.runs + 1):
                if (fw, st, run_idx) in skip_set:
                    continue
                tasks.append((fw, st, run_idx, False))

    if randomize:
        seed = args.seed if args.seed is not None else random.randint(0, 2**32)
        print(f"[SEED] Randomizing run order with seed={seed}")
        warmup_tasks = [(fw, st, ri, True) for fw, st, ri, warmup in tasks if warmup]
        measured_tasks = [(fw, st, ri, False) for fw, st, ri, warmup in tasks if not warmup]
        rng = random.Random(seed)
        rng.shuffle(measured_tasks)
        tasks = warmup_tasks + measured_tasks

    try:
        for idx, (fw, st, run_idx, warmup) in enumerate(tasks, 1):
            spec = get_spec(fw)
            messages = messages_for(fw, st)
            assert messages is not None
            queue_name = f"{spec.queue_base}_{uuid.uuid4().hex[:10]}"
            cfg = BenchmarkConfig(
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
            result = run_once(cfg)
            if result is None:
                result = {"status": "error"}
            status = str(result.get("status", "error"))
            run_status[(fw, st, run_idx)] = status
            throughput = result.get("throughput")
            if throughput is not None:
                suffix = f"  -> {throughput:.1f} msg/s [{status}]"
            else:
                suffix = f"  -> [{status}]"
            labels = []
            for key, unit in (("p50_ms", "ms"), ("p95_ms", "ms"), ("p99_ms", "ms"), ("duration_seconds", "s")):
                if key in result and result[key] is not None:
                    labels.append(f"{key}={result[key]:.1f}{unit}")
            if labels:
                suffix += "  " + "  ".join(labels)
            print(f"{'':>{len(label)}}{suffix}")
            if warmup:
                continue
            if throughput is not None and status == "ok":
                throughput_results[fw][st].append(throughput)
            metadata_file.write(json.dumps({
                "benchmark": fw,
                "framework": spec.framework,
                "mode": spec.mode,
                "task_kind": spec.task_kind,
                "sleep_time": st,
                "messages": messages,
                "run": run_idx,
                "status": status,
                "amqp_host": cfg.amqp_url.split("@")[-1].split("/")[0],
                "processes": cfg.processes,
                "concurrency": cfg.concurrency,
                "publish_concurrency": cfg.publish_concurrency,
                "publish_workers": cfg.publish_workers,
                "green_threads": cfg.green_threads,
                "python": platform.python_version(),
                "timestamp": time.time(),
                "config_hash": config_hash(cfg),
            }) + "\n")
            metadata_file.flush()
            tasks_done = result.get("tasks_done", "")
            total_messages = result.get("total_messages", messages)
            duration_seconds = result.get("duration_seconds", "")
            throughput_writer.writerow([fw, st, run_idx, status, f"{throughput:.2f}" if throughput is not None else "", tasks_done, total_messages, f"{duration_seconds:.4f}" if isinstance(duration_seconds, (int, float)) else ""])
            throughput_file.flush()
            if _is_latency(fw) and fw in latency_results and status == "ok":
                for metric in ("throughput", "p50_ms", "p95_ms", "p99_ms"):
                    if metric in result and result[metric] is not None:
                        val = float(result[metric])
                        if metric == "throughput" or val > 0:
                            latency_results[fw][st][metric].append(val)
                p50 = result.get("p50_ms", 0)
                p95 = result.get("p95_ms", 0)
                p99 = result.get("p99_ms", 0)
                lat_samples = result.get("latency_samples", 0)
                lat_duration = result.get("duration_seconds", "")
                latency_writer.writerow([
                    fw, st, run_idx, status,
                    f"{throughput:.2f}" if throughput is not None else "",
                    f"{p50:.3f}" if p50 else "0",
                    f"{p95:.3f}" if p95 else "0",
                    f"{p99:.3f}" if p99 else "0",
                    lat_samples if lat_samples else "0",
                    f"{lat_duration:.4f}" if isinstance(lat_duration, (int, float)) else "",
                ])
                latency_file.flush()
            elif _is_latency(fw) and status != "ok":
                latency_writer.writerow([
                    fw, st, run_idx, status,
                    f"{throughput:.2f}" if throughput is not None else "",
                    "", "", "", "", "",
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

    failed = sum(1 for s in run_status.values() if s != "ok")
    if failed:
        print(f"\n[WARN] {failed} run(s) had non-ok status (timeout/error)")
    suggest_message_counts(frameworks, table_sleep_times, throughput_results, args.target_duration)


if __name__ == "__main__":
    main()
