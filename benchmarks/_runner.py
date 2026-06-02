"""Generic benchmark runner used by run_all.py."""

from __future__ import annotations

import argparse
import ctypes
import importlib
import os
import subprocess
import sys
import tempfile
import threading
import time
from dataclasses import replace
from multiprocessing import Value
from pathlib import Path

from benchmarks._processes import (
    create_counter_file,
    mmap_counter,
    terminate_multiprocessing,
    terminate_processes,
)
from benchmarks._rabbitmq import delete_queue, purge_queue, reset_queue, wait_for_consumers
from benchmarks._runtime import CONFIG_ENV, BenchmarkConfig, load_config, write_config
from benchmarks._work import (
    init_latency_file,
    print_latency_results,
    print_results,
    read_latencies_from_file,
    report_mmap,
    report_value,
)


def _auxiliary_queue_names(cfg: BenchmarkConfig) -> list[str]:
    if cfg.framework == "dramatiq":
        return [f"{cfg.queue_name}.DQ", f"{cfg.queue_name}.XQ"]
    if cfg.framework == "taskiq":
        # taskiq.dead_letter is the previous global default; remove it if present.
        return [f"{cfg.queue_name}.dead_letter", "taskiq.dead_letter"]
    return []


def _run_queue_names(cfg: BenchmarkConfig) -> list[str]:
    return [cfg.queue_name, *_auxiliary_queue_names(cfg)]


def _delete_run_queues(cfg: BenchmarkConfig) -> None:
    for queue_name in _run_queue_names(cfg):
        delete_queue(cfg.amqp_url, cfg.rabbitmq_mgmt_url, queue_name)


def _purge_run_queues(cfg: BenchmarkConfig) -> None:
    for queue_name in _run_queue_names(cfg):
        purge_queue(cfg.amqp_url, cfg.rabbitmq_mgmt_url, queue_name)


def _split_messages(messages: int, processes: int) -> list[int]:
    if messages <= 0:
        return []
    count = max(1, min(processes, messages))
    per_process = messages // count
    remainder = messages % count
    return [per_process + (1 if i < remainder else 0) for i in range(count)]


def _per_publisher(value: int, processes: int) -> int:
    return max(1, (value + processes - 1) // processes)


def _publisher_config(cfg: BenchmarkConfig, messages: int, processes: int) -> BenchmarkConfig:
    burst_size = _per_publisher(cfg.burst_size, processes) if cfg.mode == "burst" else cfg.burst_size
    return replace(
        cfg,
        messages=messages,
        publish_processes=1,
        publish_concurrency=_per_publisher(cfg.publish_concurrency, processes),
        publish_workers=_per_publisher(cfg.publish_workers, processes),
        burst_size=burst_size,
    )


def _parse_enqueued_count(line: str) -> int | None:
    if not line.startswith("Enqueued:"):
        return None
    raw_count = line.removeprefix("Enqueued:").strip().split("/", 1)[0]
    try:
        return int(raw_count)
    except ValueError:
        return None


def _parse_burst_count(line: str) -> int | None:
    marker = ": published "
    if not line.startswith("Burst ") or marker not in line:
        return None
    raw_count = line.split(marker, 1)[1].split(" ", 1)[0]
    try:
        return int(raw_count)
    except ValueError:
        return None


def _publish(adapter: object, cfg: BenchmarkConfig) -> None:
    chunks = _split_messages(cfg.messages, cfg.publish_processes)
    if len(chunks) <= 1:
        adapter.publish(_publisher_config(cfg, chunks[0] if chunks else 0, 1))  # type: ignore[attr-defined]
        return

    root = Path(__file__).parents[1]
    config_paths: list[Path] = []
    stderr_files: list[object] = []
    procs: list[subprocess.Popen] = []
    stdout_threads: list[threading.Thread] = []
    progress_lock = threading.Lock()
    progress = [0] * len(chunks)
    completed_bursts = [0] * len(chunks)
    current_burst_counts = [0] * len(chunks)

    def _read_stdout(index: int, stdout: object) -> None:
        for raw in stdout:  # type: ignore[union-attr]
            line = str(raw).rstrip("\r\n")
            count = _parse_enqueued_count(line)
            burst_count = _parse_burst_count(line) if count is None else None
            if count is None and burst_count is None:
                continue
            with progress_lock:
                if burst_count is None:
                    current_burst_counts[index] = count if count is not None else 0
                else:
                    completed_bursts[index] += burst_count
                    current_burst_counts[index] = 0
                progress[index] = min(completed_bursts[index] + current_burst_counts[index], chunks[index])

    print(f"Starting {len(chunks)} publisher processes.")
    try:
        for index, messages in enumerate(chunks):
            chunk_cfg = _publisher_config(cfg, messages, len(chunks))
            config_path = write_config(chunk_cfg)
            config_paths.append(config_path)
            env = os.environ.copy()
            env[CONFIG_ENV] = str(config_path)
            stderr_file = tempfile.TemporaryFile("w+t")
            stderr_files.append(stderr_file)
            proc = subprocess.Popen(
                [sys.executable, "-u", "-m", "benchmarks._publisher", "--config", str(config_path)],
                cwd=root,
                env=env,
                stdout=subprocess.PIPE,
                stderr=stderr_file,
                text=True,
                start_new_session=True,
            )
            procs.append(proc)
            assert proc.stdout is not None
            thread = threading.Thread(target=_read_stdout, args=(index, proc.stdout), daemon=True)
            thread.start()
            stdout_threads.append(thread)

        last_done = -1
        last_enqueued = -1
        while True:
            done = sum(proc.poll() is not None for proc in procs)
            with progress_lock:
                enqueued = sum(progress)
            if done != last_done or enqueued != last_enqueued:
                print(f"Enqueued: {enqueued}/{cfg.messages} (publishers done {done}/{len(procs)})", end="\r", flush=True)
                last_done = done
                last_enqueued = enqueued
            if done == len(procs):
                break
            time.sleep(0.1)

        for thread in stdout_threads:
            thread.join(timeout=5)

        errors: list[str] = []
        for idx, proc in enumerate(procs):
            if proc.returncode == 0:
                continue
            stderr_file = stderr_files[idx]
            stderr_file.seek(0)  # type: ignore[attr-defined]
            stderr = stderr_file.read()  # type: ignore[attr-defined]
            errors.append(f"publisher {idx + 1} exited with {proc.returncode}:\n{stderr[-4000:]}")
        if errors:
            raise RuntimeError(f"Publishing failed in {len(errors)} publisher process(es). First error:\n{errors[0]}")
        print(f"Enqueued: {cfg.messages}/{cfg.messages}", end="\r", flush=True)
    finally:
        alive = [proc for proc in procs if proc.poll() is None]
        if alive:
            terminate_processes(alive)
        for stderr_file in stderr_files:
            stderr_file.close()  # type: ignore[attr-defined]
        for path in config_paths:
            try:
                path.unlink()
            except FileNotFoundError:
                pass


def _consumer_target(cfg: BenchmarkConfig) -> int:
    if cfg.framework in {"celery", "dramatiq"} and not cfg.green_threads:
        return 1
    return cfg.processes


def _needs_counter_file(cfg: BenchmarkConfig) -> bool:
    return cfg.framework in {"celery", "dramatiq", "taskiq"} or (
        cfg.framework == "repid" and cfg.task_kind == "cpu"
    )


def _start_workers(adapter: object, cfg: BenchmarkConfig, config_path: Path, counter: object | None) -> list[object]:
    return adapter.start_workers(cfg, config_path, counter)  # type: ignore[attr-defined]


def _stop_workers(adapter: object, workers: list[object]) -> None:
    if not workers:
        return
    if (
        getattr(adapter, "WORKER_KIND", None) == "multiprocessing"
        or getattr(adapter, "COUNTER_KIND") == "value"
    ):
        terminate_multiprocessing(workers)  # type: ignore[arg-type]
    else:
        terminate_processes(workers)  # type: ignore[arg-type]


def _run_value_counter(cfg: BenchmarkConfig, adapter: object, config_path: Path) -> tuple[int, bool, float, float]:
    counter = Value(ctypes.c_long, 0)
    workers: list[object] = []
    publish_first = cfg.mode in {"base", "hc", "cpu", "nogt"}

    if publish_first:
        print("Enqueueing messages...")
        _publish(adapter, cfg)
        print("\nDone enqueueing.")

    print("Starting workers.")
    workers = _start_workers(adapter, cfg, config_path, counter)
    try:
        if not publish_first:
            wait_for_consumers(cfg.amqp_url, cfg.rabbitmq_mgmt_url, cfg.queue_name, _consumer_target(cfg))
            print("Publishing messages while workers are running...")
            start = time.perf_counter()
            _publish(adapter, cfg)
        else:
            print("Starting benchmark.")
            start = time.perf_counter()
        tasks_done, timed_out, _first_task_time = report_value(start, counter, cfg.messages, cfg.time_limit)
        end = time.perf_counter()
    finally:
        _stop_workers(adapter, workers)
    return tasks_done, timed_out, start, end


def _run_mmap_counter(cfg: BenchmarkConfig, adapter: object, config_path: Path) -> tuple[int, bool, float, float]:
    counter_path = cfg.counter_path
    if counter_path is None:
        raise RuntimeError("counter_path is required for mmap benchmarks")
    workers: list[object] = []
    publish_first = cfg.mode in {"base", "hc", "cpu", "nogt"}

    if publish_first:
        print("Enqueueing messages...")
        _publish(adapter, cfg)
        print("\nDone enqueueing.")

    with mmap_counter(counter_path) as mm:
        print("Starting workers.")
        workers = _start_workers(adapter, cfg, config_path, None)
        try:
            if not publish_first:
                wait_for_consumers(cfg.amqp_url, cfg.rabbitmq_mgmt_url, cfg.queue_name, _consumer_target(cfg))
                print("Publishing messages while workers are running...")
                start = time.perf_counter()
                _publish(adapter, cfg)
            else:
                print("Starting benchmark.")
                start = time.perf_counter()
            tasks_done, timed_out, _first_task_time = report_mmap(start, mm, cfg.messages, cfg.time_limit)
            end = time.perf_counter()
        finally:
            _stop_workers(adapter, workers)
    return tasks_done, timed_out, start, end


def run(config_path: Path) -> None:
    cfg = load_config(config_path)
    if cfg.framework == "repid":
        reset_queue(cfg.amqp_url, cfg.rabbitmq_mgmt_url, cfg.queue_name)
    else:
        _delete_run_queues(cfg)

    cleanup_paths: list[str] = []
    if _needs_counter_file(cfg):
        cfg.counter_path = create_counter_file()
        cleanup_paths.append(cfg.counter_path)
    if cfg.is_latency:
        fd, latency_path = tempfile.mkstemp(prefix="repid-bench-", suffix=".latency")
        os.close(fd)
        cfg.latency_path = latency_path
        init_latency_file(latency_path, cfg.messages)
        cleanup_paths.append(latency_path)

    write_config(cfg, config_path)
    os.environ[CONFIG_ENV] = str(config_path)
    adapter = importlib.import_module(f"benchmarks.adapters.{cfg.framework}")

    try:
        if getattr(adapter, "COUNTER_KIND") == "value":
            tasks_done, timed_out, start, end = _run_value_counter(cfg, adapter, config_path)
        else:
            tasks_done, timed_out, start, end = _run_mmap_counter(cfg, adapter, config_path)

        duration = end - start
        status = "timeout" if timed_out else "ok"
        if timed_out:
            _purge_run_queues(cfg)
        print_results(tasks_done, cfg.messages, duration, status)
        if cfg.is_latency and cfg.latency_path:
            print_latency_results(read_latencies_from_file(cfg.latency_path))
    finally:
        if cfg.keep_queue:
            _purge_run_queues(cfg)
        else:
            _delete_run_queues(cfg)
        for path in cleanup_paths:
            try:
                os.unlink(path)
            except FileNotFoundError:
                pass


def main() -> None:
    parser = argparse.ArgumentParser(description="Run one configured benchmark")
    parser.add_argument("--config", type=Path, default=None)
    args = parser.parse_args()
    config_path = args.config or Path(os.environ[CONFIG_ENV])
    run(config_path)


if __name__ == "__main__":
    main()
