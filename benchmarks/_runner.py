"""Generic benchmark runner used by run_all.py."""

from __future__ import annotations

import argparse
import ctypes
import importlib
import mmap
import os
import struct
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
from benchmarks._rabbitmq import delete_queue, purge_queue, queue_metrics, reset_queue, wait_for_consumers
from benchmarks._runtime import CONFIG_ENV, BenchmarkConfig, load_config, write_config
from benchmarks._work import (
    _milestone_thresholds,
    _print_milestones,
    _record_milestones,
    init_latency_file,
    print_latency_results,
    read_latency_count,
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


def _cleanup_warning(action: str, queue_name: str, exc: Exception) -> None:
    detail = str(exc) or exc.__class__.__name__
    print(f"[WARN] Could not {action} RabbitMQ queue {queue_name!r}: {detail}", file=sys.stderr, flush=True)


def _delete_run_queues(cfg: BenchmarkConfig, *, best_effort: bool = False) -> None:
    for queue_name in _run_queue_names(cfg):
        try:
            delete_queue(cfg.amqp_url, cfg.rabbitmq_mgmt_url, queue_name)
        except Exception as exc:
            if not best_effort:
                raise
            _cleanup_warning("delete", queue_name, exc)


def _purge_run_queues(cfg: BenchmarkConfig, *, best_effort: bool = False) -> None:
    for queue_name in _run_queue_names(cfg):
        try:
            purge_queue(cfg.amqp_url, cfg.rabbitmq_mgmt_url, queue_name)
        except Exception as exc:
            if not best_effort:
                raise
            _cleanup_warning("purge", queue_name, exc)


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


def _start_publishers(cfg: BenchmarkConfig) -> tuple[list[subprocess.Popen], list[Path], list[object]]:
    chunks = _split_messages(cfg.messages, cfg.publish_processes)
    root = Path(__file__).parents[1]
    config_paths: list[Path] = []
    stderr_files: list[object] = []
    procs: list[subprocess.Popen] = []

    for messages in chunks:
        chunk_cfg = _publisher_config(cfg, messages, len(chunks))
        config_path = write_config(chunk_cfg)
        config_paths.append(config_path)
        env = os.environ.copy()
        env[CONFIG_ENV] = str(config_path)
        stderr_file = tempfile.TemporaryFile("w+t")
        stderr_files.append(stderr_file)
        procs.append(
            subprocess.Popen(
                [sys.executable, "-u", "-m", "benchmarks._publisher", "--config", str(config_path)],
                cwd=root,
                env=env,
                stdout=subprocess.DEVNULL,
                stderr=stderr_file,
                text=True,
                start_new_session=True,
            )
        )
    return procs, config_paths, stderr_files


def _cleanup_publishers(procs: list[subprocess.Popen], config_paths: list[Path], stderr_files: list[object]) -> None:
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


def _read_mmap_count(mm: mmap.mmap) -> int:
    mm.seek(0)
    return struct.unpack("q", mm.read(8))[0]


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


def _print_steady_results(
    measured_tasks: int,
    total_count: int,
    published_limit: int,
    warmup_seconds: float,
    measurement_seconds: float,
    status: str,
) -> None:
    throughput = measured_tasks / max(measurement_seconds, 0.000001)
    duration = warmup_seconds + measurement_seconds
    print(
        "",
        "Steady-state benchmark ended.",
        f"Warmup {warmup_seconds:.2f} sec; measured {measurement_seconds:.2f} sec.",
        f"Measured {measured_tasks} tasks; total completed {total_count}/{published_limit}.",
        f"Rate {throughput:.2f} msg/sec.",
        sep="\n",
    )
    print(f"STATUS: {status}")
    print(f"TASKS_DONE: {measured_tasks}")
    print(f"TOTAL_MESSAGES: {published_limit}")
    print(f"DURATION_SECONDS: {duration:.4f}")
    print(f"THROUGHPUT: {throughput:.2f}")


def _print_latency_controlled_results(
    samples: int,
    measurement_messages: int,
    offered_rate: float,
    achieved_rate: float,
    backlog_at_measurement_end: int,
    drain_seconds: float,
    duration: float,
    status: str,
) -> None:
    print(
        "",
        "Controlled-arrival latency benchmark ended.",
        f"Offered rate {offered_rate:.2f} msg/sec.",
        f"Recorded {samples}/{measurement_messages} measured latency samples.",
        f"Backlog at measurement end: {backlog_at_measurement_end}.",
        f"Post-measurement drain: {drain_seconds:.2f} sec.",
        sep="\n",
    )
    print(f"STATUS: {status}")
    print(f"TASKS_DONE: {samples}")
    print(f"TOTAL_MESSAGES: {measurement_messages}")
    print(f"DURATION_SECONDS: {duration:.4f}")
    print(f"THROUGHPUT: {achieved_rate:.2f}")
    print(f"LATENCY_OFFERED_RATE: {offered_rate:.4f}")
    print(f"LATENCY_ACHIEVED_RATE: {achieved_rate:.4f}")
    print(f"LATENCY_BACKLOG_AT_MEASUREMENT_END: {backlog_at_measurement_end}")
    print(f"LATENCY_DRAIN_SECONDS: {drain_seconds:.4f}")


def _run_latency_controlled(cfg: BenchmarkConfig, adapter: object, config_path: Path, counter: object | None) -> None:
    if not cfg.latency_path:
        raise RuntimeError("latency_path is required for latency benchmarks")

    warmup_messages = max(0, round(cfg.latency_arrival_rate * cfg.latency_warmup_seconds))
    measurement_messages = max(1, round(cfg.latency_arrival_rate * cfg.latency_measurement_seconds))
    workers: list[object] = []
    start = time.perf_counter()
    measurement_end = start
    end = start
    status = "ok"

    try:
        print("Starting workers.")
        workers = _start_workers(adapter, cfg, config_path, counter)
        wait_for_consumers(cfg.amqp_url, cfg.rabbitmq_mgmt_url, cfg.queue_name, _consumer_target(cfg))
        print(f"Publishing latency warmup at {cfg.latency_arrival_rate:g} msg/sec.")
        if warmup_messages:
            adapter.publish_latency(cfg, warmup_messages, cfg.latency_arrival_rate, False)  # type: ignore[attr-defined]
            print("\nDone latency warmup.")

        print(f"Publishing measured latency traffic at {cfg.latency_arrival_rate:g} msg/sec.")
        measure_start = time.perf_counter()
        adapter.publish_latency(cfg, measurement_messages, cfg.latency_arrival_rate, True)  # type: ignore[attr-defined]
        measurement_end = time.perf_counter()
        samples_at_measurement_end = read_latency_count(cfg.latency_path)
        backlog_at_measurement_end = max(0, measurement_messages - samples_at_measurement_end)
        deadline = measurement_end + cfg.time_limit

        while True:
            samples = read_latency_count(cfg.latency_path)
            if samples >= measurement_messages:
                end = time.perf_counter()
                break
            now = time.perf_counter()
            if now >= deadline:
                status = "timeout"
                end = now
                _purge_run_queues(cfg, best_effort=True)
                break
            print(f"Latency samples: {samples}/{measurement_messages}. Draining measured traffic.", end="\r", flush=True)
            time.sleep(0.05)

        print(" " * 80, end="\r", flush=True)
        samples = read_latency_count(cfg.latency_path)
        drain_seconds = max(0.0, end - measurement_end)
        measured_duration = max(end - measure_start, 0.000001)
        achieved_rate = samples / measured_duration
        _print_latency_controlled_results(
            samples,
            measurement_messages,
            cfg.latency_arrival_rate,
            achieved_rate,
            backlog_at_measurement_end,
            drain_seconds,
            end - start,
            status,
        )
        print_latency_results(read_latencies_from_file(cfg.latency_path))
    finally:
        _stop_workers(adapter, workers)


def _print_burst_recovery_results(
    tasks_done: int,
    total: int,
    publish_seconds: float,
    recovery_seconds: float,
    status: str,
) -> None:
    duration = max(publish_seconds + recovery_seconds, 0.000001)
    throughput = tasks_done / duration
    print(
        "",
        "Burst recovery benchmark ended.",
        f"Published burst in {publish_seconds:.2f} sec.",
        f"Recovered in {recovery_seconds:.2f} sec after burst publish completed.",
        f"Processed {tasks_done}/{total} tasks.",
        f"End-to-end rate {throughput:.2f} msg/sec.",
        sep="\n",
    )
    print(f"STATUS: {status}")
    print(f"TASKS_DONE: {tasks_done}")
    print(f"TOTAL_MESSAGES: {total}")
    print(f"DURATION_SECONDS: {duration:.4f}")
    print(f"THROUGHPUT: {throughput:.2f}")
    print(f"BURST_PUBLISH_SECONDS: {publish_seconds:.4f}")
    print(f"BURST_RECOVERY_SECONDS: {recovery_seconds:.4f}")


def _print_burst_queue_metrics(label: str, cfg: BenchmarkConfig) -> None:
    try:
        metrics = queue_metrics(cfg.amqp_url, cfg.rabbitmq_mgmt_url, cfg.queue_name)
    except Exception as exc:
        print(f"BURST_QUEUE_{label}_ERROR: {exc}")
        return
    print(
        f"BURST_QUEUE_{label}: "
        f"ready={metrics['ready']} "
        f"unacknowledged={metrics['unacknowledged']} "
        f"total={metrics['total']} "
        f"consumers={metrics['consumers']}"
    )


def _run_burst_recovery(cfg: BenchmarkConfig, adapter: object, config_path: Path, counter: object | None) -> tuple[int, bool, float, float, float]:
    workers: list[object] = []
    if getattr(adapter, "COUNTER_KIND") == "value":
        assert counter is not None
        read_count = lambda: counter.value  # type: ignore[union-attr]
    else:
        counter_path = cfg.counter_path
        if counter_path is None:
            raise RuntimeError("counter_path is required for mmap benchmarks")
        mm_ctx = mmap_counter(counter_path)
        mm = mm_ctx.__enter__()
        read_count = lambda: _read_mmap_count(mm)

    try:
        effective_slots = cfg.prefetch_count or cfg.concurrency
        messages = max(1, round(cfg.processes * effective_slots * cfg.burst_multiplier))
        cfg.messages = messages
        write_config(cfg, config_path)
        print("Starting workers.")
        workers = _start_workers(adapter, cfg, config_path, counter)
        wait_for_consumers(cfg.amqp_url, cfg.rabbitmq_mgmt_url, cfg.queue_name, _consumer_target(cfg))

        print(f"Publishing one burst of {messages} messages.")
        publish_start = time.perf_counter()
        burst_cfg = replace(cfg, burst_size=messages, burst_interval=0.0)
        _publish(adapter, burst_cfg)
        publish_end = time.perf_counter()
        _print_burst_queue_metrics("PUBLISH_END", cfg)

        thresholds = _milestone_thresholds(messages)
        milestones: dict[str, float] = {}
        queue_snapshots: set[str] = set()
        timed_out = False
        while True:
            count = read_count()
            _record_milestones(count, publish_end, thresholds, milestones)
            for label, threshold in (("90", 0.90), ("99", 0.99), ("995", 0.995)):
                if label not in queue_snapshots and count >= round(messages * threshold):
                    _print_burst_queue_metrics(label, cfg)
                    queue_snapshots.add(label)
            if count >= messages:
                end = time.perf_counter()
                break
            elapsed = time.perf_counter() - publish_end
            if elapsed >= cfg.time_limit:
                timed_out = True
                end = time.perf_counter()
                _purge_run_queues(cfg, best_effort=True)
                break
            print(f"Tasks done: {count}/{messages}. Recovery: {elapsed:.2f}s.", end="\r", flush=True)
            time.sleep(0.05)
        print(" " * 80, end="\r", flush=True)
        _print_burst_queue_metrics("100", cfg)
        _print_milestones(milestones)
        return min(read_count(), messages), timed_out, publish_start, publish_end, end
    finally:
        _stop_workers(adapter, workers)
        if getattr(adapter, "COUNTER_KIND") != "value":
            mm_ctx.__exit__(None, None, None)  # type: ignore[possibly-undefined]


def _run_steady(
    cfg: BenchmarkConfig,
    adapter: object,
    config_path: Path,
    read_count: object,
    counter: object | None,
) -> None:
    workers: list[object] = []
    publisher_procs: list[subprocess.Popen] = []
    publisher_config_paths: list[Path] = []
    publisher_stderr_files: list[object] = []
    try:
        print("Starting workers.")
        workers = _start_workers(adapter, cfg, config_path, counter)
        wait_for_consumers(cfg.amqp_url, cfg.rabbitmq_mgmt_url, cfg.queue_name, _consumer_target(cfg))
        print("Starting steady-state publishers.")
        start = time.perf_counter()
        publisher_procs, publisher_config_paths, publisher_stderr_files = _start_publishers(cfg)

        warmup_end = start + cfg.steady_warmup_seconds
        measurement_end = warmup_end + cfg.steady_measurement_seconds
        warmup_count: int | None = None
        status = "ok"
        last_count = -1

        while True:
            now = time.perf_counter()
            count = read_count()  # type: ignore[operator]
            if warmup_count is None and now >= warmup_end:
                warmup_count = count
            if count != last_count:
                phase = "warmup" if warmup_count is None else "measure"
                elapsed = now - start
                print(f"Tasks done: {count}/{cfg.messages}. Phase: {phase}. Elapsed: {elapsed:.2f}s.", end="\r", flush=True)
                last_count = count
            if now >= measurement_end:
                end_count = count
                break
            if count >= cfg.messages:
                status = "exhausted"
                end_count = count
                if warmup_count is None:
                    warmup_count = count
                break
            if any(proc.poll() not in (None, 0) for proc in publisher_procs):
                status = "error"
                end_count = count
                if warmup_count is None:
                    warmup_count = count
                break
            time.sleep(0.05)

        if warmup_count is None:
            warmup_count = end_count
        measured_tasks = max(0, end_count - warmup_count)
        print(" " * 80, end="\r", flush=True)
        _print_steady_results(
            measured_tasks,
            end_count,
            cfg.messages,
            cfg.steady_warmup_seconds,
            cfg.steady_measurement_seconds,
            status,
        )
    finally:
        _cleanup_publishers(publisher_procs, publisher_config_paths, publisher_stderr_files)
        _stop_workers(adapter, workers)


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
        warmup_messages = max(0, round(cfg.latency_arrival_rate * cfg.latency_warmup_seconds))
        measurement_messages = max(1, round(cfg.latency_arrival_rate * cfg.latency_measurement_seconds))
        cfg.messages = warmup_messages + measurement_messages
        fd, latency_path = tempfile.mkstemp(prefix="repid-bench-", suffix=".latency")
        os.close(fd)
        cfg.latency_path = latency_path
        init_latency_file(latency_path, measurement_messages)
        cleanup_paths.append(latency_path)

    write_config(cfg, config_path)
    os.environ[CONFIG_ENV] = str(config_path)
    adapter = importlib.import_module(f"benchmarks.adapters.{cfg.framework}")

    try:
        if cfg.mode == "steady":
            if getattr(adapter, "COUNTER_KIND") == "value":
                counter = Value(ctypes.c_long, 0)
                _run_steady(cfg, adapter, config_path, lambda: counter.value, counter)
            else:
                counter_path = cfg.counter_path
                if counter_path is None:
                    raise RuntimeError("counter_path is required for mmap benchmarks")
                with mmap_counter(counter_path) as mm:
                    _run_steady(cfg, adapter, config_path, lambda: _read_mmap_count(mm), None)
            return

        if cfg.mode == "latency":
            counter = Value(ctypes.c_long, 0) if getattr(adapter, "COUNTER_KIND") == "value" else None
            _run_latency_controlled(cfg, adapter, config_path, counter)
            return

        if cfg.mode == "burst":
            if getattr(adapter, "COUNTER_KIND") == "value":
                counter = Value(ctypes.c_long, 0)
                tasks_done, timed_out, publish_start, publish_end, end = _run_burst_recovery(cfg, adapter, config_path, counter)
            else:
                tasks_done, timed_out, publish_start, publish_end, end = _run_burst_recovery(cfg, adapter, config_path, None)
            status = "timeout" if timed_out else "ok"
            _print_burst_recovery_results(tasks_done, cfg.messages, publish_end - publish_start, end - publish_end, status)
            return

        if getattr(adapter, "COUNTER_KIND") == "value":
            tasks_done, timed_out, start, end = _run_value_counter(cfg, adapter, config_path)
        else:
            tasks_done, timed_out, start, end = _run_mmap_counter(cfg, adapter, config_path)

        duration = end - start
        status = "timeout" if timed_out else "ok"
        if timed_out:
            _purge_run_queues(cfg, best_effort=True)
        print_results(tasks_done, cfg.messages, duration, status)
        if cfg.is_latency and cfg.latency_path:
            print_latency_results(read_latencies_from_file(cfg.latency_path))
    finally:
        if cfg.keep_queue:
            _purge_run_queues(cfg, best_effort=True)
        else:
            _delete_run_queues(cfg, best_effort=True)
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
