"""Generic benchmark runner used by run_all.py."""

from __future__ import annotations

import argparse
import ctypes
import importlib
import os
import tempfile
import time
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


def _consumer_target(cfg: BenchmarkConfig) -> int:
    if cfg.framework in {"celery", "dramatiq"} and not cfg.green_threads:
        return 1
    return cfg.processes


def _start_workers(adapter: object, cfg: BenchmarkConfig, config_path: Path, counter: object | None) -> list[object]:
    return adapter.start_workers(cfg, config_path, counter)  # type: ignore[attr-defined]


def _stop_workers(adapter: object, workers: list[object]) -> None:
    if not workers:
        return
    if getattr(adapter, "COUNTER_KIND") == "value":
        terminate_multiprocessing(workers)  # type: ignore[arg-type]
    else:
        terminate_processes(workers)  # type: ignore[arg-type]


def _run_value_counter(cfg: BenchmarkConfig, adapter: object, config_path: Path) -> tuple[int, bool, float, float]:
    counter = Value(ctypes.c_long, 0)
    workers: list[object] = []
    publish_first = cfg.mode in {"base", "hc", "cpu", "nogt"}

    if publish_first:
        print("Enqueueing messages...")
        adapter.publish(cfg)  # type: ignore[attr-defined]
        print("\nDone enqueueing.")

    print("Starting workers.")
    workers = _start_workers(adapter, cfg, config_path, counter)
    try:
        if not publish_first:
            wait_for_consumers(cfg.amqp_url, cfg.rabbitmq_mgmt_url, cfg.queue_name, _consumer_target(cfg))
            print("Publishing messages while workers are running...")
            start = time.perf_counter()
            adapter.publish(cfg)  # type: ignore[attr-defined]
        else:
            print("Starting benchmark.")
            start = time.perf_counter()
        tasks_done, timed_out, first_message_time = report_value(start, counter, cfg.messages, cfg.time_limit)
    finally:
        _stop_workers(adapter, workers)
    end = time.perf_counter()
    duration_start = start if cfg.mode in {"streaming", "burst", "latency"} else first_message_time
    return tasks_done, timed_out, duration_start, end


def _run_mmap_counter(cfg: BenchmarkConfig, adapter: object, config_path: Path) -> tuple[int, bool, float, float]:
    counter_path = cfg.counter_path
    if counter_path is None:
        raise RuntimeError("counter_path is required for mmap benchmarks")
    workers: list[object] = []
    publish_first = cfg.mode in {"base", "hc", "cpu", "nogt"}

    if publish_first:
        print("Enqueueing messages...")
        adapter.publish(cfg)  # type: ignore[attr-defined]
        print("\nDone enqueueing.")

    with mmap_counter(counter_path) as mm:
        print("Starting workers.")
        workers = _start_workers(adapter, cfg, config_path, None)
        try:
            if not publish_first:
                wait_for_consumers(cfg.amqp_url, cfg.rabbitmq_mgmt_url, cfg.queue_name, _consumer_target(cfg))
                print("Publishing messages while workers are running...")
                start = time.perf_counter()
                adapter.publish(cfg)  # type: ignore[attr-defined]
            else:
                print("Starting benchmark.")
                start = time.perf_counter()
            tasks_done, timed_out, first_message_time = report_mmap(start, mm, cfg.messages, cfg.time_limit)
        finally:
            _stop_workers(adapter, workers)
    end = time.perf_counter()
    duration_start = start if cfg.mode in {"streaming", "burst", "latency"} else first_message_time
    return tasks_done, timed_out, duration_start, end


def run(config_path: Path) -> None:
    cfg = load_config(config_path)
    if cfg.framework == "repid":
        reset_queue(cfg.amqp_url, cfg.rabbitmq_mgmt_url, cfg.queue_name)
    else:
        delete_queue(cfg.amqp_url, cfg.rabbitmq_mgmt_url, cfg.queue_name)

    cleanup_paths: list[str] = []
    if cfg.framework in {"celery", "dramatiq", "taskiq"}:
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
            tasks_done, timed_out, duration_start, end = _run_value_counter(cfg, adapter, config_path)
        else:
            tasks_done, timed_out, duration_start, end = _run_mmap_counter(cfg, adapter, config_path)

        if timed_out:
            purge_queue(cfg.amqp_url, cfg.rabbitmq_mgmt_url, cfg.queue_name)
        print_results(tasks_done, end - duration_start)
        if cfg.is_latency and cfg.latency_path:
            print_latency_results(read_latencies_from_file(cfg.latency_path))
    finally:
        if cfg.keep_queue:
            purge_queue(cfg.amqp_url, cfg.rabbitmq_mgmt_url, cfg.queue_name)
        else:
            delete_queue(cfg.amqp_url, cfg.rabbitmq_mgmt_url, cfg.queue_name)
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
