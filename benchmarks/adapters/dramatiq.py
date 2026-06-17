from __future__ import annotations

import subprocess
import time
from pathlib import Path

import dramatiq
from dramatiq.brokers.rabbitmq import RabbitmqBroker

from benchmarks._processes import counter_incr_mmap, subprocess_kwargs
from benchmarks._publishing import publish_sync_at_rate, publish_threaded_bursts, publish_threaded_chunks
from benchmarks._runtime import BenchmarkConfig, load_config
from benchmarks._work import cpu_work, record_latency_to_file

COUNTER_KIND = "mmap"
WORKER_SHUTDOWN_TIMEOUT_MS = 1000
config = load_config()
broker = RabbitmqBroker(url=config.amqp_url)
dramatiq.set_broker(broker)


@dramatiq.actor(queue_name=config.queue_name)
def benchmark_task(enqueue_time: float | None = None) -> None:
    if config.task_kind == "cpu":
        cpu_work(config.cpu_work_iterations)
    else:
        time.sleep(config.sleep_time)
    if enqueue_time is not None:
        record_latency_to_file(config.latency_path, time.perf_counter() - enqueue_time)
    counter_incr_mmap(config.counter_path)


def _send() -> None:
    msg = benchmark_task.message(time.perf_counter()) if config.is_latency else benchmark_task.message()
    broker.enqueue(msg)


def _send_latency(record: bool) -> None:
    broker.enqueue(benchmark_task.message(time.perf_counter() if record else None))


def _publish_chunk(count: int, progress: object) -> None:
    sent = 0
    for i in range(1, count + 1):
        _send()
        if i % config.enqueue_batch_size == 0:
            sent += config.enqueue_batch_size
            progress(config.enqueue_batch_size)  # type: ignore[operator]
    leftover = count - sent
    if leftover:
        progress(leftover)  # type: ignore[operator]


def publish(cfg: BenchmarkConfig) -> None:
    if cfg.mode == "burst":
        publish_threaded_bursts(cfg.messages, cfg.burst_size, cfg.burst_interval, cfg.publish_workers, cfg.enqueue_batch_size, _send)
    else:
        publish_threaded_chunks(cfg.messages, cfg.publish_workers, cfg.enqueue_batch_size, _publish_chunk)


def publish_latency(cfg: BenchmarkConfig, messages: int, rate_per_second: float, record: bool) -> None:
    publish_sync_at_rate(messages, rate_per_second, lambda: _send_latency(record))


def start_workers(cfg: BenchmarkConfig, config_path: Path, counter: object | None = None) -> list[subprocess.Popen]:
    shutdown_args = ["--worker-shutdown-timeout", str(WORKER_SHUTDOWN_TIMEOUT_MS)]
    kwargs = subprocess_kwargs(config_path, cfg.worker_log_dir, f"{cfg.name}-dramatiq")
    if cfg.prefetch_count is not None:
        kwargs["env"]["dramatiq_queue_prefetch"] = str(cfg.prefetch_count)  # type: ignore[index]
    if cfg.green_threads:
        args = [
            "dramatiq-gevent",
            "benchmarks.adapters.dramatiq",
            "-p",
            str(cfg.processes),
            "-t",
            str(cfg.concurrency),
            *shutdown_args,
        ]
    else:
        args = [
            "dramatiq",
            "benchmarks.adapters.dramatiq",
            "-p",
            str(cfg.processes),
            "-t",
            "1",
            *shutdown_args,
        ]
    return [subprocess.Popen(args, **kwargs)]
