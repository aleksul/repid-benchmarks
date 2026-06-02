from __future__ import annotations

import asyncio
import subprocess
import time
from pathlib import Path

import uvloop
from taskiq_aio_pika import AioPikaBroker
from taskiq_aio_pika.queue import Queue as TQQueue, QueueType

from benchmarks._processes import counter_incr_mmap, subprocess_kwargs
from benchmarks._publishing import publish_async, publish_async_bursts
from benchmarks._runtime import BenchmarkConfig, load_config
from benchmarks._work import cpu_work, record_latency_to_file

COUNTER_KIND = "mmap"
config = load_config()
broker = AioPikaBroker(
    config.amqp_url,
    qos=config.concurrency,
    task_queues=[TQQueue(name=config.queue_name, durable=True, type=QueueType.CLASSIC)],
)


@broker.task(task_name="benchmark_task")
async def benchmark_task(enqueue_time: float | None = None) -> None:
    if config.task_kind == "cpu":
        cpu_work(config.cpu_work_iterations)
    else:
        await asyncio.sleep(config.sleep_time)
    if enqueue_time is not None:
        record_latency_to_file(config.latency_path, time.perf_counter() - enqueue_time)
    counter_incr_mmap(config.counter_path)


async def _send() -> None:
    if config.is_latency:
        await benchmark_task.kiq(time.perf_counter())
    else:
        await benchmark_task.kiq()


async def _publish_all(cfg: BenchmarkConfig) -> None:
    await broker.startup()
    try:
        await publish_async(cfg.messages, cfg.publish_concurrency, _send)
    finally:
        await broker.shutdown()


async def _publish_bursts(cfg: BenchmarkConfig) -> None:
    await broker.startup()
    try:
        await publish_async_bursts(cfg.messages, cfg.burst_size, cfg.burst_interval, cfg.publish_concurrency, _send)
    finally:
        await broker.shutdown()


def publish(cfg: BenchmarkConfig) -> None:
    uvloop.run(_publish_bursts(cfg) if cfg.mode == "burst" else _publish_all(cfg))


def start_workers(cfg: BenchmarkConfig, config_path: Path, counter: object | None = None) -> list[subprocess.Popen]:
    args = [
        "taskiq",
        "worker",
        "benchmarks.adapters.taskiq:broker",
        "--workers",
        "1",
        "--log-level",
        "WARNING",
        "--max-async-tasks",
        str(cfg.concurrency),
        "--max-prefetch",
        str(cfg.concurrency),
        "--ack-type",
        "when_executed",
    ]
    return [
        subprocess.Popen(args, **subprocess_kwargs(config_path, cfg.worker_log_dir, f"{cfg.name}-taskiq-{i}"))
        for i in range(cfg.processes)
    ]
