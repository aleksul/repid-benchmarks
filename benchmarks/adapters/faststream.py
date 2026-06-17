from __future__ import annotations

import asyncio
import ctypes
import os
import time
from multiprocessing import Process, Value
from multiprocessing.sharedctypes import Synchronized
from pathlib import Path

import uvloop
from faststream import FastStream
from faststream.rabbit import Channel, RabbitBroker, RabbitQueue
from pydantic import BaseModel

from benchmarks._publishing import publish_async, publish_async_at_rate, publish_async_bursts
from benchmarks._runtime import BenchmarkConfig, load_config
from benchmarks._work import cpu_work, record_latency_to_file

COUNTER_KIND = "value"
config = load_config()
broker = RabbitBroker(config.amqp_url)
app = FastStream(broker)
_counter: Synchronized = Value(ctypes.c_long, 0)
prefetch_count = config.prefetch_count or config.concurrency


class TaskPayload(BaseModel):
    enqueue_time: float | None


if config.is_latency:

    @broker.subscriber(RabbitQueue(config.queue_name, durable=True), channel=Channel(prefetch_count=prefetch_count))
    async def benchmark_task(body: TaskPayload) -> None:
        await asyncio.sleep(config.sleep_time)
        if body.enqueue_time is not None:
            record_latency_to_file(config.latency_path, time.perf_counter() - body.enqueue_time)
        with _counter.get_lock():
            _counter.value += 1

elif config.task_kind == "cpu":

    @broker.subscriber(RabbitQueue(config.queue_name, durable=True), channel=Channel(prefetch_count=prefetch_count))
    def benchmark_task() -> None:
        cpu_work(config.cpu_work_iterations)
        with _counter.get_lock():
            _counter.value += 1

else:

    @broker.subscriber(RabbitQueue(config.queue_name, durable=True), channel=Channel(prefetch_count=prefetch_count))
    async def benchmark_task() -> None:
        await asyncio.sleep(config.sleep_time)
        with _counter.get_lock():
            _counter.value += 1


async def _publish_one(pub_broker: RabbitBroker) -> None:
    payload: object = {"enqueue_time": time.perf_counter()} if config.is_latency else b""
    await pub_broker.publish(payload, queue=config.queue_name)


async def _publish_one_latency(pub_broker: RabbitBroker, record: bool) -> None:
    await pub_broker.publish({"enqueue_time": time.perf_counter() if record else None}, queue=config.queue_name)


async def _publish_all(cfg: BenchmarkConfig) -> None:
    async with RabbitBroker(cfg.amqp_url) as pub_broker:
        await pub_broker.declare_queue(RabbitQueue(cfg.queue_name, durable=True))
        await publish_async(cfg.messages, cfg.publish_concurrency, lambda: _publish_one(pub_broker))


async def _publish_bursts(cfg: BenchmarkConfig) -> None:
    async with RabbitBroker(cfg.amqp_url) as pub_broker:
        await pub_broker.declare_queue(RabbitQueue(cfg.queue_name, durable=True))
        await publish_async_bursts(cfg.messages, cfg.burst_size, cfg.burst_interval, cfg.publish_concurrency, lambda: _publish_one(pub_broker))


def publish(cfg: BenchmarkConfig) -> None:
    uvloop.run(_publish_bursts(cfg) if cfg.mode == "burst" else _publish_all(cfg))


def publish_latency(cfg: BenchmarkConfig, messages: int, rate_per_second: float, record: bool) -> None:
    async def _run() -> None:
        async with RabbitBroker(cfg.amqp_url) as pub_broker:
            await pub_broker.declare_queue(RabbitQueue(cfg.queue_name, durable=True))
            await publish_async_at_rate(messages, rate_per_second, lambda: _publish_one_latency(pub_broker, record))

    uvloop.run(_run())


async def _run(counter: Synchronized) -> None:
    global _counter
    _counter = counter
    await app.run()


def _redirect_worker_output(log_dir: str | None, name: str) -> None:
    if log_dir:
        Path(log_dir).mkdir(parents=True, exist_ok=True)
        output_path = Path(log_dir) / f"{name}.log"
        fd = os.open(output_path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
    else:
        fd = os.open(os.devnull, os.O_WRONLY)
    try:
        os.dup2(fd, 1)
        os.dup2(fd, 2)
    finally:
        os.close(fd)


def _worker_process(counter: Synchronized, log_dir: str | None, name: str) -> None:
    _redirect_worker_output(log_dir, name)
    uvloop.run(_run(counter))


def start_workers(cfg: BenchmarkConfig, config_path: Path, counter: Synchronized | None = None) -> list[Process]:
    assert counter is not None
    processes = [
        Process(target=_worker_process, args=(counter, cfg.worker_log_dir, f"{cfg.name}-faststream-{i}"))
        for i in range(cfg.processes)
    ]
    for process in processes:
        process.start()
    return processes
