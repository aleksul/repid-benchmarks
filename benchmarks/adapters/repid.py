from __future__ import annotations

import asyncio
import ctypes
import json
import time
from multiprocessing import Process, Value
from multiprocessing.sharedctypes import Synchronized
from pathlib import Path

import uvloop
from repid import AmqpServer, Repid, Router

from benchmarks._processes import counter_incr_mmap
from benchmarks._publishing import publish_async, publish_async_at_rate, publish_async_bursts
from benchmarks._runtime import BenchmarkConfig, load_config
from benchmarks._work import cpu_work, record_latency_to_file

config = load_config()
COUNTER_KIND = "mmap" if config.task_kind == "cpu" else "value"
WORKER_KIND = "multiprocessing"
server = AmqpServer(dsn=config.amqp_url)
app = Repid()
app.servers.register_server("default", server, is_default=True)
router = Router(channel=config.queue_name)
_counter: Synchronized = Value(ctypes.c_long, 0)


def _increment_counter() -> None:
    if COUNTER_KIND == "mmap":
        counter_incr_mmap(config.counter_path)
        return
    with _counter.get_lock():
        _counter.value += 1


if config.is_latency:

    @router.actor
    async def benchmark_task(enqueue_time: float | None) -> None:
        await asyncio.sleep(config.sleep_time)
        if enqueue_time is not None:
            record_latency_to_file(config.latency_path, time.perf_counter() - enqueue_time)
        _increment_counter()

elif config.task_kind == "cpu":

    @router.actor(run_in_process=True)
    def benchmark_task() -> None:
        cpu_work(config.cpu_work_iterations)
        _increment_counter()

else:

    @router.actor
    async def benchmark_task() -> None:
        await asyncio.sleep(config.sleep_time)
        _increment_counter()


app.include_router(router)


async def _publish_one() -> None:
    if config.is_latency:
        payload = json.dumps({"enqueue_time": time.perf_counter()}).encode()
    else:
        payload = b""
    await app.send_message(channel=config.queue_name, payload=payload, headers={"topic": "benchmark_task"})


async def _publish_one_latency(record: bool) -> None:
    payload = json.dumps({"enqueue_time": time.perf_counter() if record else None}).encode()
    await app.send_message(channel=config.queue_name, payload=payload, headers={"topic": "benchmark_task"})


async def _publish_all(cfg: BenchmarkConfig) -> None:
    async with server.connection():
        await publish_async(cfg.messages, cfg.publish_concurrency, _publish_one)


async def _publish_bursts(cfg: BenchmarkConfig) -> None:
    async with server.connection():
        await publish_async_bursts(cfg.messages, cfg.burst_size, cfg.burst_interval, cfg.publish_concurrency, _publish_one)


def publish(cfg: BenchmarkConfig) -> None:
    uvloop.run(_publish_bursts(cfg) if cfg.mode == "burst" else _publish_all(cfg))


def publish_latency(cfg: BenchmarkConfig, messages: int, rate_per_second: float, record: bool) -> None:
    async def _run() -> None:
        async with server.connection():
            await publish_async_at_rate(messages, rate_per_second, lambda: _publish_one_latency(record))

    uvloop.run(_run())


async def _run(counter: Synchronized | None) -> None:
    global _counter
    if counter is not None:
        _counter = counter
    async with server.connection():
        await app.run_worker(graceful_shutdown_time=0, tasks_limit=config.prefetch_count or config.concurrency)


def _worker_process(counter: Synchronized | None) -> None:
    uvloop.run(_run(counter))


def start_workers(cfg: BenchmarkConfig, config_path: Path, counter: Synchronized | None = None) -> list[Process]:
    if COUNTER_KIND == "value":
        assert counter is not None
    processes = [Process(target=_worker_process, args=(counter,)) for _ in range(cfg.processes)]
    for process in processes:
        process.start()
    return processes
