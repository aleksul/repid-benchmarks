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

from benchmarks._publishing import publish_async, publish_async_bursts
from benchmarks._runtime import BenchmarkConfig, load_config
from benchmarks._work import cpu_work, record_latency_to_file

COUNTER_KIND = "value"
config = load_config()
server = AmqpServer(dsn=config.amqp_url)
app = Repid()
app.servers.register_server("default", server, is_default=True)
router = Router(channel=config.queue_name)
_counter: Synchronized = Value(ctypes.c_long, 0)


if config.is_latency:

    @router.actor
    async def benchmark_task(enqueue_time: float) -> None:
        await asyncio.sleep(config.sleep_time)
        record_latency_to_file(config.latency_path, time.perf_counter() - enqueue_time)
        with _counter.get_lock():
            _counter.value += 1

elif config.task_kind == "cpu":

    @router.actor(run_in_process=False)
    def benchmark_task() -> None:
        cpu_work(config.cpu_work_iterations)
        with _counter.get_lock():
            _counter.value += 1

else:

    @router.actor
    async def benchmark_task() -> None:
        await asyncio.sleep(config.sleep_time)
        with _counter.get_lock():
            _counter.value += 1


app.include_router(router)


async def _publish_one() -> None:
    if config.is_latency:
        payload = json.dumps({"enqueue_time": time.perf_counter()}).encode()
    else:
        payload = b""
    await app.send_message(channel=config.queue_name, payload=payload, headers={"topic": "benchmark_task"})


async def _publish_all(cfg: BenchmarkConfig) -> None:
    async with server.connection():
        await publish_async(cfg.messages, cfg.publish_concurrency, _publish_one)


async def _publish_bursts(cfg: BenchmarkConfig) -> None:
    async with server.connection():
        await publish_async_bursts(cfg.messages, cfg.burst_size, cfg.burst_interval, cfg.publish_concurrency, _publish_one)


def publish(cfg: BenchmarkConfig) -> None:
    uvloop.run(_publish_bursts(cfg) if cfg.mode == "burst" else _publish_all(cfg))


async def _run(counter: Synchronized) -> None:
    global _counter
    _counter = counter
    async with server.connection():
        await app.run_worker(graceful_shutdown_time=0, tasks_limit=config.concurrency)


def _worker_process(counter: Synchronized) -> None:
    uvloop.run(_run(counter))


def start_workers(cfg: BenchmarkConfig, config_path: Path, counter: Synchronized | None = None) -> list[Process]:
    assert counter is not None
    processes = [Process(target=_worker_process, args=(counter,)) for _ in range(cfg.processes)]
    for process in processes:
        process.start()
    return processes
