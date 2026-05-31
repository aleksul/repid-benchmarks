"""FastStream burst benchmark — messages published in periodic bursts."""

from __future__ import annotations

import asyncio
import ctypes
import os
import threading
import time
from multiprocessing import Process, Value
from multiprocessing.sharedctypes import Synchronized
from time import perf_counter

import uvloop
from _common import (
    AMQP_URL,
    BURST_INTERVAL,
    BURST_SIZE,
    MESSAGES_AMOUNT,
    SLEEP_TIME,
    print_results,
    purge_queue,
    report_value,
)
from faststream import FastStream
from faststream.rabbit import Channel, RabbitBroker, RabbitQueue

PROCESSES = 8
PUBLISH_CONCURRENCY = 10000
MAX_WORKERS = int(os.getenv("CONCURRENCY_LIMIT", "2000"))

QUEUE = "fs_burst_bench"

broker = RabbitBroker(AMQP_URL)
app = FastStream(broker)

_counter: Synchronized = Value(ctypes.c_long, 0)


@broker.subscriber(RabbitQueue(QUEUE, durable=True), channel=Channel(prefetch_count=MAX_WORKERS))
async def benchmark_task() -> None:
    await asyncio.sleep(SLEEP_TIME)
    with _counter.get_lock():
        _counter.value += 1


async def _publish_bursts() -> None:
    async with RabbitBroker(AMQP_URL) as pub_broker:
        await pub_broker.declare_queue(RabbitQueue(QUEUE, durable=True))
        remaining = MESSAGES_AMOUNT
        burst_num = 0

        while remaining > 0:
            n = min(BURST_SIZE, remaining)
            sem = asyncio.Semaphore(PUBLISH_CONCURRENCY)
            pending: set[asyncio.Task] = set()

            async def _send() -> None:
                await pub_broker.publish(b"", queue=QUEUE)
                sem.release()

            for _ in range(n):
                await sem.acquire()
                t = asyncio.create_task(_send())
                pending.add(t)
                t.add_done_callback(pending.discard)

            if pending:
                await asyncio.gather(*pending)

            remaining -= n
            burst_num += 1
            print(
                f"Burst {burst_num}: published {n} messages ({remaining} remaining)",
                flush=True,
            )

            if remaining > 0:
                await asyncio.sleep(BURST_INTERVAL)


async def run(counter: Synchronized) -> None:
    global _counter
    _counter = counter
    await app.run()


def _worker_process(counter: Synchronized) -> None:
    uvloop.run(run(counter))


if __name__ == "__main__":
    purge_queue(QUEUE)

    counter: Synchronized = Value(ctypes.c_long, 0)
    processes: list[Process] = [
        Process(target=_worker_process, args=(counter,)) for _ in range(PROCESSES)
    ]

    print("Starting workers first...")
    for process in processes:
        process.start()

    time.sleep(2.0)

    print(f"Publishing in bursts of {BURST_SIZE} every {BURST_INTERVAL}s...")
    pub_start = perf_counter()
    pub_thread = threading.Thread(
        target=lambda: asyncio.run(_publish_bursts()),
        daemon=True,
    )
    pub_thread.start()

    try:
        tasks_done, timed_out, _ = report_value(pub_start, counter)
    finally:
        for process in processes:
            process.terminate()
        for process in processes:
            process.join()

    end = perf_counter()
    duration = end - pub_start

    pub_thread.join(timeout=60)

    if timed_out:
        purge_queue(QUEUE)

    print_results(tasks_done, duration)
