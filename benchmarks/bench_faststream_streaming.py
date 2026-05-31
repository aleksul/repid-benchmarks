"""FastStream streaming benchmark — workers start first, then messages are published."""

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

QUEUE = "fs_streaming_bench"

broker = RabbitBroker(AMQP_URL)
app = FastStream(broker)

_counter: Synchronized = Value(ctypes.c_long, 0)


@broker.subscriber(RabbitQueue(QUEUE, durable=True), channel=Channel(prefetch_count=MAX_WORKERS))
async def benchmark_task() -> None:
    await asyncio.sleep(SLEEP_TIME)
    with _counter.get_lock():
        _counter.value += 1


async def _publish() -> None:
    """Publish all messages without purging (queue is already clean)."""
    async with RabbitBroker(AMQP_URL) as pub_broker:
        await pub_broker.declare_queue(RabbitQueue(QUEUE, durable=True))
        sem = asyncio.Semaphore(PUBLISH_CONCURRENCY)
        pending: set[asyncio.Task] = set()

        async def _send() -> None:
            await pub_broker.publish(b"", queue=QUEUE)
            sem.release()

        for i in range(MESSAGES_AMOUNT):
            await sem.acquire()
            t = asyncio.create_task(_send())
            pending.add(t)
            t.add_done_callback(pending.discard)
            if (i + 1) % 2000 == 0:
                print(f"Enqueued: {i + 1}/{MESSAGES_AMOUNT}", end="\r", flush=True)

        if pending:
            await asyncio.gather(*pending)
        print(f"Enqueued: {MESSAGES_AMOUNT}/{MESSAGES_AMOUNT}", end="\r", flush=True)


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

    print("Publishing messages while workers are running...")
    pub_start = perf_counter()
    pub_thread = threading.Thread(
        target=lambda: asyncio.run(_publish()),
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
