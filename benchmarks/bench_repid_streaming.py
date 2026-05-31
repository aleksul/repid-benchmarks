"""Repid streaming benchmark — workers start first, then messages are published.

Measures throughput from first publish to last completion, exposing publish
overhead and broker round-trip costs not visible in the pre-enqueue benchmark.
"""

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
    declare_queue,
    print_results,
    purge_queue,
    report_value,
)
from repid import AmqpServer, Repid, Router

PROCESSES = 8
PUBLISH_CONCURRENCY = 10000
TASKS_LIMIT = int(os.getenv("CONCURRENCY_LIMIT", "2000"))

CHANNEL = "repid_streaming_bench"

server = AmqpServer(dsn=AMQP_URL)
app = Repid()
app.servers.register_server("default", server, is_default=True)
r = Router(channel=CHANNEL)

_counter: Synchronized = Value(ctypes.c_long, 0)


@r.actor
async def benchmark_task() -> None:
    await asyncio.sleep(SLEEP_TIME)
    with _counter.get_lock():
        _counter.value += 1


app.include_router(r)


async def _publish() -> None:
    """Publish all messages without purging (queue is already clean)."""
    async with server.connection():
        sem = asyncio.Semaphore(PUBLISH_CONCURRENCY)
        pending: set[asyncio.Task] = set()

        async def _send() -> None:
            await app.send_message(
                channel=CHANNEL,
                payload=b"",
                headers={"topic": "benchmark_task"},
            )
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
    async with server.connection():
        await app.run_worker(graceful_shutdown_time=0, tasks_limit=TASKS_LIMIT)


def _worker_process(counter: Synchronized) -> None:
    uvloop.run(run(counter))


if __name__ == "__main__":
    # Pre-declare queue as durable so aio-pika workers don't hit transient_nonexcl_queues error.
    declare_queue(CHANNEL)
    # Purge queue before starting workers to avoid stale messages.
    purge_queue(CHANNEL)

    counter: Synchronized = Value(ctypes.c_long, 0)
    processes: list[Process] = [
        Process(target=_worker_process, args=(counter,)) for _ in range(PROCESSES)
    ]

    print("Starting workers first...")
    for process in processes:
        process.start()

    # Give workers time to connect to the broker before we start publishing.
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
        purge_queue(CHANNEL)

    print_results(tasks_done, duration)
