"""Repid burst benchmark — messages published in periodic bursts.

Workers run continuously. Messages arrive in batches of BURST_SIZE every
BURST_INTERVAL seconds, testing queue drain speed and idle-to-active transition.
Throughput is measured from first publish to last completion.
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
    BURST_INTERVAL,
    BURST_SIZE,
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

CHANNEL = "repid_burst_bench"

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


async def _publish_bursts() -> None:
    """Publish MESSAGES_AMOUNT messages in bursts of BURST_SIZE."""
    async with server.connection():
        remaining = MESSAGES_AMOUNT
        burst_num = 0

        while remaining > 0:
            n = min(BURST_SIZE, remaining)
            sem = asyncio.Semaphore(PUBLISH_CONCURRENCY)
            pending: set[asyncio.Task] = set()

            async def _send() -> None:
                await app.send_message(
                    channel=CHANNEL,
                    payload=b"",
                    headers={"topic": "benchmark_task"},
                )
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
    async with server.connection():
        await app.run_worker(graceful_shutdown_time=0, tasks_limit=TASKS_LIMIT)


def _worker_process(counter: Synchronized) -> None:
    uvloop.run(run(counter))


if __name__ == "__main__":
    declare_queue(CHANNEL)
    purge_queue(CHANNEL)

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

    if timed_out:
        purge_queue(CHANNEL)

    print_results(tasks_done, duration)
