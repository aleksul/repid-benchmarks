"""FastStream latency benchmark — tracks per-message end-to-end latency (p50/p95/p99).

Message body is a dict containing the enqueue timestamp.
Workers record completion latency to a shared mmap file.
"""

from __future__ import annotations

import asyncio
import ctypes
import os
import tempfile
import time
from multiprocessing import Process, Value
from multiprocessing.sharedctypes import Synchronized
from time import perf_counter

import uvloop
from _common import (
    AMQP_URL,
    MESSAGES_AMOUNT,
    SLEEP_TIME,
    init_latency_file,
    print_latency_results,
    print_results,
    purge_queue,
    read_latencies_from_file,
    record_latency_to_file,
    report_value,
)
from faststream import FastStream
from faststream.rabbit import Channel, RabbitBroker, RabbitQueue
from pydantic import BaseModel

PROCESSES = 8
PUBLISH_CONCURRENCY = 10000
MAX_WORKERS = int(os.getenv("CONCURRENCY_LIMIT", "2000"))

QUEUE = "fs_latency_bench"

broker = RabbitBroker(AMQP_URL)
app = FastStream(broker)

_counter: Synchronized = Value(ctypes.c_long, 0)


class TaskPayload(BaseModel):
    enqueue_time: float


@broker.subscriber(RabbitQueue(QUEUE, durable=True), channel=Channel(prefetch_count=MAX_WORKERS))
async def benchmark_task(body: TaskPayload) -> None:
    await asyncio.sleep(SLEEP_TIME)
    latency = time.time() - body.enqueue_time
    latency_path = os.getenv("LATENCY_PATH", "")
    if latency_path:
        record_latency_to_file(latency_path, latency)
    with _counter.get_lock():
        _counter.value += 1


async def prepare() -> None:
    purge_queue(QUEUE)
    async with RabbitBroker(AMQP_URL) as pub_broker:
        await pub_broker.declare_queue(RabbitQueue(QUEUE, durable=True))
        sem = asyncio.Semaphore(PUBLISH_CONCURRENCY)
        pending: set[asyncio.Task] = set()

        async def _send() -> None:
            await pub_broker.publish(
                {"enqueue_time": time.time()},
                queue=QUEUE,
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
    await app.run()


def _worker_process(counter: Synchronized) -> None:
    uvloop.run(run(counter))


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    with tempfile.NamedTemporaryFile(delete=False, suffix=".latency") as tf:
        latency_path = tf.name
    init_latency_file(latency_path, MESSAGES_AMOUNT)
    os.environ["LATENCY_PATH"] = latency_path

    print("Enqueueing messages...")
    loop.run_until_complete(prepare())
    print("\nDone enqueueing.")

    counter: Synchronized = Value(ctypes.c_long, 0)
    processes: list[Process] = [
        Process(target=_worker_process, args=(counter,)) for _ in range(PROCESSES)
    ]

    print("Starting benchmark.")
    start = perf_counter()

    for process in processes:
        process.start()

    try:
        tasks_done, timed_out, first_message_time = report_value(start, counter)
    finally:
        for process in processes:
            process.terminate()
        for process in processes:
            process.join()

    end = perf_counter()
    duration = end - first_message_time

    if timed_out:
        purge_queue(QUEUE)

    print_results(tasks_done, duration)
    latencies = read_latencies_from_file(latency_path)
    os.unlink(latency_path)
    print_latency_results(latencies)
