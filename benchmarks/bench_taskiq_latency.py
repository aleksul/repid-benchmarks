"""Taskiq latency benchmark — tracks per-message end-to-end latency (p50/p95/p99).

The enqueue timestamp is passed as a task argument.
Workers record completion latency to a shared mmap file.
"""

from __future__ import annotations

import asyncio
import mmap
import os
import subprocess
import tempfile
import time
from time import perf_counter

from _common import (
    AMQP_URL,
    MESSAGES_AMOUNT,
    SLEEP_TIME,
    counter_incr_mmap,
    create_counter_file,
    init_latency_file,
    print_latency_results,
    print_results,
    purge_queue,
    read_latencies_from_file,
    record_latency_to_file,
    report_mmap,
)
from taskiq_aio_pika import AioPikaBroker
from taskiq_aio_pika.queue import Queue as TQQueue, QueueType

MAX_ASYNC_TASKS = int(os.getenv("CONCURRENCY_LIMIT", "2000"))
PUBLISH_CONCURRENCY = 2000
PROCESSES = 8

QUEUE = "taskiq_latency_bench"
COUNTER_PATH = os.getenv("COUNTER_PATH", "")

broker = AioPikaBroker(
    AMQP_URL,
    task_queues=[TQQueue(name=QUEUE, durable=True, type=QueueType.QUORUM)],
)


@broker.task(task_name="benchmark_task")
async def benchmark_task(enqueue_time: float) -> None:
    await asyncio.sleep(SLEEP_TIME)
    lp = os.getenv("LATENCY_PATH", "")
    if lp:
        record_latency_to_file(lp, time.time() - enqueue_time)
    counter_incr_mmap(COUNTER_PATH)


async def prepare() -> None:
    purge_queue(QUEUE)
    await broker.startup()
    try:
        sem = asyncio.Semaphore(PUBLISH_CONCURRENCY)
        pending: set[asyncio.Task] = set()

        async def _send() -> None:
            await benchmark_task.kiq(time.time())
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
    finally:
        await broker.shutdown()


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    counter_path = create_counter_file()

    with tempfile.NamedTemporaryFile(delete=False, suffix=".latency") as tf:
        latency_path = tf.name
    init_latency_file(latency_path, MESSAGES_AMOUNT)

    print("Enqueueing messages...")
    loop.run_until_complete(prepare())
    print("\nDone enqueueing.")

    worker_cwd = os.path.dirname(os.path.abspath(__file__))
    worker_env = {
        **os.environ,
        "COUNTER_PATH": counter_path,
        "LATENCY_PATH": latency_path,
    }

    with open(counter_path, "r+b") as cf, mmap.mmap(cf.fileno(), 8) as mm:
        procs: list[subprocess.Popen] = [
            subprocess.Popen(
                [
                    "taskiq",
                    "worker",
                    "bench_taskiq_latency:broker",
                    "--workers",
                    "1",
                    "--log-level",
                    "WARNING",
                    "--max-async-tasks",
                    str(MAX_ASYNC_TASKS),
                    "--max-prefetch",
                    str(MAX_ASYNC_TASKS),
                    "--ack-type",
                    "when_executed",
                ],
                env=worker_env,
                cwd=worker_cwd,
            )
            for _ in range(PROCESSES)
        ]

        print("Starting benchmark.")
        start = perf_counter()
        try:
            tasks_done, timed_out, first_message_time = report_mmap(start, mm)
        finally:
            for p in procs:
                p.terminate()
            for p in procs:
                p.wait()

    os.unlink(counter_path)
    duration = perf_counter() - first_message_time

    if timed_out:
        purge_queue(QUEUE)

    print_results(tasks_done, duration)
    latencies = read_latencies_from_file(latency_path)
    os.unlink(latency_path)
    print_latency_results(latencies)
