"""Taskiq streaming benchmark — workers start first, then messages are published."""

from __future__ import annotations

import asyncio
import mmap
import os
import subprocess
import threading
import time
from time import perf_counter

from _common import (
    AMQP_URL,
    MESSAGES_AMOUNT,
    SLEEP_TIME,
    counter_incr_mmap,
    create_counter_file,
    print_results,
    purge_queue,
    report_mmap,
)
from taskiq_aio_pika import AioPikaBroker
from taskiq_aio_pika.queue import Queue as TQQueue, QueueType

MAX_ASYNC_TASKS = int(os.getenv("CONCURRENCY_LIMIT", "2000"))
PUBLISH_CONCURRENCY = 2000
PROCESSES = 8

QUEUE = "taskiq_streaming_bench"
COUNTER_PATH = os.getenv("COUNTER_PATH", "")

broker = AioPikaBroker(
    AMQP_URL,
    task_queues=[TQQueue(name=QUEUE, durable=True, type=QueueType.QUORUM)],
)


@broker.task(task_name="benchmark_task")
async def benchmark_task() -> None:
    await asyncio.sleep(SLEEP_TIME)
    counter_incr_mmap(COUNTER_PATH)


async def _publish() -> None:
    """Publish all messages without purging (queue is already clean)."""
    await broker.startup()
    try:
        sem = asyncio.Semaphore(PUBLISH_CONCURRENCY)
        pending: set[asyncio.Task] = set()

        async def _send() -> None:
            await benchmark_task.kiq()
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

    purge_queue(QUEUE)

    counter_path = create_counter_file()

    worker_cwd = os.path.dirname(os.path.abspath(__file__))

    with open(counter_path, "r+b") as cf, mmap.mmap(cf.fileno(), 8) as mm:
        procs: list[subprocess.Popen] = [
            subprocess.Popen(
                [
                    "taskiq",
                    "worker",
                    "bench_taskiq_streaming:broker",
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
                env={**os.environ, "COUNTER_PATH": counter_path},
                cwd=worker_cwd,
            )
            for _ in range(PROCESSES)
        ]

        print("Starting workers first...")
        time.sleep(3.0)

        print("Publishing messages while workers are running...")
        pub_start = perf_counter()
        pub_thread = threading.Thread(
            target=lambda: asyncio.run(_publish()),
            daemon=True,
        )
        pub_thread.start()

        try:
            tasks_done, timed_out, _ = report_mmap(pub_start, mm)
        finally:
            for p in procs:
                p.terminate()
            for p in procs:
                p.wait()

    os.unlink(counter_path)
    duration = perf_counter() - pub_start
    pub_thread.join(timeout=60)

    if timed_out:
        purge_queue(QUEUE)

    print_results(tasks_done, duration)
