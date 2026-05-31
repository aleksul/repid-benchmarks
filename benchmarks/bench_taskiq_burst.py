"""Taskiq burst benchmark — messages published in periodic bursts."""

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
    BURST_INTERVAL,
    BURST_SIZE,
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

QUEUE = "taskiq_burst_bench"
COUNTER_PATH = os.getenv("COUNTER_PATH", "")

broker = AioPikaBroker(
    AMQP_URL,
    task_queues=[TQQueue(name=QUEUE, durable=True, type=QueueType.QUORUM)],
)


@broker.task(task_name="benchmark_task")
async def benchmark_task() -> None:
    await asyncio.sleep(SLEEP_TIME)
    counter_incr_mmap(COUNTER_PATH)


async def _publish_bursts() -> None:
    """Publish MESSAGES_AMOUNT messages in bursts of BURST_SIZE."""
    await broker.startup()
    try:
        remaining = MESSAGES_AMOUNT
        burst_num = 0

        while remaining > 0:
            n = min(BURST_SIZE, remaining)
            sem = asyncio.Semaphore(PUBLISH_CONCURRENCY)
            pending: set[asyncio.Task] = set()

            async def _send() -> None:
                await benchmark_task.kiq()
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
                    "bench_taskiq_burst:broker",
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

        print(f"Publishing in bursts of {BURST_SIZE} every {BURST_INTERVAL}s...")
        pub_start = perf_counter()
        pub_thread = threading.Thread(
            target=lambda: asyncio.run(_publish_bursts()),
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
