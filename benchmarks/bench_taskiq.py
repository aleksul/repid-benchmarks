from __future__ import annotations

import asyncio
import fcntl
import mmap
import os
import struct
import subprocess
import tempfile
from time import perf_counter

from _common import (
    AMQP_URL,
    MESSAGES_AMOUNT,
    SLEEP_TIME,
    print_results,
    purge_queue,
    report_mmap,
)
from taskiq_aio_pika import AioPikaBroker

PROCESSES = 8
MAX_ASYNC_TASKS = 2000
PUBLISH_CONCURRENCY = 2000

QUEUE = "taskiq_benchmark"
COUNTER_PATH = os.getenv("COUNTER_PATH", "")

broker = AioPikaBroker(AMQP_URL, queue_name=QUEUE)


def _counter_incr() -> None:
    if not COUNTER_PATH:
        return
    with open(COUNTER_PATH, "r+b") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        with mmap.mmap(f.fileno(), 8) as mm:
            val = struct.unpack_from("q", mm, 0)[0]
            struct.pack_into("q", mm, 0, val + 1)


@broker.task
async def benchmark_task() -> None:
    await asyncio.sleep(SLEEP_TIME)
    _counter_incr()


async def prepare() -> None:
    purge_queue(QUEUE)
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

    print("Enqueueing messages...")
    loop.run_until_complete(prepare())
    print("\nDone enqueueing.")

    with tempfile.NamedTemporaryFile(delete=False, suffix=".counter") as tf:
        tf.write(b"\x00" * 8)
        counter_path = tf.name

    with open(counter_path, "r+b") as cf, mmap.mmap(cf.fileno(), 8) as mm:
        procs: list[subprocess.Popen] = []
        for _ in range(PROCESSES):
            procs.append(
                subprocess.Popen(
                    [
                        "taskiq",
                        "worker",
                        "bench_taskiq:broker",
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
                    cwd=os.path.dirname(os.path.abspath(__file__)),
                ),
            )

        print("Starting benchmark.")
        start = perf_counter()
        tasks_done, timed_out, first_message_time = report_mmap(start, mm)

    os.unlink(counter_path)
    end = perf_counter()
    duration = end - first_message_time

    for p in procs:
        p.terminate()
        p.wait()

    if timed_out:
        purge_queue(QUEUE)

    print_results(tasks_done, duration)
