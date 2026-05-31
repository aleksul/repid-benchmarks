"""Dramatiq streaming benchmark — workers start first, then messages are published."""

from __future__ import annotations

import mmap
import os
import subprocess
import threading
import time
from time import perf_counter

import dramatiq
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
from dramatiq.brokers.rabbitmq import RabbitmqBroker

GEVENTS = int(os.getenv("CONCURRENCY_LIMIT", "2000"))
PROCESSES = 8
PUBLISH_WORKERS = 16
ENQUEUE_BATCH_SIZE = 1000

QUEUE = "dramatiq_streaming_bench"
COUNTER_PATH = os.getenv("COUNTER_PATH", "")

broker = RabbitmqBroker(url=AMQP_URL)
dramatiq.set_broker(broker)


@dramatiq.actor(queue_name=QUEUE)
def benchmark_task() -> None:
    time.sleep(SLEEP_TIME)
    counter_incr_mmap(COUNTER_PATH)


def _publish() -> None:
    """Publish all messages without purging (queue is already clean)."""
    pub_lock = threading.Lock()
    pub_done = [0]

    per_worker = MESSAGES_AMOUNT // PUBLISH_WORKERS
    remainder = MESSAGES_AMOUNT % PUBLISH_WORKERS
    counts = [per_worker + (1 if i < remainder else 0) for i in range(PUBLISH_WORKERS)]

    def publish_chunk(n: int) -> None:
        local_broker = RabbitmqBroker(url=AMQP_URL)
        for i in range(1, n + 1):
            msg = benchmark_task.message()
            local_broker.enqueue(msg)
            if i % ENQUEUE_BATCH_SIZE == 0:
                with pub_lock:
                    pub_done[0] += ENQUEUE_BATCH_SIZE
        leftover = n % ENQUEUE_BATCH_SIZE
        if leftover:
            with pub_lock:
                pub_done[0] += leftover

    threads = [
        threading.Thread(target=publish_chunk, args=(n,), daemon=True) for n in counts
    ]
    for t in threads:
        t.start()
    while any(t.is_alive() for t in threads):
        with pub_lock:
            p = pub_done[0]
        print(f"Enqueued: {p}/{MESSAGES_AMOUNT}", end="\r", flush=True)
        time.sleep(0.1)
    for t in threads:
        t.join()
    print(f"Enqueued: {MESSAGES_AMOUNT}/{MESSAGES_AMOUNT}", end="\r", flush=True)


if __name__ == "__main__":
    purge_queue(QUEUE)

    counter_path = create_counter_file()

    worker_cwd = os.path.dirname(os.path.abspath(__file__))
    worker_env = {**os.environ, "COUNTER_PATH": counter_path}

    with open(counter_path, "r+b") as cf, mmap.mmap(cf.fileno(), 8) as mm:
        subprocess_args = [
            "dramatiq-gevent",
            "bench_dramatiq_streaming",
            "-p",
            str(PROCESSES),
            "-t",
            str(GEVENTS),
        ]

        proc = subprocess.Popen(
            subprocess_args,
            env=worker_env,
            cwd=worker_cwd,
        )

        print("Starting workers first...")
        time.sleep(3.0)

        print("Publishing messages while workers are running...")
        pub_start = perf_counter()
        pub_thread = threading.Thread(target=_publish, daemon=True)
        pub_thread.start()

        try:
            tasks_done, timed_out, _ = report_mmap(pub_start, mm)
        finally:
            proc.terminate()
            proc.wait()

    duration = perf_counter() - pub_start
    pub_thread.join(timeout=60)

    if timed_out:
        purge_queue(QUEUE)

    print_results(tasks_done, duration)

    os.unlink(counter_path)
