"""Dramatiq burst benchmark — messages published in periodic bursts."""

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
from dramatiq.brokers.rabbitmq import RabbitmqBroker

GEVENTS = int(os.getenv("CONCURRENCY_LIMIT", "2000"))
PROCESSES = 8
PUBLISH_WORKERS = 16
ENQUEUE_BATCH_SIZE = 1000

QUEUE = "dramatiq_burst_bench"
COUNTER_PATH = os.getenv("COUNTER_PATH", "")

broker = RabbitmqBroker(url=AMQP_URL)
dramatiq.set_broker(broker)


@dramatiq.actor(queue_name=QUEUE)
def benchmark_task() -> None:
    time.sleep(SLEEP_TIME)
    counter_incr_mmap(COUNTER_PATH)


def _publish_bursts() -> None:
    """Publish MESSAGES_AMOUNT messages in bursts of BURST_SIZE."""
    remaining = MESSAGES_AMOUNT
    burst_num = 0

    while remaining > 0:
        n = min(BURST_SIZE, remaining)
        pub_lock = threading.Lock()
        pub_done = [0]

        per_worker = n // PUBLISH_WORKERS
        remainder_msgs = n % PUBLISH_WORKERS
        counts = [
            per_worker + (1 if i < remainder_msgs else 0)
            for i in range(PUBLISH_WORKERS)
        ]

        def publish_chunk(count: int) -> None:
            local_broker = RabbitmqBroker(url=AMQP_URL)
            local_broker.declare_queue(QUEUE)
            for j in range(1, count + 1):
                msg = benchmark_task.message()
                local_broker.enqueue(msg)
                if j % ENQUEUE_BATCH_SIZE == 0:
                    with pub_lock:
                        pub_done[0] += ENQUEUE_BATCH_SIZE
            leftover = count % ENQUEUE_BATCH_SIZE
            if leftover:
                with pub_lock:
                    pub_done[0] += leftover

        threads = [
            threading.Thread(target=publish_chunk, args=(c,), daemon=True)
            for c in counts
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        remaining -= n
        burst_num += 1
        print(
            f"Burst {burst_num}: published {n} messages ({remaining} remaining)",
            flush=True,
        )

        if remaining > 0:
            time.sleep(BURST_INTERVAL)


if __name__ == "__main__":
    purge_queue(QUEUE)

    counter_path = create_counter_file()

    worker_cwd = os.path.dirname(os.path.abspath(__file__))
    worker_env = {**os.environ, "COUNTER_PATH": counter_path}

    with open(counter_path, "r+b") as cf, mmap.mmap(cf.fileno(), 8) as mm:
        subprocess_args = [
            "dramatiq-gevent",
            "bench_dramatiq_burst",
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

        print(f"Publishing in bursts of {BURST_SIZE} every {BURST_INTERVAL}s...")
        pub_start = perf_counter()
        pub_thread = threading.Thread(target=_publish_bursts, daemon=True)
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
