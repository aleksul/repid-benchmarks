"""Dramatiq latency benchmark — tracks per-message end-to-end latency (p50/p95/p99).

The enqueue timestamp is passed as a task argument.
Workers record completion latency to a shared mmap file.
"""

from __future__ import annotations

import mmap
import os
import subprocess
import tempfile
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
    init_latency_file,
    print_latency_results,
    print_results,
    purge_queue,
    read_latencies_from_file,
    record_latency_to_file,
    report_mmap,
)
from dramatiq.brokers.rabbitmq import RabbitmqBroker

GEVENTS = int(os.getenv("CONCURRENCY_LIMIT", "2000"))
PROCESSES = 8
PUBLISH_WORKERS = 16
ENQUEUE_BATCH_SIZE = 1000

QUEUE = "dramatiq_latency_bench"
COUNTER_PATH = os.getenv("COUNTER_PATH", "")

broker = RabbitmqBroker(url=AMQP_URL)
dramatiq.set_broker(broker)


@dramatiq.actor(queue_name=QUEUE)
def benchmark_task(enqueue_time: float) -> None:
    time.sleep(SLEEP_TIME)
    lp = os.getenv("LATENCY_PATH", "")
    if lp:
        record_latency_to_file(lp, time.time() - enqueue_time)
    counter_incr_mmap(COUNTER_PATH)


def prepare() -> None:
    purge_queue(QUEUE)

    pub_lock = threading.Lock()
    pub_done = [0]

    per_worker = MESSAGES_AMOUNT // PUBLISH_WORKERS
    remainder = MESSAGES_AMOUNT % PUBLISH_WORKERS
    counts = [per_worker + (1 if i < remainder else 0) for i in range(PUBLISH_WORKERS)]

    def publish_chunk(n: int) -> None:
        local_broker = RabbitmqBroker(url=AMQP_URL)
        for i in range(1, n + 1):
            msg = benchmark_task.message(time.time())
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
    print("Enqueueing messages...")
    prepare()
    print("Done enqueueing.")

    counter_path = create_counter_file()

    with tempfile.NamedTemporaryFile(delete=False, suffix=".latency") as tf:
        latency_path = tf.name
    init_latency_file(latency_path, MESSAGES_AMOUNT)

    worker_cwd = os.path.dirname(os.path.abspath(__file__))
    worker_env = {
        **os.environ,
        "COUNTER_PATH": counter_path,
        "LATENCY_PATH": latency_path,
    }

    print("Starting benchmark.")
    start_time = perf_counter()

    with open(counter_path, "r+b") as cf, mmap.mmap(cf.fileno(), 8) as mm:
        subprocess_args = [
            "dramatiq-gevent",
            "bench_dramatiq_latency",
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

        try:
            tasks_done, timed_out, first_message_time = report_mmap(start_time, mm)
        finally:
            proc.terminate()
            proc.wait()

    duration = perf_counter() - first_message_time

    if timed_out:
        purge_queue(QUEUE)

    print_results(tasks_done, duration)
    latencies = read_latencies_from_file(latency_path)
    os.unlink(latency_path)
    print_latency_results(latencies)

    os.unlink(counter_path)
