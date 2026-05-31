"""Celery latency benchmark — tracks per-message end-to-end latency (p50/p95/p99).

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

import celery
from kombu import Exchange as KombuExchange, Queue as KombuQueue
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

GEVENTS = int(os.getenv("CONCURRENCY_LIMIT", "2000"))
PROCESSES = 8
PUBLISH_WORKERS = 8
ENQUEUE_BATCH_SIZE = 1000

QUEUE = "celery_latency_bench"
COUNTER_PATH = os.getenv("COUNTER_PATH", "")
LATENCY_PATH = os.getenv("LATENCY_PATH", "")

celery_app = celery.Celery(broker=AMQP_URL.replace("amqp://", "pyamqp://", 1))
celery_app.conf.task_default_queue = QUEUE
celery_app.conf.task_queues = (KombuQueue(QUEUE, exchange=KombuExchange("celery", type="direct"), routing_key=QUEUE, durable=True),)
celery_app.conf.worker_enable_remote_control = False
celery_app.conf.event_queue_exclusive = True


@celery_app.task(name="celery-latency-bench", acks_late=True)
def latency_bench(enqueue_time: float) -> None:
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
        with celery_app.producer_pool.acquire(block=True) as producer:
            for i in range(1, n + 1):
                latency_bench.apply_async(args=[time.time()], producer=producer)
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

    worker_env = {
        **os.environ,
        "COUNTER_PATH": counter_path,
        "LATENCY_PATH": latency_path,
    }
    worker_cwd = os.path.dirname(os.path.abspath(__file__))
    base_args = ["celery", "-A", "bench_celery_latency.celery_app", "worker", "--without-mingle", "--without-gossip"]

    print("Starting benchmark.")
    start_time = perf_counter()

    with open(counter_path, "r+b") as cf, mmap.mmap(cf.fileno(), 8) as mm:
        procs = [
            subprocess.Popen(
                base_args
                + [
                    "-P",
                    "gevent",
                    "-c",
                    str(GEVENTS),
                    "-Q",
                    QUEUE,
                    "-n",
                    f"worker{i}@%h",
                ],
                env=worker_env,
                cwd=worker_cwd,
            )
            for i in range(PROCESSES)
        ]

        try:
            tasks_done, timed_out, first_message_time = report_mmap(start_time, mm)
        finally:
            for proc in procs:
                proc.terminate()
            for proc in procs:
                proc.wait()

    os.unlink(counter_path)
    duration = perf_counter() - first_message_time

    if timed_out:
        purge_queue(QUEUE)

    print_results(tasks_done, duration)
    latencies = read_latencies_from_file(latency_path)
    os.unlink(latency_path)
    print_latency_results(latencies)
