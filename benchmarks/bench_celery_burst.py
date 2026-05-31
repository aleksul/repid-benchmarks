"""Celery burst benchmark — messages published in periodic bursts."""

from __future__ import annotations

import mmap
import os
import subprocess
import threading
import time
from time import perf_counter

import celery
from kombu import Exchange as KombuExchange, Queue as KombuQueue
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

GEVENTS = int(os.getenv("CONCURRENCY_LIMIT", "2000"))
PROCESSES = 8
PUBLISH_WORKERS = 8
ENQUEUE_BATCH_SIZE = 1000

QUEUE = "celery_burst_bench"
COUNTER_PATH = os.getenv("COUNTER_PATH", "")

celery_app = celery.Celery(broker=AMQP_URL.replace("amqp://", "pyamqp://", 1))
celery_app.conf.task_default_queue = QUEUE
celery_app.conf.task_queues = (KombuQueue(QUEUE, exchange=KombuExchange("celery", type="direct"), routing_key=QUEUE, durable=True),)
celery_app.conf.worker_enable_remote_control = False
celery_app.conf.event_queue_exclusive = True


@celery_app.task(name="celery-burst-bench", acks_late=True)
def burst_bench() -> None:
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
            with celery_app.producer_pool.acquire(block=True) as producer:
                for j in range(1, count + 1):
                    burst_bench.apply_async(producer=producer)
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

    worker_env = {**os.environ, "COUNTER_PATH": counter_path}
    worker_cwd = os.path.dirname(os.path.abspath(__file__))
    base_args = ["celery", "-A", "bench_celery_burst.celery_app", "worker", "--without-mingle", "--without-gossip"]

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

        print("Starting workers first...")
        time.sleep(3.0)

        print(f"Publishing in bursts of {BURST_SIZE} every {BURST_INTERVAL}s...")
        pub_start = perf_counter()
        pub_thread = threading.Thread(target=_publish_bursts, daemon=True)
        pub_thread.start()

        try:
            tasks_done, timed_out, _ = report_mmap(pub_start, mm)
        finally:
            for proc in procs:
                proc.terminate()
            for proc in procs:
                proc.wait()

    os.unlink(counter_path)
    duration = perf_counter() - pub_start
    pub_thread.join(timeout=60)

    if timed_out:
        purge_queue(QUEUE)

    print_results(tasks_done, duration)
