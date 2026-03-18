import fcntl
import mmap
import os
import struct
import subprocess
import tempfile
import threading
import time

import celery
from _common import (
    AMQP_URL,
    MESSAGES_AMOUNT,
    SLEEP_TIME,
    print_results,
    purge_queue,
    report_mmap,
)

USE_GREEN_THREADS = os.getenv("USE_GREEN_THREADS", "1") != "0"
GEVENTS = 2000
PROCESSES = 8
PUBLISH_WORKERS = 8
ENQUEUE_BATCH_SIZE = 1000

QUEUE = "celery_benchmark"
COUNTER_PATH = os.getenv("COUNTER_PATH", "")

celery_app = celery.Celery(broker=AMQP_URL.replace("amqp://", "pyamqp://", 1))
celery_app.conf.task_default_queue = QUEUE


def _counter_incr() -> None:
    if not COUNTER_PATH:
        return
    with open(COUNTER_PATH, "r+b") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        try:
            with mmap.mmap(f.fileno(), 8) as mm:
                val = struct.unpack_from("q", mm, 0)[0]
                struct.pack_into("q", mm, 0, val + 1)
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)


@celery_app.task(name="latency-bench", acks_late=True)
def latency_bench() -> None:
    time.sleep(SLEEP_TIME)
    _counter_incr()


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
                latency_bench.apply_async(producer=producer)
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

    print("Starting benchmark.")
    start_time = time.perf_counter()

    with tempfile.NamedTemporaryFile(delete=False, suffix=".counter") as tf:
        tf.write(b"\x00" * 8)
        counter_path = tf.name

    with open(counter_path, "r+b") as cf, mmap.mmap(cf.fileno(), 8) as mm:
        base_args = ["celery", "-A", "bench_celery.celery_app", "worker"]
        if USE_GREEN_THREADS:
            worker_env = {**os.environ, "COUNTER_PATH": counter_path}
            worker_cwd = os.path.dirname(os.path.abspath(__file__))
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
        else:
            procs = [
                subprocess.Popen(
                    base_args + ["-c", str(PROCESSES), "-Q", QUEUE],
                    env={**os.environ, "COUNTER_PATH": counter_path},
                    cwd=os.path.dirname(os.path.abspath(__file__)),
                )
            ]

        tasks_done, timed_out, first_message_time = report_mmap(start_time, mm)

    os.unlink(counter_path)
    duration = time.perf_counter() - first_message_time
    for proc in procs:
        proc.terminate()
    for proc in procs:
        proc.wait()

    if timed_out:
        purge_queue(QUEUE)

    print_results(tasks_done, duration)
