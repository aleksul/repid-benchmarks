import fcntl
import mmap
import os
import struct
import subprocess
import tempfile
import threading
import time

import dramatiq
from _common import (
    AMQP_URL,
    MESSAGES_AMOUNT,
    SLEEP_TIME,
    print_results,
    purge_queue,
    report_mmap,
)
from dramatiq.brokers.rabbitmq import RabbitmqBroker

USE_GREEN_THREADS = os.getenv("USE_GREEN_THREADS", "1") != "0"
GEVENTS = 2000
PROCESSES = 8
PUBLISH_WORKERS = 16
ENQUEUE_BATCH_SIZE = 1000

QUEUE = "dramatiq_benchmark"
COUNTER_PATH = os.getenv("COUNTER_PATH", "")

broker = RabbitmqBroker(url=AMQP_URL)
dramatiq.set_broker(broker)


def _counter_incr() -> None:
    if not COUNTER_PATH:
        return
    with open(COUNTER_PATH, "r+b") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        with mmap.mmap(f.fileno(), 8) as mm:
            val = struct.unpack_from("q", mm, 0)[0]
            struct.pack_into("q", mm, 0, val + 1)


@dramatiq.actor(queue_name=QUEUE)
def benchmark_task() -> None:
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
    print("Enqueueing messages...")
    prepare()
    print("Done enqueueing.")

    print("Starting benchmark.")
    start_time = time.perf_counter()

    with tempfile.NamedTemporaryFile(delete=False, suffix=".counter") as tf:
        tf.write(b"\x00" * 8)
        counter_path = tf.name

    with open(counter_path, "r+b") as cf, mmap.mmap(cf.fileno(), 8) as mm:
        if USE_GREEN_THREADS:
            subprocess_args = [
                "dramatiq-gevent",
                "bench_dramatiq",
                "-p",
                str(PROCESSES),
                "-t",
                str(GEVENTS),
            ]
        else:
            subprocess_args = ["dramatiq", "bench_dramatiq", "-p", str(PROCESSES)]

        proc = subprocess.Popen(
            subprocess_args,
            env={**os.environ, "COUNTER_PATH": counter_path},
            cwd=os.path.dirname(os.path.abspath(__file__)),
        )

        tasks_done, timed_out, first_message_time = report_mmap(start_time, mm)

    os.unlink(counter_path)
    duration = time.perf_counter() - first_message_time
    proc.terminate()
    proc.wait()

    if timed_out:
        purge_queue(QUEUE)

    print_results(tasks_done, duration)
