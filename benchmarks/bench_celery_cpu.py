import mmap
import os
import subprocess
import threading
import time

import celery
from kombu import Exchange as KombuExchange, Queue as KombuQueue
from _common import (
    AMQP_URL,
    MESSAGES_AMOUNT,
    SLEEP_TIME,
    counter_incr_mmap,
    create_counter_file,
    cpu_work,
    print_results,
    purge_queue,
    report_mmap,
)

PROCESSES = 8
PUBLISH_WORKERS = 8
ENQUEUE_BATCH_SIZE = 1000

QUEUE = "celery_benchmark"
COUNTER_PATH = os.getenv("COUNTER_PATH", "")

celery_app = celery.Celery(broker=AMQP_URL.replace("amqp://", "pyamqp://", 1))
celery_app.conf.task_default_queue = QUEUE
celery_app.conf.task_queues = (KombuQueue(QUEUE, exchange=KombuExchange("celery", type="direct"), routing_key=QUEUE, durable=True),)
celery_app.conf.worker_enable_remote_control = False
celery_app.conf.event_queue_exclusive = True


@celery_app.task(name="celery-bench", acks_late=True)
def benchmark_task() -> None:
    cpu_work(SLEEP_TIME)
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
                benchmark_task.apply_async(producer=producer)
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

    counter_path = create_counter_file()

    with open(counter_path, "r+b") as cf, mmap.mmap(cf.fileno(), 8) as mm:
        base_args = ["celery", "-A", "bench_celery_cpu.celery_app", "worker", "--without-mingle", "--without-gossip"]
        procs = [
            subprocess.Popen(
                base_args + ["-c", str(PROCESSES), "-Q", QUEUE],
                env={**os.environ, "COUNTER_PATH": counter_path},
                cwd=os.path.dirname(os.path.abspath(__file__)),
            )
        ]

        try:
            tasks_done, timed_out, first_message_time = report_mmap(start_time, mm)
        finally:
            for proc in procs:
                proc.terminate()
            for proc in procs:
                proc.wait()

    os.unlink(counter_path)
    duration = time.perf_counter() - first_message_time

    if timed_out:
        purge_queue(QUEUE)

    print_results(tasks_done, duration)