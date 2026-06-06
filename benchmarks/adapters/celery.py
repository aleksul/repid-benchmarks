from __future__ import annotations

import math
import subprocess
import time
from pathlib import Path

import celery as celery_lib
from kombu import Exchange as KombuExchange, Queue as KombuQueue

from benchmarks._processes import counter_incr_mmap, subprocess_kwargs
from benchmarks._publishing import publish_sync_at_rate, publish_threaded_bursts, publish_threaded_chunks
from benchmarks._runtime import BenchmarkConfig, load_config
from benchmarks._work import cpu_work, record_latency_to_file

COUNTER_KIND = "mmap"
config = load_config()
celery_app = celery_lib.Celery(broker=config.amqp_url.replace("amqp://", "pyamqp://", 1))
celery_app.conf.task_default_queue = config.queue_name
celery_app.conf.task_queues = (
    KombuQueue(config.queue_name, exchange=KombuExchange("celery", type="direct"), routing_key=config.queue_name, durable=True),
)
celery_app.conf.worker_enable_remote_control = False
celery_app.conf.event_queue_exclusive = True
celery_app.conf.worker_prefetch_multiplier = max(1, math.ceil((config.prefetch_count or config.concurrency) / config.concurrency))
celery_app.conf.task_ignore_result = True
celery_app.conf.result_backend = None
celery_app.conf.worker_disable_rate_limits = True


@celery_app.task(name="celery-bench", acks_late=True)
def benchmark_task(enqueue_time: float | None = None) -> None:
    if config.task_kind == "cpu":
        cpu_work(config.cpu_work_iterations)
    else:
        time.sleep(config.sleep_time)
    if enqueue_time is not None:
        record_latency_to_file(config.latency_path, time.perf_counter() - enqueue_time)
    counter_incr_mmap(config.counter_path)


def _send_with_producer(producer: object) -> None:
    args = [time.perf_counter()] if config.is_latency else None
    benchmark_task.apply_async(args=args, producer=producer)


def _send() -> None:
    with celery_app.producer_pool.acquire(block=True) as producer:
        _send_with_producer(producer)


def _send_latency_with_producer(producer: object, record: bool) -> None:
    benchmark_task.apply_async(args=[time.perf_counter() if record else None], producer=producer)


def _publish_chunk(count: int, progress: object) -> None:
    sent = 0
    with celery_app.producer_pool.acquire(block=True) as producer:
        for i in range(1, count + 1):
            _send_with_producer(producer)
            if i % config.enqueue_batch_size == 0:
                sent += config.enqueue_batch_size
                progress(config.enqueue_batch_size)  # type: ignore[operator]
    leftover = count - sent
    if leftover:
        progress(leftover)  # type: ignore[operator]


def publish(cfg: BenchmarkConfig) -> None:
    if cfg.mode == "burst":
        publish_threaded_bursts(cfg.messages, cfg.burst_size, cfg.burst_interval, cfg.publish_workers, cfg.enqueue_batch_size, _send)
    else:
        publish_threaded_chunks(cfg.messages, cfg.publish_workers, cfg.enqueue_batch_size, _publish_chunk)


def publish_latency(cfg: BenchmarkConfig, messages: int, rate_per_second: float, record: bool) -> None:
    with celery_app.producer_pool.acquire(block=True) as producer:
        publish_sync_at_rate(messages, rate_per_second, lambda: _send_latency_with_producer(producer, record))


def start_workers(cfg: BenchmarkConfig, config_path: Path, counter: object | None = None) -> list[subprocess.Popen]:
    base = ["celery", "-A", "benchmarks.adapters.celery.celery_app", "worker", "--without-mingle", "--without-gossip"]
    procs: list[subprocess.Popen] = []
    if cfg.green_threads:
        for i in range(cfg.processes):
            procs.append(subprocess.Popen(base + ["-P", "gevent", "-c", str(cfg.concurrency), "-Q", cfg.queue_name, "-n", f"worker{i}@%h"], **subprocess_kwargs(config_path, cfg.worker_log_dir, f"{cfg.name}-celery-{i}")))
    else:
        procs.append(subprocess.Popen(base + ["-c", str(cfg.processes), "-Q", cfg.queue_name], **subprocess_kwargs(config_path, cfg.worker_log_dir, f"{cfg.name}-celery")))
    return procs
