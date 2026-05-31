"""Shared constants and utilities used by all benchmark scripts."""

from __future__ import annotations

import base64
import fcntl
import hashlib
import mmap
import os
import struct
import time
import urllib.error
import urllib.parse
import urllib.request
from multiprocessing.sharedctypes import Synchronized

MESSAGES_AMOUNT = int(os.getenv("MESSAGES_AMOUNT", "80000"))
SLEEP_TIME = float(os.getenv("SLEEP_TIME", "1.0"))
TIME_LIMIT = float(os.getenv("TIME_LIMIT", "300"))

TASK_TYPE = os.getenv("TASK_TYPE", "io")  # "io" (sleep) or "cpu" (compute)

BURST_SIZE = int(os.getenv("BURST_SIZE", "5000"))
BURST_INTERVAL = float(os.getenv("BURST_INTERVAL", "1.0"))

AMQP_URL = os.getenv("AMQP_URL", "amqp://user:testtest@localhost:5672")
_amqp = urllib.parse.urlparse(AMQP_URL)
RABBITMQ_MGMT_URL = os.getenv("RABBITMQ_MGMT_URL", f"http://{_amqp.hostname}:15672")
RABBITMQ_USER = _amqp.username or "user"
RABBITMQ_PASS = _amqp.password or "testtest"


def purge_queue(queue_name: str) -> None:
    """Purge *queue_name* via the RabbitMQ Management API (ignores 404)."""
    url = f"{RABBITMQ_MGMT_URL}/api/queues/%2F/{queue_name}/contents"
    credentials = base64.b64encode(f"{RABBITMQ_USER}:{RABBITMQ_PASS}".encode()).decode()
    request = urllib.request.Request(
        url,
        method="DELETE",
        headers={"Authorization": f"Basic {credentials}"},
    )
    try:
        urllib.request.urlopen(request)
    except urllib.error.HTTPError as e:
        if e.code != 404:
            raise


def read_counter_mmap(mm: mmap.mmap) -> int:
    """Read the 64-bit signed integer counter stored at the start of *mm*."""
    mm.seek(0)
    return struct.unpack("q", mm.read(8))[0]


def report_mmap(start: float, mm: mmap.mmap) -> tuple[int, bool, float]:
    """Poll *mm* until all messages are processed or TIME_LIMIT is reached.

    Returns ``(tasks_done, timed_out, first_message_time)`` where
    *first_message_time* is the timestamp when the first message was observed
    as processed (falls back to *start* if no message was ever processed).
    """
    first_message_time: float | None = None
    while True:
        count = read_counter_mmap(mm)
        if first_message_time is None and count > 0:
            first_message_time = time.perf_counter()
        if count >= MESSAGES_AMOUNT:
            break
        elapsed = time.perf_counter() - start
        if elapsed >= TIME_LIMIT:
            tasks_done = read_counter_mmap(mm)
            print(
                f"\nTime limit reached ({elapsed:.1f}s). Processed: {tasks_done}/{MESSAGES_AMOUNT}."
            )
            return (
                tasks_done,
                True,
                first_message_time if first_message_time is not None else start,
            )
        print(
            f"Tasks done: {count}/{MESSAGES_AMOUNT}. Elapsed: {elapsed:.2f}s.",
            end="\r",
            flush=True,
        )
        time.sleep(0.05)
    return (
        MESSAGES_AMOUNT,
        False,
        first_message_time if first_message_time is not None else start,
    )


def report_value(start: float, counter: Synchronized) -> tuple[int, bool, float]:
    """Poll a multiprocessing *counter* until all messages are processed or TIME_LIMIT is reached.

    Returns ``(tasks_done, timed_out, first_message_time)`` where
    *first_message_time* is the timestamp when the first message was observed
    as processed (falls back to *start* if no message was ever processed).
    """
    first_message_time: float | None = None
    while True:
        count = counter.value
        if first_message_time is None and count > 0:
            first_message_time = time.perf_counter()
        if count >= MESSAGES_AMOUNT:
            break
        elapsed = time.perf_counter() - start
        if elapsed >= TIME_LIMIT:
            tasks_done = counter.value
            print(
                f"\nTime limit reached ({elapsed:.1f}s). Processed: {tasks_done}/{MESSAGES_AMOUNT}."
            )
            return (
                tasks_done,
                True,
                first_message_time if first_message_time is not None else start,
            )
        print(
            f"Tasks done: {count}/{MESSAGES_AMOUNT}. Elapsed: {elapsed:.2f}s.",
            end="\r",
            flush=True,
        )
        time.sleep(0.05)
    return (
        MESSAGES_AMOUNT,
        False,
        first_message_time if first_message_time is not None else start,
    )


def counter_incr_mmap(counter_path: str) -> None:
    """Atomically increment the 64-bit counter in the mmap file at *counter_path*."""
    if not counter_path:
        return
    with open(counter_path, "r+b") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        try:
            with mmap.mmap(f.fileno(), 8) as mm:
                val = struct.unpack_from("q", mm, 0)[0]
                struct.pack_into("q", mm, 0, val + 1)
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)


def create_counter_file() -> str:
    """Create a temporary mmap counter file and return its path. The caller
    must unlink the file when done."""
    import tempfile
    tf = tempfile.NamedTemporaryFile(delete=False, suffix=".counter")
    tf.write(b"\x00" * 8)
    tf.close()
    return tf.name


def declare_queue(queue_name: str) -> None:
    """Declare a durable queue via the RabbitMQ Management API."""
    url = f"{RABBITMQ_MGMT_URL}/api/queues/%2F/{queue_name}"
    credentials = base64.b64encode(f"{RABBITMQ_USER}:{RABBITMQ_PASS}".encode()).decode()
    request = urllib.request.Request(
        url,
        data=b'{"durable":true}',
        method="PUT",
        headers={
            "Authorization": f"Basic {credentials}",
            "Content-Type": "application/json",
        },
    )
    try:
        urllib.request.urlopen(request)
    except urllib.error.HTTPError as e:
        if e.code not in (200, 201, 204):
            raise


def cpu_work(duration_sec: float) -> None:
    """Perform CPU-bound work for approximately *duration_sec* seconds."""
    deadline = time.perf_counter() + duration_sec
    data = b"benchmark_cpu_payload_data" * 40  # ~1 KB
    while time.perf_counter() < deadline:
        hashlib.sha256(data).digest()


# ---------------------------------------------------------------------------
# Latency file helpers
# ---------------------------------------------------------------------------


def init_latency_file(path: str, capacity: int) -> None:
    """Create a latency mmap file: 8-byte int64 count + *capacity* float64 slots."""
    with open(path, "wb") as f:
        f.write(b"\x00" * (8 + capacity * 8))


def record_latency_to_file(path: str, latency: float) -> None:
    """Atomically append a latency value (seconds) to the latency mmap file."""
    with open(path, "r+b") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        try:
            with mmap.mmap(f.fileno(), 0) as mm:
                idx = struct.unpack_from("q", mm, 0)[0]
                struct.pack_into("d", mm, 8 + idx * 8, latency)
                struct.pack_into("q", mm, 0, idx + 1)
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)


def read_latencies_from_file(path: str) -> list[float]:
    """Read all recorded latency values (seconds) from the latency mmap file."""
    with open(path, "rb") as f:
        with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
            n = struct.unpack_from("q", mm, 0)[0]
            return [struct.unpack_from("d", mm, 8 + i * 8)[0] for i in range(n)]


def print_latency_results(latencies: list[float]) -> None:
    """Print latency percentiles (milliseconds) and machine-readable LATENCY_P* lines."""
    if not latencies:
        print("No latency data collected.")
        return
    s = sorted(latencies)
    n = len(s)
    p50 = s[min(int(n * 0.50), n - 1)] * 1000
    p95 = s[min(int(n * 0.95), n - 1)] * 1000
    p99 = s[min(int(n * 0.99), n - 1)] * 1000
    p999 = s[min(int(n * 0.999), n - 1)] * 1000
    print(f"Latency samples: {n}")
    print(f"Latency p50:    {p50:.2f} ms")
    print(f"Latency p95:    {p95:.2f} ms")
    print(f"Latency p99:    {p99:.2f} ms")
    print(f"Latency p99.9:  {p999:.2f} ms")
    print(f"LATENCY_P50: {p50:.4f}")
    print(f"LATENCY_P95: {p95:.4f}")
    print(f"LATENCY_P99: {p99:.4f}")


def print_results(tasks_done: int, duration: float) -> None:
    """Print the final throughput summary."""
    throughput = tasks_done / duration
    print(
        "",
        "Benchmark ended.",
        f"Took {duration:.2f} sec.",
        f"Rate {throughput:.2f} msg/sec.",
        sep="\n",
    )
    print(f"THROUGHPUT: {throughput:.2f}")
