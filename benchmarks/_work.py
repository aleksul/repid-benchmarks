"""Shared task work, polling, latency, and result reporting helpers."""

from __future__ import annotations

import fcntl
import hashlib
import mmap
import struct
import time
from multiprocessing.sharedctypes import Synchronized

CPU_WORK_CALIBRATION_SECONDS = 0.25
_CPU_WORK_DATA = b"benchmark_cpu_payload_data" * 40


def calibrate_cpu_work_iterations(duration_sec: float) -> int:
    if duration_sec <= 0:
        return 0
    sample_seconds = max(CPU_WORK_CALIBRATION_SECONDS, 0.001)
    digest = hashlib.sha256
    iterations = 0
    start = time.perf_counter()
    deadline = start + sample_seconds
    while time.perf_counter() < deadline:
        digest(_CPU_WORK_DATA).digest()
        iterations += 1
    elapsed = max(time.perf_counter() - start, 0.001)
    return max(1, round(iterations * duration_sec / elapsed))


def cpu_work(iterations: int | None) -> None:
    if iterations is None:
        raise RuntimeError("CPU work iterations are required for CPU benchmarks")
    digest = hashlib.sha256
    for _ in range(iterations):
        digest(_CPU_WORK_DATA).digest()


def init_latency_file(path: str, capacity: int) -> None:
    with open(path, "wb") as f:
        f.write(b"\x00" * (8 + capacity * 8))


def record_latency_to_file(path: str | None, latency: float) -> None:
    if not path:
        return
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
    with open(path, "rb") as f:
        with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
            n = struct.unpack_from("q", mm, 0)[0]
            return [struct.unpack_from("d", mm, 8 + i * 8)[0] for i in range(n)]


def print_latency_results(latencies: list[float]) -> None:
    if not latencies:
        print("No latency data collected.")
        return
    values = sorted(latencies)
    n = len(values)
    p50 = values[min(int(n * 0.50), n - 1)] * 1000
    p95 = values[min(int(n * 0.95), n - 1)] * 1000
    p99 = values[min(int(n * 0.99), n - 1)] * 1000
    p999 = values[min(int(n * 0.999), n - 1)] * 1000
    print(f"Latency samples: {n}")
    print(f"Latency p50:    {p50:.2f} ms")
    print(f"Latency p95:    {p95:.2f} ms")
    print(f"Latency p99:    {p99:.2f} ms")
    print(f"Latency p99.9:  {p999:.2f} ms")
    print(f"LATENCY_P50: {p50:.4f}")
    print(f"LATENCY_P95: {p95:.4f}")
    print(f"LATENCY_P99: {p99:.4f}")


def print_results(tasks_done: int, duration: float) -> None:
    duration = max(duration, 0.000001)
    throughput = tasks_done / duration
    print("", "Benchmark ended.", f"Took {duration:.2f} sec.", f"Rate {throughput:.2f} msg/sec.", sep="\n")
    print(f"THROUGHPUT: {throughput:.2f}")


def report_value(start: float, counter: Synchronized, total: int, time_limit: float) -> tuple[int, bool, float]:
    first_message_time: float | None = None
    while True:
        count = counter.value
        if first_message_time is None and count > 0:
            first_message_time = time.perf_counter()
        if count >= total:
            return total, False, first_message_time if first_message_time is not None else start
        elapsed = time.perf_counter() - start
        if elapsed >= time_limit:
            tasks_done = counter.value
            print(f"\nTime limit reached ({elapsed:.1f}s). Processed: {tasks_done}/{total}.")
            return tasks_done, True, first_message_time if first_message_time is not None else start
        print(f"Tasks done: {count}/{total}. Elapsed: {elapsed:.2f}s.", end="\r", flush=True)
        time.sleep(0.05)


def report_mmap(start: float, mm: mmap.mmap, total: int, time_limit: float) -> tuple[int, bool, float]:
    first_message_time: float | None = None
    while True:
        mm.seek(0)
        count = struct.unpack("q", mm.read(8))[0]
        if first_message_time is None and count > 0:
            first_message_time = time.perf_counter()
        if count >= total:
            return total, False, first_message_time if first_message_time is not None else start
        elapsed = time.perf_counter() - start
        if elapsed >= time_limit:
            mm.seek(0)
            tasks_done = struct.unpack("q", mm.read(8))[0]
            print(f"\nTime limit reached ({elapsed:.1f}s). Processed: {tasks_done}/{total}.")
            return tasks_done, True, first_message_time if first_message_time is not None else start
        print(f"Tasks done: {count}/{total}. Elapsed: {elapsed:.2f}s.", end="\r", flush=True)
        time.sleep(0.05)
