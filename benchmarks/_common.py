"""Shared constants and utilities used by all benchmark scripts."""

from __future__ import annotations

import base64
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
