"""Shared publishing loops and progress output."""

from __future__ import annotations

import asyncio
import threading
import time
from collections.abc import Awaitable, Callable


async def publish_async(messages: int, concurrency: int, send: Callable[[], Awaitable[None]]) -> None:
    sem = asyncio.Semaphore(concurrency)
    pending: set[asyncio.Task[None]] = set()

    async def _send() -> None:
        try:
            await send()
        finally:
            sem.release()

    for i in range(messages):
        await sem.acquire()
        task = asyncio.create_task(_send())
        pending.add(task)
        task.add_done_callback(pending.discard)
        if (i + 1) % 2000 == 0:
            print(f"Enqueued: {i + 1}/{messages}", end="\r", flush=True)
    if pending:
        await asyncio.gather(*pending)
    print(f"Enqueued: {messages}/{messages}", end="\r", flush=True)


async def publish_async_bursts(
    messages: int,
    burst_size: int,
    burst_interval: float,
    concurrency: int,
    send: Callable[[], Awaitable[None]],
) -> None:
    remaining = messages
    burst_num = 0
    while remaining > 0:
        count = min(burst_size, remaining)
        await publish_async(count, concurrency, send)
        remaining -= count
        burst_num += 1
        print(f"Burst {burst_num}: published {count} messages ({remaining} remaining)", flush=True)
        if remaining > 0:
            await asyncio.sleep(burst_interval)


def publish_threaded(
    messages: int,
    workers: int,
    batch_size: int,
    send: Callable[[], None],
) -> None:
    lock = threading.Lock()
    done = [0]
    per_worker = messages // workers
    remainder = messages % workers
    counts = [per_worker + (1 if i < remainder else 0) for i in range(workers)]

    def publish_chunk(count: int) -> None:
        local_done = 0
        for i in range(1, count + 1):
            send()
            if i % batch_size == 0:
                local_done += batch_size
                with lock:
                    done[0] += batch_size
        leftover = count - local_done
        if leftover:
            with lock:
                done[0] += leftover

    threads = [threading.Thread(target=publish_chunk, args=(n,), daemon=True) for n in counts]
    for thread in threads:
        thread.start()
    while any(thread.is_alive() for thread in threads):
        with lock:
            progress = done[0]
        print(f"Enqueued: {progress}/{messages}", end="\r", flush=True)
        time.sleep(0.1)
    for thread in threads:
        thread.join()
    print(f"Enqueued: {messages}/{messages}", end="\r", flush=True)


def publish_threaded_chunks(
    messages: int,
    workers: int,
    batch_size: int,
    publish_chunk: Callable[[int, Callable[[int], None]], None],
) -> None:
    lock = threading.Lock()
    done = [0]
    per_worker = messages // workers
    remainder = messages % workers
    counts = [per_worker + (1 if i < remainder else 0) for i in range(workers)]

    def progress(amount: int) -> None:
        with lock:
            done[0] += amount

    def run_chunk(count: int) -> None:
        publish_chunk(count, progress)

    threads = [threading.Thread(target=run_chunk, args=(n,), daemon=True) for n in counts]
    for thread in threads:
        thread.start()
    while any(thread.is_alive() for thread in threads):
        with lock:
            current = done[0]
        print(f"Enqueued: {current}/{messages}", end="\r", flush=True)
        time.sleep(0.1)
    for thread in threads:
        thread.join()
    print(f"Enqueued: {messages}/{messages}", end="\r", flush=True)


def publish_threaded_bursts(
    messages: int,
    burst_size: int,
    burst_interval: float,
    workers: int,
    batch_size: int,
    send: Callable[[], None],
) -> None:
    remaining = messages
    burst_num = 0
    while remaining > 0:
        count = min(burst_size, remaining)
        publish_threaded(count, workers, batch_size, send)
        remaining -= count
        burst_num += 1
        print(f"Burst {burst_num}: published {count} messages ({remaining} remaining)", flush=True)
        if remaining > 0:
            time.sleep(burst_interval)
