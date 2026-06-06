"""Shared publishing loops and progress output."""

from __future__ import annotations

import asyncio
import threading
import time
from collections.abc import Awaitable, Callable


async def publish_async(messages: int, concurrency: int, send: Callable[[], Awaitable[None]]) -> None:
    if concurrency < 1:
        raise ValueError("publish concurrency must be at least 1")
    if messages < 1:
        print(f"Enqueued: {messages}/{messages}", end="\r", flush=True)
        return

    next_message = 0
    done = 0

    async def _worker() -> None:
        nonlocal done, next_message
        while next_message < messages:
            next_message += 1
            await send()
            done += 1
            if done % 2000 == 0:
                print(f"Enqueued: {done}/{messages}", end="\r", flush=True)

    tasks = [asyncio.create_task(_worker()) for _ in range(min(concurrency, messages))]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    errors = [r for r in results if isinstance(r, Exception)]
    if errors:
        raise RuntimeError(f"Publishing failed: {len(errors)} error(s), first: {errors[0]}")
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


async def publish_async_at_rate(
    messages: int,
    rate_per_second: float,
    send: Callable[[], Awaitable[None]],
) -> None:
    if rate_per_second <= 0:
        raise ValueError("arrival rate must be greater than 0")
    start = time.perf_counter()
    interval = 1.0 / rate_per_second
    for i in range(1, messages + 1):
        await send()
        if i % 1000 == 0:
            print(f"Enqueued: {i}/{messages}", end="\r", flush=True)
        deadline = start + i * interval
        delay = deadline - time.perf_counter()
        if delay > 0:
            await asyncio.sleep(delay)
    print(f"Enqueued: {messages}/{messages}", end="\r", flush=True)


def publish_sync_at_rate(messages: int, rate_per_second: float, send: Callable[[], None]) -> None:
    if rate_per_second <= 0:
        raise ValueError("arrival rate must be greater than 0")
    start = time.perf_counter()
    interval = 1.0 / rate_per_second
    for i in range(1, messages + 1):
        send()
        if i % 1000 == 0:
            print(f"Enqueued: {i}/{messages}", end="\r", flush=True)
        deadline = start + i * interval
        delay = deadline - time.perf_counter()
        if delay > 0:
            time.sleep(delay)
    print(f"Enqueued: {messages}/{messages}", end="\r", flush=True)


def publish_threaded(
    messages: int,
    workers: int,
    batch_size: int,
    send: Callable[[], None],
) -> None:
    lock = threading.Lock()
    done = [0]
    errors: list[Exception] = []
    per_worker = messages // workers
    remainder = messages % workers
    counts = [per_worker + (1 if i < remainder else 0) for i in range(workers)]

    def publish_chunk(count: int) -> None:
        local_done = 0
        for i in range(1, count + 1):
            try:
                send()
            except Exception as e:
                with lock:
                    errors.append(e)
                return
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
    if errors:
        raise RuntimeError(f"Publishing failed: {len(errors)} error(s), first: {errors[0]}")
    print(f"Enqueued: {messages}/{messages}", end="\r", flush=True)


def publish_threaded_chunks(
    messages: int,
    workers: int,
    batch_size: int,
    publish_chunk: Callable[[int, Callable[[int], None]], None],
) -> None:
    lock = threading.Lock()
    done = [0]
    errors: list[Exception] = []
    per_worker = messages // workers
    remainder = messages % workers
    counts = [per_worker + (1 if i < remainder else 0) for i in range(workers)]

    def progress(amount: int) -> None:
        with lock:
            done[0] += amount

    def run_chunk(count: int) -> None:
        try:
            publish_chunk(count, progress)
        except Exception as e:
            with lock:
                errors.append(e)

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
    if errors:
        raise RuntimeError(f"Publishing failed: {len(errors)} error(s), first: {errors[0]}")
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
