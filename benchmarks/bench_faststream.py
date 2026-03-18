from __future__ import annotations

import asyncio
import ctypes
from multiprocessing import Process, Value
from multiprocessing.sharedctypes import Synchronized
from time import perf_counter

import uvloop
from _common import (
    AMQP_URL,
    MESSAGES_AMOUNT,
    SLEEP_TIME,
    print_results,
    purge_queue,
    report_value,
)
from faststream import FastStream
from faststream.rabbit import Channel, RabbitBroker, RabbitQueue

PROCESSES = 8
PUBLISH_CONCURRENCY = 10000
MAX_WORKERS = 2000

QUEUE = "fs_benchmark"

broker = RabbitBroker(AMQP_URL)
app = FastStream(broker)

# Shared counter — set to a real Value before workers start.
_counter: Synchronized = Value(
    ctypes.c_long, 0
)  # placeholder; replaced in _worker_process


@broker.subscriber(QUEUE, channel=Channel(prefetch_count=MAX_WORKERS))
async def benchmark_task() -> None:
    await asyncio.sleep(SLEEP_TIME)
    with _counter.get_lock():
        _counter.value += 1


async def prepare() -> None:
    purge_queue(QUEUE)
    # Use a separate broker instance for publishing to avoid side effects
    async with RabbitBroker(AMQP_URL) as pub_broker:
        await pub_broker.declare_queue(RabbitQueue(QUEUE))
        sem = asyncio.Semaphore(PUBLISH_CONCURRENCY)
        pending: set[asyncio.Task] = set()

        async def _send() -> None:
            await pub_broker.publish(b"", queue=QUEUE)
            sem.release()

        for i in range(MESSAGES_AMOUNT):
            await sem.acquire()
            t = asyncio.create_task(_send())
            pending.add(t)
            t.add_done_callback(pending.discard)
            if (i + 1) % 2000 == 0:
                print(f"Enqueued: {i + 1}/{MESSAGES_AMOUNT}", end="\r", flush=True)

        if pending:
            await asyncio.gather(*pending)
        print(f"Enqueued: {MESSAGES_AMOUNT}/{MESSAGES_AMOUNT}", end="\r", flush=True)


async def run(counter: Synchronized) -> None:
    global _counter
    _counter = counter
    await app.run()


def _worker_process(counter: Synchronized) -> None:
    uvloop.run(run(counter))


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    print("Enqueueing messages...")
    loop.run_until_complete(prepare())
    print("\nDone enqueueing.")

    counter: Synchronized = Value(ctypes.c_long, 0)

    processes: list[Process] = [
        Process(target=_worker_process, args=(counter,)) for _ in range(PROCESSES)
    ]

    print("Starting benchmark.")
    start = perf_counter()

    for process in processes:
        process.start()

    tasks_done, timed_out, first_message_time = report_value(start, counter)

    end = perf_counter()
    duration = end - first_message_time

    for process in processes:
        process.terminate()
        process.join()

    if timed_out:
        purge_queue(QUEUE)

    print_results(tasks_done, duration)
