from __future__ import annotations

import asyncio
import base64
import ctypes
import urllib.error
import urllib.request
from multiprocessing import Process, Value
from multiprocessing.sharedctypes import Synchronized
from time import perf_counter

import uvloop
from _common import (
    AMQP_URL,
    MESSAGES_AMOUNT,
    RABBITMQ_MGMT_URL,
    RABBITMQ_PASS,
    RABBITMQ_USER,
    SLEEP_TIME,
    print_results,
    purge_queue,
    report_value,
)
from repid import AmqpServer, Repid, Router

PROCESSES = 8
PUBLISH_CONCURRENCY = 10000
TASKS_LIMIT = 2000

CHANNEL = "repid_benchmark"

server = AmqpServer(dsn=AMQP_URL)

app = Repid()
app.servers.register_server("default", server, is_default=True)

r = Router(channel=CHANNEL)

# Shared counter — set to a real Value before workers start.
_counter: Synchronized = Value(ctypes.c_long, 0)  # placeholder; replaced in __main__


@r.actor
async def benchmark_task() -> None:
    await asyncio.sleep(SLEEP_TIME)
    with _counter.get_lock():
        _counter.value += 1


app.include_router(r)


def _declare_queue() -> None:
    """Declare the benchmark queue via the RabbitMQ Management API."""
    url = f"{RABBITMQ_MGMT_URL}/api/queues/%2F/{CHANNEL}"
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


def _purge_queue() -> None:
    """Purge the benchmark queue via the RabbitMQ Management API (ignores 404)."""
    purge_queue(CHANNEL)


async def prepare() -> None:
    _declare_queue()
    _purge_queue()
    async with server.connection():
        sem = asyncio.Semaphore(PUBLISH_CONCURRENCY)
        pending: set[asyncio.Task] = set()

        async def _send() -> None:
            await app.send_message(
                channel=CHANNEL,
                payload=b"",
                headers={"topic": "benchmark_task"},
            )
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
    async with server.connection():
        await app.run_worker(graceful_shutdown_time=0, tasks_limit=TASKS_LIMIT)


def _worker_process(counter: Synchronized) -> None:
    uvloop.run(run(counter))


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    print("Enqueueing messages...")
    loop.run_until_complete(prepare())
    print("Done enqueueing.")

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
        _purge_queue()

    print_results(tasks_done, duration)
