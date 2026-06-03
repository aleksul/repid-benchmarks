"""Benchmark definitions used by run_all.py and the generic runner."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from ._runtime import Mode, TaskKind

Framework = Literal["repid", "celery", "dramatiq", "faststream", "taskiq"]

DEFAULT_SLEEP_TIMES = [0.01, 0.1, 0.5, 1.0, 5.0]
LATENCY_SLEEP_TIMES = [0.01, 0.1, 0.5, 1.0]
CPU_SLEEP_TIMES = [0.01, 0.1]
DEFAULT_TARGET_DURATION = 15.0
HIGH_CONCURRENCY_MESSAGE_CAP = 75_000
STREAMING_MESSAGE_CAP = 150_000
BURST_MESSAGE_CAP = 75_000


@dataclass(frozen=True, slots=True)
class BenchmarkSpec:
    name: str
    framework: Framework
    mode: Mode
    task_kind: TaskKind = "io"
    queue_base: str = "benchmark"
    processes: int = 8
    concurrency: int = 2000
    publish_concurrency: int = 10000
    publish_workers: int = 8
    enqueue_batch_size: int = 1000
    green_threads: bool = True
    sleep_times: tuple[float, ...] = tuple(DEFAULT_SLEEP_TIMES)
    messages: dict[float, int] | None = None


# Fallback message counts used when --calibrate is not run and no --messages override
# is given. These are approximate and may be stale; use --calibrate --target-duration
# to produce calibrated counts adapted to your hardware and broker.
BASE_MESSAGES: dict[str, dict[float, int]] = {
    "repid": {0.01: 672_000, 0.1: 643_000, 0.5: 425_000, 1.0: 220_000, 5.0: 48_000},
    "celery": {0.01: 30_000, 0.1: 28_000, 0.5: 27_000, 1.0: 23_000, 5.0: 16_000},
    "celery_nogt": {0.01: 20_000, 0.1: 5_000},
    "dramatiq": {0.01: 115_000, 0.1: 83_000, 0.5: 130_000, 1.0: 125_000, 5.0: 16_000},
    "dramatiq_nogt": {0.01: 30_000, 0.1: 10_000, 0.5: 5_000, 1.0: 2_000},
    "faststream": {0.01: 330_000, 0.1: 310_000, 0.5: 275_000, 1.0: 195_000, 5.0: 48_000},
    "taskiq": {0.01: 250_000, 0.1: 250_000, 0.5: 190_000, 1.0: 150_000, 5.0: 48_000},
}

CPU_MESSAGES = {0.01: 4_000, 0.1: 1_000}
LATENCY_MESSAGES = {0.01: 10_000, 0.1: 10_000, 0.5: 10_000, 1.0: 10_000}


def _cap_messages(messages: dict[float, int], cap: int) -> dict[float, int]:
    return {sleep_time: min(count, cap) for sleep_time, count in messages.items()}


def _queue(framework: str, mode: str) -> str:
    return f"{framework}_{mode}_bench" if mode not in ("base", "nogt", "hc", "cpu") else f"{framework}_benchmark"


def _spec(name: str, framework: Framework, mode: Mode, **kwargs: object) -> BenchmarkSpec:
    return BenchmarkSpec(
        name=name,
        framework=framework,
        mode=mode,
        queue_base=_queue(framework, mode),
        **kwargs,
    )


def _framework_specs(framework: Framework, publish_workers: int, publish_concurrency: int) -> list[BenchmarkSpec]:
    base_messages = BASE_MESSAGES[framework]
    return [
        _spec(framework, framework, "base", messages=base_messages, publish_workers=publish_workers, publish_concurrency=publish_concurrency),
        _spec(f"{framework}_hc", framework, "hc", concurrency=10000, messages=_cap_messages(base_messages, HIGH_CONCURRENCY_MESSAGE_CAP), publish_workers=publish_workers, publish_concurrency=publish_concurrency),
        _spec(f"{framework}_cpu", framework, "cpu", task_kind="cpu", sleep_times=tuple(CPU_SLEEP_TIMES), messages=CPU_MESSAGES, publish_workers=publish_workers, publish_concurrency=publish_concurrency),
        _spec(f"{framework}_streaming", framework, "streaming", messages=_cap_messages(base_messages, STREAMING_MESSAGE_CAP), publish_workers=publish_workers, publish_concurrency=publish_concurrency),
        _spec(f"{framework}_burst", framework, "burst", messages=_cap_messages(base_messages, BURST_MESSAGE_CAP), publish_workers=publish_workers, publish_concurrency=publish_concurrency),
        _spec(f"{framework}_latency", framework, "latency", sleep_times=tuple(LATENCY_SLEEP_TIMES), messages=LATENCY_MESSAGES, publish_workers=publish_workers, publish_concurrency=publish_concurrency),
    ]


SPECS: dict[str, BenchmarkSpec] = {}
for item in _framework_specs("repid", publish_workers=8, publish_concurrency=10000):
    SPECS[item.name] = item
for item in _framework_specs("celery", publish_workers=8, publish_concurrency=10000):
    SPECS[item.name] = item
for item in _framework_specs("dramatiq", publish_workers=16, publish_concurrency=10000):
    SPECS[item.name] = item
for item in _framework_specs("faststream", publish_workers=8, publish_concurrency=10000):
    SPECS[item.name] = item
for item in _framework_specs("taskiq", publish_workers=8, publish_concurrency=2000):
    SPECS[item.name] = item

SPECS["celery_nogt"] = _spec(
    "celery_nogt",
    "celery",
    "nogt",
    green_threads=False,
    messages=BASE_MESSAGES["celery_nogt"],
)
SPECS["dramatiq_nogt"] = _spec(
    "dramatiq_nogt",
    "dramatiq",
    "nogt",
    green_threads=False,
    publish_workers=16,
    messages=BASE_MESSAGES["dramatiq_nogt"],
)

ALL_BENCHMARKS = list(SPECS)


def get_spec(name: str) -> BenchmarkSpec:
    try:
        return SPECS[name]
    except KeyError as e:
        raise ValueError(f"Unknown benchmark: {name}") from e
