"""Runtime configuration shared by the benchmark runner and workers."""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Literal

Mode = Literal["base", "hc", "cpu", "burst", "latency", "steady", "nogt"]
TaskKind = Literal["io", "cpu"]

CONFIG_ENV = "BENCHMARK_CONFIG_PATH"


@dataclass(slots=True)
class BenchmarkConfig:
    name: str
    framework: str
    mode: Mode
    task_kind: TaskKind
    queue_name: str
    messages: int
    sleep_time: float
    time_limit: float
    amqp_url: str
    rabbitmq_mgmt_url: str
    processes: int
    concurrency: int
    publish_concurrency: int
    publish_workers: int
    enqueue_batch_size: int
    green_threads: bool
    burst_size: int
    burst_interval: float
    prefetch_count: int | None = None
    cpu_work_iterations: int | None = None
    counter_path: str | None = None
    latency_path: str | None = None
    keep_queue: bool = False
    worker_log_dir: str | None = None
    publish_processes: int = 1
    steady_warmup_seconds: float = 10.0
    steady_measurement_seconds: float = 30.0
    latency_arrival_rate: float = 1000.0
    latency_warmup_seconds: float = 10.0
    latency_measurement_seconds: float = 30.0
    burst_multiplier: float = 2.0

    @property
    def is_latency(self) -> bool:
        return self.mode == "latency"


def write_config(config: BenchmarkConfig, path: Path | None = None) -> Path:
    if path is None:
        fd, raw_path = tempfile.mkstemp(prefix="repid-bench-", suffix=".json")
        os.close(fd)
        path = Path(raw_path)
    path.write_text(json.dumps(asdict(config), indent=2), encoding="utf-8")
    return path


def load_config(path: str | Path | None = None) -> BenchmarkConfig:
    raw_path = path or os.environ.get(CONFIG_ENV)
    if not raw_path:
        raise RuntimeError(f"{CONFIG_ENV} is not set")
    data = json.loads(Path(raw_path).read_text(encoding="utf-8"))
    return BenchmarkConfig(**data)


def worker_env(config_path: Path) -> dict[str, str]:
    env = os.environ.copy()
    env[CONFIG_ENV] = str(config_path)
    return env
