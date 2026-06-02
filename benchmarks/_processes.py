"""Shared process and counter utilities for benchmark execution."""

from __future__ import annotations

import fcntl
import mmap
import os
import signal
import struct
import subprocess
import tempfile
import time
from contextlib import contextmanager
from multiprocessing import Process
from pathlib import Path
from typing import Iterator


def create_counter_file() -> str:
    tf = tempfile.NamedTemporaryFile(delete=False, suffix=".counter")
    tf.write(b"\x00" * 8)
    tf.close()
    return tf.name


def counter_incr_mmap(counter_path: str | None) -> None:
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


def read_counter_mmap(mm: mmap.mmap) -> int:
    mm.seek(0)
    return struct.unpack("q", mm.read(8))[0]


def terminate_processes(procs: list[subprocess.Popen], timeout: float = 10.0) -> None:
    for proc in procs:
        if proc.poll() is None:
            try:
                os.killpg(proc.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            except OSError:
                proc.terminate()
    deadline = time.monotonic() + timeout
    for proc in procs:
        remaining = max(0.1, deadline - time.monotonic())
        try:
            proc.wait(timeout=remaining)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            except OSError:
                proc.kill()
            proc.wait(timeout=5)


def terminate_multiprocessing(procs: list[Process], timeout: float = 10.0) -> None:
    for proc in procs:
        if proc.is_alive():
            proc.terminate()
    deadline = time.monotonic() + timeout
    for proc in procs:
        proc.join(timeout=max(0.1, deadline - time.monotonic()))
        if proc.is_alive():
            proc.kill()
            proc.join(timeout=5)


def subprocess_kwargs(config_path: Path, log_dir: str | None, name: str) -> dict[str, object]:
    env = os.environ.copy()
    env["BENCHMARK_CONFIG_PATH"] = str(config_path)
    kwargs: dict[str, object] = {
        "env": env,
        "cwd": str(Path(__file__).parents[1]),
        "start_new_session": True,
    }
    if log_dir:
        Path(log_dir).mkdir(parents=True, exist_ok=True)
        log = open(Path(log_dir) / f"{name}.log", "ab")
        kwargs["stdout"] = log
        kwargs["stderr"] = subprocess.STDOUT
    else:
        kwargs["stdout"] = subprocess.DEVNULL
        kwargs["stderr"] = subprocess.DEVNULL
    return kwargs


@contextmanager
def mmap_counter(path: str) -> Iterator[mmap.mmap]:
    with open(path, "r+b") as f, mmap.mmap(f.fileno(), 8) as mm:
        yield mm
