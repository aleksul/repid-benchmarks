"""Taskiq benchmark — high concurrency (CONCURRENCY_LIMIT=10000, 5× base)."""

import os
import subprocess
import sys
from pathlib import Path

os.environ["CONCURRENCY_LIMIT"] = "10000"

result = subprocess.run(
    [sys.executable, "-m", "bench_taskiq"],
    cwd=str(Path(__file__).parent),
)
sys.exit(result.returncode)
