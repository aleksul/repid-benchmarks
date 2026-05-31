"""FastStream benchmark — high concurrency (CONCURRENCY_LIMIT=10000, 5× base)."""

import os
import runpy
from pathlib import Path

os.environ["CONCURRENCY_LIMIT"] = "10000"

runpy.run_path(str(Path(__file__).parent / "bench_faststream.py"), run_name="__main__")
