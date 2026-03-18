"""Celery benchmark — no green threads (prefork pool only)."""

import os
import runpy
from pathlib import Path

os.environ["USE_GREEN_THREADS"] = "0"
os.environ.setdefault("MESSAGES_AMOUNT", "10000")

runpy.run_path(str(Path(__file__).parent / "bench_celery.py"), run_name="__main__")
