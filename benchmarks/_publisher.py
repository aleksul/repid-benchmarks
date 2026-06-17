"""Subprocess entry point for publishing one chunk of benchmark messages."""

from __future__ import annotations

import argparse
import importlib
import os
from pathlib import Path

from benchmarks._runtime import CONFIG_ENV, load_config


def run(config_path: Path) -> None:
    os.environ[CONFIG_ENV] = str(config_path)
    cfg = load_config(config_path)
    adapter = importlib.import_module(f"benchmarks.adapters.{cfg.framework}")
    adapter.publish(cfg)  # type: ignore[attr-defined]


def main() -> None:
    parser = argparse.ArgumentParser(description="Publish one benchmark message chunk")
    parser.add_argument("--config", type=Path, required=True)
    args = parser.parse_args()
    run(args.config)


if __name__ == "__main__":
    main()
