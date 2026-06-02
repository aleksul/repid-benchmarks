# Benchmarks

## Preparation

Benchmarks are meant to be executed on Python 3.13+.

```bash
uv sync

docker compose up -d
```

## Frameworks

All frameworks run against **RabbitMQ** as the message broker:

| Framework | Concurrency model | Notes |
|---|---|---|
| repid | async (uvloop, in-process) | |
| celery | sync processes | with gevent, prefetch=1, no result backend |
| celery_nogt | sync processes | without gevent, 1 thread per worker |
| dramatiq | sync processes | with gevent |
| dramatiq_nogt | sync processes | without gevent, 1 thread per worker |
| faststream | async (uvloop, in-process) | |
| taskiq | async (subprocess CLI) | publisher uses uvloop |

## Running all benchmarks

Use the orchestrator to run every benchmark spec across a range of sleep times
and collect throughput statistics (mean ± std over 5 runs). Run order is
randomized by default to reduce order bias:

```bash
python run_all.py
```

Results are printed as an ASCII table and saved to `benchmarks_results.csv`.
Timed-out and errored runs are recorded with their status but excluded from
averages.

### Options

```
python run_all.py --help

--frameworks          repid celery celery_nogt dramatiq dramatiq_nogt faststream taskiq ...
                      # subset of benchmark specs to run (default: all)
--sleep-times         0.01 0.1 0.5 1.0 5.0
                      # task sleep durations in seconds (default: as shown)
--runs                5
                      # repeated runs per (framework, sleep_time) cell
--messages            N
                      # override message count for all frameworks/sleep-times
--messages-per-framework  FW:N [FW:N ...]
                      # override message count for a specific framework, e.g. celery_nogt:5000
--cpu-work-iterations N
                      # fixed hash iterations per CPU task; if omitted, run_all calibrates one
                      # shared value per CPU sleep time and passes it to all CPU frameworks
--amqp-url            amqp://user:testtest@localhost:5672
                      # AMQP broker URL passed to every benchmark
--rabbitmq-mgmt-url   http://localhost:15672
                      # RabbitMQ management HTTP URL (derived from --amqp-url by default)
--warmup-runs         N
                      # warmup repetitions that are not written to CSV
--no-randomize        # disable run order randomization (randomized by default)
--seed                N
                      # random seed for run order
--time-limit          300
                      # max seconds to wait for processing
--publish-processes   N
                      # publisher subprocesses per run (default: worker process count)
--resume              # skip already-completed ok runs
                      # by loading the existing benchmarks_results.csv
--calibrate           # run calibration to determine message counts per framework/sleep-time
--target-duration     15.0
                      # target worker run duration in seconds for calibration
--counts-file         FILE
                      # load/save calibrated message counts from/to a JSON file
--keep-queues         # keep RabbitMQ queues after each run instead of deleting them
--worker-log-dir      DIR
                      # directory for worker subprocess logs
```

### Calibration

By default, message counts come from hard-coded fallback values. For more
statistically stable results, use calibration to determine counts that target
a specific worker run duration (15 seconds by default):

```bash
python run_all.py --calibrate --target-duration 15
```

This runs a short benchmark for each (framework, sleep_time) pair, measures
throughput, and computes the message count needed to run for approximately
`--target-duration` seconds. Calibrated counts can be saved and reused:

```bash
python run_all.py --calibrate --counts-file counts.json
python run_all.py --counts-file counts.json
```

## Plotting results

After running benchmarks, generate category-aware charts from
`benchmarks_results.csv` and `latency_results.csv`:

```bash
uv run python plot_benchmarks.py
```

Charts are saved to `benchmark_charts/`:

| Chart | Description |
|---|---|
| `throughput_hc.svg` | High-concurrency throughput |
| `throughput_cpu.svg` | CPU-bound throughput |
| `throughput_streaming.svg` | Streaming publish/consume throughput |
| `throughput_burst.svg` | Bursty-load throughput |
| `latency_tradeoff.svg` | Throughput plus p50, p95, and p99 latency from latency-instrumented runs |
| `latency_tradeoff_scatter.svg` | p95 latency vs throughput tradeoff; upper-left is best |

## Running Specific Benchmarks

Direct per-variant benchmark files are intentionally not supported. Run one or
more specs through `run_all.py` instead:

```bash
python run_all.py --frameworks repid taskiq_latency --sleep-times 0.1 --runs 1 --no-randomize
python run_all.py --frameworks celery --amqp-url amqp://user:testtest@host:5672/
```

`run_all.py` writes a runtime config for each run, resets a unique RabbitMQ
queue, waits for workers to become consumers when workers start before publish,
and cleans up the queue plus framework auxiliary queues after the run. Timed-out
runs are marked in the CSV with `status=timeout` and excluded from averages.

## Methodology notes

- **Run order**: randomized by default (`--no-randomize` to disable); warmup
  runs execute before measured runs.
- **Duration**: measured from benchmark start to last task completion,
  excluding worker shutdown time.
- **Timeouts**: runs exceeding `--time-limit` are recorded as `status=timeout`
  with partial throughput and excluded from averages.
- **Failure propagation**: publisher exceptions and worker crashes cause the
  run to fail with `status=error`.
- **Message counts**: hard-coded per framework by default; use `--calibrate` to
  derive counts that target equal run duration across frameworks.
- **Acknowledgement**: Celery uses `acks_late=True`, Taskiq uses
  `when_executed`, Dramatiq and FastStream use framework defaults, repid uses
  default acknowledgement. These differ intentionally to match production
  configuration for each framework.
