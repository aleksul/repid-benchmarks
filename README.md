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
| celery | sync processes | with gevent |
| celery_nogt | sync processes | without gevent |
| dramatiq | sync processes | with gevent |
| dramatiq_nogt | sync processes | without gevent |
| faststream | async (uvloop, in-process) | |
| taskiq | async (subprocess CLI) | |

## Running all benchmarks

Use the orchestrator to run every benchmark spec across a range of sleep times
and collect throughput statistics (mean ± std over 5 runs):

```bash
python run_all.py
```

Results are printed as an ASCII table and saved to `benchmarks_results.csv`.

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
--randomize-order     # randomize run order to reduce order bias
--time-limit          300
                      # max seconds to wait for processing
--resume              # skip already-completed (framework, sleep_time, run) triples
                      # by loading the existing benchmarks_results.csv
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
python run_all.py --frameworks repid taskiq_latency --sleep-times 0.1 --runs 1
python run_all.py --frameworks celery --amqp-url amqp://user:testtest@host:5672/
```

`run_all.py` writes a runtime config for each run, resets a unique RabbitMQ
queue, waits for workers to become consumers when workers start before publish,
and cleans up the queue after the run.
