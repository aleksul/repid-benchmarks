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

Use the orchestrator to run every framework across a range of sleep times and
collect throughput statistics (mean ± std over 5 runs):

```bash
python run_all.py
```

Results are printed as an ASCII table and saved to `benchmarks_results.csv`.

### Options

```
python run_all.py --help

--frameworks          repid celery celery_nogt dramatiq dramatiq_nogt faststream taskiq
                      # subset of frameworks to run (default: all)
--sleep-times         0.01 0.1 0.5 1.0 5.0
                      # task sleep durations in seconds (default: as shown)
--runs                5
                      # repeated runs per (framework, sleep_time) cell
--messages            N
                      # override message count for all frameworks/sleep-times
--messages-per-framework  FW:N [FW:N ...]
                      # override message count for a specific framework, e.g. celery_nogt:5000
--amqp-url            amqp://user:testtest@localhost:5672
                      # AMQP broker URL passed to every benchmark
--rabbitmq-mgmt-url   http://localhost:15672
                      # RabbitMQ management HTTP URL (derived from --amqp-url by default)
--resume              # skip already-completed (framework, sleep_time, run) triples
                      # by loading the existing benchmarks_results.csv
```

## Plotting results

After running benchmarks, generate a chart from `benchmarks_results.csv`:

```bash
python plot_benchmarks.py
```

The chart is saved as `benchmarks_chart.svg`.

## Running a single benchmark

Each file lives in `benchmarks/` and can be run standalone. The following
environment variables are supported:

| Variable | Default | Description |
|---|---|---|
| `SLEEP_TIME` | `1.0` | Task sleep duration (seconds) |
| `MESSAGES_AMOUNT` | `80000` | Number of messages to enqueue |
| `TIME_LIMIT` | `300` | Max seconds to wait for processing |
| `AMQP_URL` | `amqp://user:testtest@localhost:5672` | AMQP broker URL |
| `RABBITMQ_MGMT_URL` | derived from `AMQP_URL` | RabbitMQ management HTTP URL |

```bash
SLEEP_TIME=0.1 MESSAGES_AMOUNT=1000 python benchmarks/bench_repid.py
python benchmarks/bench_celery.py
python benchmarks/bench_celery_nogt.py
python benchmarks/bench_dramatiq.py
python benchmarks/bench_dramatiq_nogt.py
python benchmarks/bench_faststream.py
python benchmarks/bench_taskiq.py
```

Every benchmark automatically purges its queue before enqueueing, so no
manual queue cleanup is needed between runs.
