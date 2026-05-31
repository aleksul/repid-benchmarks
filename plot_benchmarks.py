#!/usr/bin/env python3
"""Generate category-aware benchmark charts.

The benchmark CSVs store workload variants in the framework column, for example
``repid_hc`` and ``celery_latency``. This script splits those values into a base
framework and a workload category so every chart compares equivalent runs only.
"""

from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from pathlib import Path
from statistics import mean, pstdev

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from matplotlib.transforms import Bbox

ROOT = Path(__file__).parent
BENCHMARKS_CSV = ROOT / "benchmarks_results.csv"
LATENCY_CSV = ROOT / "latency_results.csv"
OUTPUT_DIR = ROOT / "benchmark_charts"

KNOWN_CATEGORIES = ("hc", "cpu", "streaming", "burst", "latency")

CATEGORY_DISPLAY = {
    "base": "Base I/O-bound",
    "hc": "High concurrency",
    "cpu": "CPU-bound",
    "streaming": "Streaming publish/consume",
    "burst": "Bursty load",
    "latency": "Latency-instrumented",
}

CATEGORY_NOTES = {
    "base": "Default queue-drain benchmark.",
    "hc": "High worker concurrency; higher throughput is better.",
    "cpu": "CPU-bound task duration; higher throughput is better.",
    "streaming": "Publishing continues while workers consume; higher throughput is better.",
    "burst": "Messages are published in bursts; higher throughput is better.",
    "latency": "Latency-instrumented throughput; higher throughput is better.",
}

FRAMEWORK_ORDER = ["repid", "faststream", "dramatiq", "taskiq", "celery"]
FRAMEWORK_DISPLAY = {
    "repid": "repid",
    "faststream": "faststream",
    "dramatiq": "dramatiq",
    "taskiq": "taskiq",
    "celery": "celery",
    "celery_nogt": "celery w/o green threads",
    "dramatiq_nogt": "dramatiq w/o green threads",
}

FRAMEWORK_STYLES = {
    "repid": {"color": "#2563EB", "marker": "o", "lw": 2.8, "zorder": 8},
    "faststream": {"color": "#F59E0B", "marker": "s", "lw": 2.2, "zorder": 7},
    "dramatiq": {"color": "#10B981", "marker": "^", "lw": 2.2, "zorder": 6},
    "taskiq": {"color": "#EF4444", "marker": "D", "lw": 2.2, "zorder": 5},
    "celery": {"color": "#8B5CF6", "marker": "v", "lw": 2.2, "zorder": 4},
    "dramatiq_nogt": {
        "color": "#6EE7B7",
        "marker": "^",
        "lw": 1.9,
        "ls": "--",
        "zorder": 3,
    },
    "celery_nogt": {
        "color": "#C4B5FD",
        "marker": "v",
        "lw": 1.9,
        "ls": "--",
        "zorder": 2,
    },
}

plt.rcParams.update(
    {
        "figure.facecolor": "white",
        "axes.facecolor": "white",
        "axes.spines.top": False,
        "axes.spines.right": False,
        "axes.linewidth": 0.9,
        "axes.edgecolor": "#CBD5E1",
        "grid.color": "#E2E8F0",
        "grid.linewidth": 0.8,
        "font.family": "sans-serif",
        "font.size": 11,
        "xtick.color": "#64748B",
        "ytick.color": "#64748B",
        "axes.labelcolor": "#475569",
    }
)


Series = dict[str, dict[float, list[float]]]
CategoryData = dict[str, Series]


def split_framework(raw: str) -> tuple[str, str]:
    for category in KNOWN_CATEGORIES:
        suffix = f"_{category}"
        if raw.endswith(suffix):
            return raw[: -len(suffix)], category
    return raw, "base"


def load_throughput(path: Path) -> CategoryData:
    data: CategoryData = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    with path.open(newline="") as f:
        reader = csv.DictReader(f)
        required = {"framework", "sleep_time", "throughput_msg_per_sec"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"{path.name} is missing columns: {', '.join(sorted(missing))}")
        for row in reader:
            framework, category = split_framework(row["framework"])
            sleep_time = float(row["sleep_time"])
            throughput = float(row["throughput_msg_per_sec"])
            data[category][framework][sleep_time].append(throughput)
    return data


def load_latency(path: Path) -> dict[str, dict[float, dict[str, list[float]]]]:
    data: dict[str, dict[float, dict[str, list[float]]]] = defaultdict(
        lambda: defaultdict(lambda: defaultdict(list))
    )
    with path.open(newline="") as f:
        reader = csv.DictReader(f)
        required = {
            "framework",
            "sleep_time",
            "throughput_msg_per_sec",
            "p50_ms",
            "p95_ms",
            "p99_ms",
        }
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"{path.name} is missing columns: {', '.join(sorted(missing))}")
        for row in reader:
            framework, _ = split_framework(row["framework"])
            sleep_time = float(row["sleep_time"])
            for metric in ("throughput_msg_per_sec", "p50_ms", "p95_ms", "p99_ms"):
                data[framework][sleep_time][metric].append(float(row[metric]))
    return data


def average(values: list[float]) -> float:
    return mean(values)


def stddev(values: list[float]) -> float:
    return pstdev(values) if len(values) > 1 else 0.0


def fmt_compact(value: float, _pos: object = None) -> str:
    if abs(value) >= 1_000_000:
        scaled = value / 1_000_000
        return f"{scaled:.0f}M" if scaled >= 10 else f"{scaled:.1f}M"
    if abs(value) >= 1_000:
        scaled = value / 1_000
        return f"{scaled:.0f}K" if scaled >= 10 else f"{scaled:.1f}K"
    if value >= 10:
        return f"{value:.0f}"
    return f"{value:.1f}"


def fmt_sleep(value: float) -> str:
    return f"{value:g}s"


def framework_sort_key(framework: str) -> tuple[int, str]:
    try:
        return FRAMEWORK_ORDER.index(framework), framework
    except ValueError:
        return len(FRAMEWORK_ORDER), framework


def category_sort_key(category: str) -> tuple[int, str]:
    order = ["base", "hc", "cpu", "streaming", "burst", "latency"]
    try:
        return order.index(category), category
    except ValueError:
        return len(order), category


def apply_axis_style(ax: plt.Axes, sleep_times: list[float], ylabel: str) -> None:
    ax.set_xscale("log")
    ax.set_xticks(sleep_times)
    if sleep_times:
        ax.set_xlim(min(sleep_times) / 1.12, max(sleep_times) * 1.55)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _pos: fmt_sleep(x)))
    ax.xaxis.set_minor_formatter(mticker.NullFormatter())
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(fmt_compact))
    ax.grid(True, axis="y", linestyle="--", alpha=0.65)
    ax.grid(True, axis="x", linestyle="--", alpha=0.35)
    ax.set_xlabel("Task duration")
    ax.set_ylabel(ylabel)


def plot_series(
    ax: plt.Axes,
    data: Series,
    metric_name: str | None,
    show_std: bool,
    annotate_end: bool = True,
    annotate_frameworks: set[str] | None = None,
) -> None:
    end_labels: list[tuple[str, float, float, str]] = []
    for framework in sorted(data, key=framework_sort_key):
        points = data[framework]
        xs = sorted(points)
        ys = [average(points[x]) for x in xs]
        errs = [stddev(points[x]) for x in xs]
        style = FRAMEWORK_STYLES.get(framework, {})
        color = style.get("color", "#64748B")

        if show_std and any(errs):
            ax.fill_between(
                xs,
                [max(0.0, y - err) for y, err in zip(ys, errs)],
                [y + err for y, err in zip(ys, errs)],
                color=color,
                alpha=0.12,
                linewidth=0,
            )

        ax.plot(
            xs,
            ys,
            label=FRAMEWORK_DISPLAY.get(framework, framework),
            color=color,
            linewidth=style.get("lw", 2.1),
            linestyle=style.get("ls", "-"),
            marker=style.get("marker", "o"),
            markersize=6.5,
            markeredgewidth=1.4,
            markeredgecolor="white",
            solid_capstyle="round",
            zorder=style.get("zorder", 1),
        )

        if annotate_end and (annotate_frameworks is None or framework in annotate_frameworks):
            end_labels.append((framework, xs[-1], ys[-1], color))

    values = [average(values) for by_sleep in data.values() for values in by_sleep.values()]
    if values:
        ymax = max(values) * 1.18
        ax.set_ylim(0, ymax if ymax > 0 else 1)
    if metric_name:
        ax.set_title(metric_name, loc="left", fontsize=13, fontweight="bold", color="#0F172A")

    if annotate_end and end_labels:
        ymin, ymax = ax.get_ylim()
        gap = (ymax - ymin) * 0.045
        adjusted: list[tuple[str, float, float, float, str]] = []
        for framework, x, y, color in sorted(end_labels, key=lambda item: item[2]):
            label_y = y if not adjusted else max(y, adjusted[-1][3] + gap)
            adjusted.append((framework, x, y, label_y, color))
        highest_label = max(item[3] for item in adjusted)
        if highest_label > ymax * 0.98:
            ax.set_ylim(ymin, highest_label * 1.08)
        for framework, x, y, label_y, color in adjusted:
            if abs(label_y - y) > gap * 0.15:
                ax.plot(
                    [x, x * 1.055],
                    [y, label_y],
                    color=color,
                    linewidth=0.7,
                    alpha=0.45,
                    clip_on=False,
                )
            ax.text(
                x * 1.095,
                label_y,
                fmt_compact(y),
                va="center",
                fontsize=8,
                color=color,
                fontweight="bold" if framework == "repid" else "normal",
                clip_on=False,
            )


def plot_throughput_category(category: str, data: Series, output_dir: Path) -> Path:
    sleep_times = sorted({sleep_time for points in data.values() for sleep_time in points})
    fig, ax = plt.subplots(figsize=(11.5, 7.0))
    fig.subplots_adjust(left=0.09, right=0.78, top=0.82, bottom=0.13)

    title = f"{CATEGORY_DISPLAY.get(category, category.title())} Throughput"
    plot_series(ax, data, None, show_std=True)
    apply_axis_style(ax, sleep_times, "Messages / second")

    fig.text(
        0.09,
        0.965,
        title,
        ha="left",
        va="top",
        fontsize=18,
        fontweight="bold",
        color="#0F172A",
    )
    fig.text(
        0.09,
        0.915,
        f"{CATEGORY_NOTES.get(category, '')} Lines show mean of repeated runs; shaded bands show +/- 1 std dev.",
        ha="left",
        va="top",
        fontsize=10,
        color="#64748B",
    )

    handles, labels = ax.get_legend_handles_labels()
    ax.legend(
        handles,
        labels,
        loc="center left",
        bbox_to_anchor=(1.02, 0.5),
        frameon=False,
        fontsize=9.5,
    )

    output = output_dir / f"throughput_{category}.svg"
    fig.savefig(output, format="svg")
    plt.close(fig)
    return output


def latency_metric_series(
    data: dict[str, dict[float, dict[str, list[float]]]],
    metric: str,
    include_taskiq: bool | None,
) -> Series:
    series: Series = defaultdict(lambda: defaultdict(list))
    for framework, by_sleep in data.items():
        if include_taskiq is False and framework == "taskiq":
            continue
        if include_taskiq is True and framework != "taskiq":
            continue
        for sleep_time, values in by_sleep.items():
            series[framework][sleep_time] = values[metric]
    return series


def plot_latency_tradeoff(
    data: dict[str, dict[float, dict[str, list[float]]]], output_dir: Path
) -> Path:
    metrics = [("p50_ms", "p50 latency"), ("p95_ms", "p95 latency"), ("p99_ms", "p99 latency")]
    sleep_times = sorted({sleep_time for by_sleep in data.values() for sleep_time in by_sleep})
    fig = plt.figure(figsize=(17, 11.0))
    grid = fig.add_gridspec(3, 3, height_ratios=[1.35, 2.4, 0.95])
    fig.subplots_adjust(left=0.06, right=0.84, top=0.84, bottom=0.08, wspace=0.24, hspace=0.55)

    throughput_ax = fig.add_subplot(grid[0, :])
    plot_series(
        throughput_ax,
        latency_metric_series(data, "throughput_msg_per_sec", include_taskiq=False),
        "Throughput from latency-instrumented runs",
        show_std=True,
    )
    apply_axis_style(throughput_ax, sleep_times, "Messages / second")

    for col, (metric, title) in enumerate(metrics):
        ax = fig.add_subplot(grid[1, col])
        overflow_ax = fig.add_subplot(grid[2, col], sharex=ax)
        metric_series = latency_metric_series(data, metric, include_taskiq=False)
        overflow_series = latency_metric_series(data, metric, include_taskiq=None)

        plot_series(ax, metric_series, title, show_std=True)
        apply_axis_style(ax, sleep_times, "Milliseconds")
        useful_values = [
            average(v[metric])
            for framework, by_sleep in data.items()
            if framework != "taskiq"
            for v in by_sleep.values()
        ]
        if useful_values:
            ax.set_ylim(0, max(useful_values) * 1.22)

        if overflow_series:
            plot_series(
                overflow_ax,
                overflow_series,
                title.replace(" latency", " overflow"),
                show_std=True,
                annotate_end=True,
                annotate_frameworks={"taskiq"},
            )
            apply_axis_style(overflow_ax, sleep_times, "Milliseconds")
            overflow_ax.tick_params(axis="both", labelsize=8)
            overflow_ax.title.set_fontsize(9)
            overflow_ax.title.set_color("#64748B")
        else:
            overflow_ax.axis("off")

    handles = []
    labels = []
    for framework in sorted(data, key=framework_sort_key):
        style = FRAMEWORK_STYLES.get(framework, {})
        handles.append(
            plt.Line2D(
                [],
                [],
                color=style.get("color", "#64748B"),
                marker=style.get("marker", "o"),
                linestyle=style.get("ls", "-"),
                linewidth=style.get("lw", 2.1),
                markersize=6,
                markeredgecolor="white",
            )
        )
        labels.append(FRAMEWORK_DISPLAY.get(framework, framework))
    fig.legend(
        handles,
        labels,
        loc="center right",
        bbox_to_anchor=(0.985, 0.5),
        frameon=False,
        fontsize=10,
    )
    fig.suptitle(
        "Latency Tradeoff by Framework",
        x=0.06,
        y=0.965,
        ha="left",
        fontsize=20,
        fontweight="bold",
        color="#0F172A",
    )
    fig.text(
        0.06,
        0.915,
        "Throughput and latency from the same instrumented runs. Higher throughput is better; lower latency is better. Overflow panels show the full latency scale.",
        ha="left",
        va="top",
        fontsize=10.5,
        color="#64748B",
    )

    output = output_dir / "latency_tradeoff.svg"
    fig.savefig(output, format="svg")
    plt.close(fig)
    return output


def plot_latency_tradeoff_scatter(
    data: dict[str, dict[float, dict[str, list[float]]]], output_dir: Path
) -> Path:
    fig, ax = plt.subplots(figsize=(11.5, 7.4))
    fig.subplots_adjust(left=0.1, right=0.78, top=0.8, bottom=0.13)
    point_labels = []
    point_positions = []

    for framework in sorted(data, key=framework_sort_key):
        style = FRAMEWORK_STYLES.get(framework, {})
        color = style.get("color", "#64748B")
        xs = []
        ys = []
        labels = []
        for sleep_time in sorted(data[framework]):
            values = data[framework][sleep_time]
            xs.append(average(values["p95_ms"]))
            ys.append(average(values["throughput_msg_per_sec"]))
            labels.append(fmt_sleep(sleep_time))
            point_positions.append((xs[-1], ys[-1]))
        ax.scatter(
            xs,
            ys,
            label=FRAMEWORK_DISPLAY.get(framework, framework),
            color=color,
            marker=style.get("marker", "o"),
            s=58 if framework != "repid" else 78,
            edgecolor="white",
            linewidth=1.2,
            zorder=style.get("zorder", 1),
        )
        for x, y, label in zip(xs, ys, labels):
            point_labels.append(ax.annotate(
                label,
                xy=(x, y),
                xytext=(5, 4),
                textcoords="offset points",
                fontsize=7.5,
                color=color,
                alpha=0.85,
            ))

    ax.set_xscale("log")
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(fmt_compact))
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(fmt_compact))
    ax.grid(True, axis="both", linestyle="--", alpha=0.55)
    ax.set_xlabel("p95 latency (ms, log scale)")
    ax.set_ylabel("Throughput (messages / second)")
    corner_labels = [
        (0.02, 0.97, "higher throughput\nlower latency", "left", "top"),
        (0.98, 0.97, "higher throughput\nhigher latency", "right", "top"),
        (0.02, 0.03, "lower throughput\nlower latency", "left", "bottom"),
        (0.98, 0.03, "lower throughput\nhigher latency", "right", "bottom"),
    ]
    for x, y, label, ha, va in corner_labels:
        point_labels.append(ax.text(
            x,
            y,
            label,
            transform=ax.transAxes,
            ha=ha,
            va=va,
            fontsize=8.5,
            color="#475569",
            alpha=0.42,
            fontweight="bold",
        ))

    handles, labels = ax.get_legend_handles_labels()
    ax.legend(
        handles,
        labels,
        loc="center left",
        bbox_to_anchor=(1.02, 0.5),
        frameon=False,
        fontsize=9.5,
    )
    fig.text(
        0.1,
        0.965,
        "Latency Throughput Tradeoff",
        ha="left",
        va="top",
        fontsize=18,
        fontweight="bold",
        color="#0F172A",
    )
    fig.text(
        0.1,
        0.915,
        "Each point is one task duration from the latency-instrumented benchmark. Upper-left is best.",
        ha="left",
        va="top",
        fontsize=10,
        color="#64748B",
    )

    fig.canvas.draw()
    renderer = fig.canvas.get_renderer()
    static_obstacles = [
        label.get_window_extent(renderer).expanded(1.04, 1.08)
        for label in point_labels[-len(corner_labels) :]
    ]
    for x, y in point_positions:
        px, py = ax.transData.transform((x, y))
        static_obstacles.append(Bbox.from_bounds(px - 7, py - 7, 14, 14))

    sleep_labels = point_labels[: -len(corner_labels)]
    placed = []
    candidate_offsets = [
        (6, 5),
        (6, -13),
        (-24, 5),
        (-24, -13),
        (12, 16),
        (-32, 16),
        (12, -25),
        (-32, -25),
        (0, 28),
        (0, -34),
        (34, 0),
        (-48, 0),
    ]
    for label in sleep_labels:
        original = label.get_position()
        chosen = original
        for offset in candidate_offsets:
            label.set_position(offset)
            fig.canvas.draw()
            box = label.get_window_extent(renderer).expanded(1.08, 1.15)
            if not any(box.overlaps(obstacle) for obstacle in static_obstacles + placed):
                placed.append(box)
                chosen = offset
                break
        else:
            label.set_position(original)
            fig.canvas.draw()
            placed.append(label.get_window_extent(renderer).expanded(1.08, 1.15))
        if chosen != original:
            label.arrow_patch = ax.annotate(
                "",
                xy=label.xy,
                xytext=chosen,
                textcoords="offset points",
                arrowprops={"arrowstyle": "-", "color": label.get_color(), "lw": 0.65, "alpha": 0.45},
                zorder=label.get_zorder() - 1,
            ).arrow_patch

    output = output_dir / "latency_tradeoff_scatter.svg"
    fig.savefig(output, format="svg")
    plt.close(fig)
    return output


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--benchmarks-csv", type=Path, default=BENCHMARKS_CSV)
    parser.add_argument("--latency-csv", type=Path, default=LATENCY_CSV)
    parser.add_argument("--output-dir", type=Path, default=OUTPUT_DIR)
    parser.add_argument(
        "--no-show",
        action="store_true",
        help="Kept for non-interactive compatibility; charts are always written to files.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    outputs: list[Path] = []
    throughput = load_throughput(args.benchmarks_csv)
    if throughput:
        for category in sorted(throughput, key=category_sort_key):
            if category == "latency":
                continue
            outputs.append(plot_throughput_category(category, throughput[category], args.output_dir))

    if args.latency_csv.exists():
        latency = load_latency(args.latency_csv)
        if latency:
            outputs.append(plot_latency_tradeoff(latency, args.output_dir))
            outputs.append(plot_latency_tradeoff_scatter(latency, args.output_dir))

    print("Generated charts:")
    for output in outputs:
        print(f"  {output.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
