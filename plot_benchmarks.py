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
from statistics import mean, stdev

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from matplotlib.transforms import Bbox

ROOT = Path(__file__).parent
BENCHMARKS_CSV = ROOT / "benchmarks_results.csv"
LATENCY_CSV = ROOT / "latency_results.csv"
OUTPUT_DIR = ROOT / "benchmark_charts"

KNOWN_CATEGORIES = ("hc", "cpu", "burst", "latency", "steady")

CATEGORY_DISPLAY = {
    "base": "Base I/O-bound",
    "hc": "High concurrency",
    "cpu": "CPU-bound",
    "burst": "Bursty load",
    "latency": "Latency-instrumented",
    "steady": "Steady-state windowed",
}

CATEGORY_NOTES = {
    "base": "Default queue-drain benchmark.",
    "hc": "High worker concurrency; higher throughput is better.",
    "cpu": "CPU-bound task duration; higher throughput is better.",
    "burst": "Burst recovery benchmark; recovery time is primary, throughput is secondary.",
    "latency": "Latency-instrumented throughput; higher throughput is better.",
    "steady": "Fixed-window measurement after warmup; final drain is intentionally ignored.",
}

FRAMEWORK_ORDER = [
    "repid",
    "faststream",
    "dramatiq",
    "dramatiq_nogt",
    "taskiq",
    "celery",
    "celery_nogt",
]
FRAMEWORK_DISPLAY = {
    "repid": "repid",
    "faststream": "faststream",
    "dramatiq": "dramatiq",
    "taskiq": "taskiq",
    "celery": "celery",
    "celery_nogt": "celery no-GT",
    "dramatiq_nogt": "dramatiq no-GT",
}

CONCURRENCY = 2000
WORKERS = 8
BASE_THROUGHPUT_Y_MAX = 60_000
THEORETICAL_COLOR = "#94A3B8"
THEORETICAL_KEY = "__theoretical_max__"

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


def load_throughput(path: Path) -> tuple[CategoryData, dict[str, dict[float, int]]]:
    data: CategoryData = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    attempted: dict[str, dict[float, int]] = defaultdict(lambda: defaultdict(int))
    with path.open(newline="") as f:
        reader = csv.DictReader(f)
        required = {"framework", "sleep_time", "throughput_msg_per_sec"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"{path.name} is missing columns: {', '.join(sorted(missing))}")
        for row in reader:
            framework, category = split_framework(row["framework"])
            sleep_time = float(row["sleep_time"])
            status = row.get("status", "ok")
            attempted[framework][sleep_time] += 1
            if status == "ok" and row.get("throughput_msg_per_sec"):
                throughput = float(row["throughput_msg_per_sec"])
                data[category][framework][sleep_time].append(throughput)
    return data, attempted


def load_tail(path: Path, metric: str = "tail_99_to_100_seconds") -> CategoryData:
    data: CategoryData = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    with path.open(newline="") as f:
        reader = csv.DictReader(f)
        required = {"framework", "sleep_time", metric}
        if not required.issubset(set(reader.fieldnames or [])):
            return data
        for row in reader:
            framework, category = split_framework(row["framework"])
            if category == "steady":
                continue
            status = row.get("status", "ok")
            value = row.get(metric, "")
            if status == "ok" and value:
                data[category][framework][float(row["sleep_time"])].append(float(value))
    return data


def load_category_metric(path: Path, metric: str, category_filter: set[str] | None = None) -> CategoryData:
    data: CategoryData = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    with path.open(newline="") as f:
        reader = csv.DictReader(f)
        required = {"framework", "sleep_time", metric}
        if not required.issubset(set(reader.fieldnames or [])):
            return data
        for row in reader:
            framework, category = split_framework(row["framework"])
            if category_filter is not None and category not in category_filter:
                continue
            status = row.get("status", "ok")
            value = row.get(metric, "")
            if status == "ok" and value:
                data[category][framework][float(row["sleep_time"])].append(float(value))
    return data


def load_latency(path: Path) -> tuple[dict[str, dict[float, dict[str, list[float]]]], dict[str, dict[float, int]]]:
    data: dict[str, dict[float, dict[str, list[float]]]] = defaultdict(
        lambda: defaultdict(lambda: defaultdict(list))
    )
    attempted: dict[str, dict[float, int]] = defaultdict(lambda: defaultdict(int))
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
            status = row.get("status", "ok")
            attempted[framework][sleep_time] += 1
            if status == "ok":
                for metric in ("throughput_msg_per_sec", "p50_ms", "p95_ms", "p99_ms"):
                    val = row.get(metric, "")
                    if val:
                        data[framework][sleep_time][metric].append(float(val))
    return data, attempted


def average(values: list[float]) -> float:
    return mean(values)


def stddev(values: list[float]) -> float:
    return stdev(values) if len(values) > 1 else 0.0


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


def theoretical_max(sleep_time: float) -> float:
    return CONCURRENCY * WORKERS * (1.0 / sleep_time)


def framework_sort_key(framework: str) -> tuple[int, str]:
    try:
        return FRAMEWORK_ORDER.index(framework), framework
    except ValueError:
        return len(FRAMEWORK_ORDER), framework


def category_sort_key(category: str) -> tuple[int, str]:
    order = ["base", "hc", "cpu", "burst", "steady", "latency"]
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


def add_point_labels(
    fig: plt.Figure,
    ax: plt.Axes,
    data: Series,
    sleep_times: list[float],
    *,
    include_theoretical: bool = False,
    label_series_end: bool = True,
) -> None:
    ymin, ymax = ax.get_ylim()
    y_range = ymax - ymin
    if y_range <= 0:
        return

    ax_h_pts = ax.get_position().height * fig.get_figheight() * 72

    def to_disp(y: float) -> float:
        return (y - ymin) / y_range * ax_h_pts

    def deconflict(
        items: list[tuple[str, float]],
        *,
        base_pts: float = 13,
        gap_pts: float = 11,
        centered: bool = False,
    ) -> dict[str, float]:
        ordered = sorted(items, key=lambda item: item[1])
        pos = [to_disp(raw_y) + (0 if centered else base_pts) for _, raw_y in ordered]
        for i in range(1, len(pos)):
            if pos[i] - pos[i - 1] < gap_pts:
                pos[i] = pos[i - 1] + gap_pts
        return {key: p - to_disp(raw_y) for (key, raw_y), p in zip(ordered, pos)}

    for sleep_time in sleep_times:
        items = [
            (framework, average(data[framework][sleep_time]))
            for framework in sorted(data, key=framework_sort_key)
            if sleep_time in data[framework]
            and (label_series_end or sleep_time != max(data[framework]))
        ]
        tv = theoretical_max(sleep_time)
        if include_theoretical and ymin <= tv <= ymax:
            items.append((THEORETICAL_KEY, tv))
        if not items:
            continue

        is_last = sleep_time == max(sleep_times)
        offsets = deconflict(items, centered=is_last)
        for key, raw_y in items:
            color = (
                THEORETICAL_COLOR
                if key == THEORETICAL_KEY
                else FRAMEWORK_STYLES.get(key, {}).get("color", "#888")
            )
            ax.annotate(
                fmt_compact(raw_y),
                xy=(sleep_time, raw_y),
                xytext=(4, offsets[key]),
                textcoords="offset points",
                ha="left",
                va="center" if is_last else "bottom",
                fontsize=7.5,
                color=color,
                alpha=0.85,
                fontweight="bold" if key == "repid" else "normal",
                annotation_clip=True,
            )


def add_base_summary_legend(
    fig: plt.Figure,
    ax: plt.Axes,
    data: Series,
    sleep_times: list[float],
) -> None:
    ymin, ymax = ax.get_ylim()
    y_range = ymax - ymin
    if y_range <= 0 or not sleep_times:
        return

    last_x = max(sleep_times)
    min_sleep = min(sleep_times)
    ax_h_pts = ax.get_position().height * fig.get_figheight() * 72

    def terminus(framework: str) -> tuple[float, float]:
        xs = sorted(data[framework])
        x = xs[-1]
        return x, average(data[framework][x])

    def peak_y(framework: str) -> float:
        if min_sleep in data[framework]:
            return average(data[framework][min_sleep])
        return terminus(framework)[1]

    final_items = [
        (framework, *terminus(framework), peak_y(framework))
        for framework in sorted(data, key=framework_sort_key)
    ]
    final_items.append(
        (THEORETICAL_KEY, last_x, theoretical_max(last_x), theoretical_max(min_sleep))
    )

    label_gap_pts = 16
    ordered = sorted(final_items, key=lambda item: item[3])
    total_pts = label_gap_pts * (len(ordered) - 1)
    start_pts = ax_h_pts / 2 - total_pts / 2
    label_y = {
        framework: ymin + (start_pts + i * label_gap_pts) / ax_h_pts * y_range
        for i, (framework, _, _, _) in enumerate(ordered)
    }
    gap_data = 11 / ax_h_pts * y_range
    conn_x = last_x * 1.025
    celery_peak = peak_y("celery") if "celery" in data else None

    fig.canvas.draw()
    for framework, term_x, term_y, peak in final_items:
        is_theoretical = framework == THEORETICAL_KEY
        color = (
            THEORETICAL_COLOR
            if is_theoretical
            else FRAMEWORK_STYLES.get(framework, {}).get("color", "#888")
        )
        disp_y = label_y[framework]

        if term_x < last_x:
            ax.plot(
                [term_x, conn_x],
                [term_y, term_y],
                color=color,
                lw=0.8,
                alpha=0.4,
                linestyle=":",
                clip_on=False,
            )
        if abs(disp_y - term_y) > gap_data * 0.1:
            ax.plot(
                [conn_x, conn_x],
                [term_y, disp_y],
                color=color,
                lw=0.8,
                alpha=0.55,
                clip_on=False,
            )

        display = FRAMEWORK_DISPLAY.get(framework, framework)
        if is_theoretical:
            label = f"  theoretical max   {fmt_compact(peak)}/s"
        elif framework == "celery":
            label = f"  {display}   {fmt_compact(peak)}/s  -  baseline"
        elif celery_peak:
            ratio = peak / celery_peak
            ratio_str = f"{ratio:.1f}" if ratio < 10 else f"{ratio:.0f}"
            label = f"  {display}   {fmt_compact(peak)}/s  -  {ratio_str}x vs celery"
        else:
            label = f"  {display}   {fmt_compact(peak)}/s"

        ax.annotate(
            label,
            xy=(last_x, disp_y),
            xytext=(58, 0),
            textcoords="offset points",
            fontsize=9.5,
            fontweight="bold" if framework == "repid" else "normal",
            color=color,
            va="center",
            annotation_clip=False,
        )

        style = FRAMEWORK_STYLES.get(framework, {})
        anchor_display = ax.transData.transform((last_x, disp_y))
        x0 = anchor_display[0] + 22
        x1 = x0 + 30
        y0 = y1 = anchor_display[1]
        p0 = ax.transData.inverted().transform((x0, y0))
        p1 = ax.transData.inverted().transform((x1, y1))
        ax.plot(
            [p0[0], p1[0]],
            [p0[1], p1[1]],
            color=color,
            linewidth=1.5 if is_theoretical else style.get("lw", 2.1),
            linestyle=(0, (6, 4)) if is_theoretical else style.get("ls", "-"),
            marker=None if is_theoretical else style.get("marker", "o"),
            markersize=5,
            markeredgewidth=1.2,
            markeredgecolor="white",
            clip_on=False,
            zorder=10,
        )


def add_base_throughput_annotations(
    fig: plt.Figure,
    ax: plt.Axes,
    data: Series,
    sleep_times: list[float],
) -> None:
    if not sleep_times:
        return

    theoretical_values = [theoretical_max(sleep_time) for sleep_time in sleep_times]
    _, ymax = ax.get_ylim()
    ax.set_ylim(0, max(ymax, min(BASE_THROUGHPUT_Y_MAX, max(theoretical_values))))
    ax.plot(
        sleep_times,
        theoretical_values,
        color=THEORETICAL_COLOR,
        linewidth=1.5,
        linestyle=(0, (6, 4)),
        zorder=0,
    )
    add_point_labels(fig, ax, data, sleep_times, include_theoretical=True)
    add_base_summary_legend(fig, ax, data, sleep_times)


def plot_throughput_category(category: str, data: Series, output_dir: Path) -> Path:
    sleep_times = sorted({sleep_time for points in data.values() for sleep_time in points})
    is_base = category == "base"
    fig, ax = plt.subplots(figsize=(13.0, 8.0) if is_base else (11.5, 7.0))
    fig.subplots_adjust(
        left=0.09,
        right=0.72 if is_base else 0.78,
        top=0.82,
        bottom=0.13,
    )

    title = f"{CATEGORY_DISPLAY.get(category, category.title())} Throughput"
    plot_series(ax, data, None, show_std=True, annotate_end=not is_base)
    apply_axis_style(ax, sleep_times, "Messages / second")
    if is_base:
        add_base_throughput_annotations(fig, ax, data, sleep_times)
    else:
        add_point_labels(fig, ax, data, sleep_times, label_series_end=False)

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

    if not is_base:
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


def plot_tail_category(category: str, data: Series, output_dir: Path) -> Path:
    sleep_times = sorted({sleep_time for points in data.values() for sleep_time in points})
    fig, ax = plt.subplots(figsize=(11.5, 7.0))
    fig.subplots_adjust(left=0.09, right=0.78, top=0.82, bottom=0.13)

    plot_series(ax, data, None, show_std=True)
    apply_axis_style(ax, sleep_times, "Seconds from 99% to 100%")
    add_point_labels(fig, ax, data, sleep_times, label_series_end=False)

    fig.text(
        0.09,
        0.965,
        f"{CATEGORY_DISPLAY.get(category, category.title())} Tail Drain",
        ha="left",
        va="top",
        fontsize=18,
        fontweight="bold",
        color="#0F172A",
    )
    fig.text(
        0.09,
        0.915,
        "Time spent processing the final 1% of completed messages. Lower is better.",
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

    output = output_dir / f"tail_99_to_100_{category}.svg"
    fig.savefig(output, format="svg")
    plt.close(fig)
    return output


def plot_recovery_category(category: str, data: Series, output_dir: Path) -> Path:
    sleep_times = sorted({sleep_time for points in data.values() for sleep_time in points})
    fig, ax = plt.subplots(figsize=(11.5, 7.0))
    fig.subplots_adjust(left=0.09, right=0.78, top=0.82, bottom=0.13)

    plot_series(ax, data, None, show_std=True)
    apply_axis_style(ax, sleep_times, "Recovery seconds")
    add_point_labels(fig, ax, data, sleep_times, label_series_end=False)

    fig.text(
        0.09,
        0.965,
        f"{CATEGORY_DISPLAY.get(category, category.title())} Recovery",
        ha="left",
        va="top",
        fontsize=18,
        fontweight="bold",
        color="#0F172A",
    )
    fig.text(
        0.09,
        0.915,
        "Time from burst publish completion to all burst messages completed. Lower is better.",
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

    output = output_dir / f"recovery_{category}.svg"
    fig.savefig(output, format="svg")
    plt.close(fig)
    return output


def latency_metric_series(
    data: dict[str, dict[float, dict[str, list[float]]]],
    metric: str,
) -> Series:
    series: Series = defaultdict(lambda: defaultdict(list))
    for framework, by_sleep in data.items():
        for sleep_time, values in by_sleep.items():
            series[framework][sleep_time] = values[metric]
    return series


def plot_latency_tradeoff(
    data: dict[str, dict[float, dict[str, list[float]]]], output_dir: Path
) -> Path:
    metrics = [("p50_ms", "p50 latency"), ("p95_ms", "p95 latency"), ("p99_ms", "p99 latency")]
    sleep_times = sorted({sleep_time for by_sleep in data.values() for sleep_time in by_sleep})
    fig = plt.figure(figsize=(17, 9.2))
    grid = fig.add_gridspec(2, 3, height_ratios=[1.25, 2.4])
    fig.subplots_adjust(left=0.06, right=0.84, top=0.82, bottom=0.1, wspace=0.24, hspace=0.42)

    throughput_ax = fig.add_subplot(grid[0, :])
    plot_series(
        throughput_ax,
        latency_metric_series(data, "throughput_msg_per_sec"),
        "Throughput from latency-instrumented runs",
        show_std=True,
    )
    apply_axis_style(throughput_ax, sleep_times, "Messages / second")
    add_point_labels(
        fig,
        throughput_ax,
        latency_metric_series(data, "throughput_msg_per_sec"),
        sleep_times,
        label_series_end=False,
    )

    for col, (metric, title) in enumerate(metrics):
        ax = fig.add_subplot(grid[1, col])
        metric_series = latency_metric_series(data, metric)

        plot_series(ax, metric_series, title, show_std=True)
        apply_axis_style(ax, sleep_times, "Milliseconds")
        useful_values = [
            average(v[metric])
            for by_sleep in data.values()
            for v in by_sleep.values()
        ]
        if useful_values:
            ax.set_ylim(0, max(useful_values) * 1.22)
        add_point_labels(fig, ax, metric_series, sleep_times, label_series_end=False)

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
        "Throughput and latency from the same instrumented runs. Higher throughput is better; lower latency is better.",
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
    throughput, throughput_attempted = load_throughput(args.benchmarks_csv)
    if throughput:
        for category in sorted(throughput, key=category_sort_key):
            if category == "latency":
                continue
            outputs.append(plot_throughput_category(category, throughput[category], args.output_dir))

    tail = load_tail(args.benchmarks_csv)
    if tail:
        for category in sorted(tail, key=category_sort_key):
            outputs.append(plot_tail_category(category, tail[category], args.output_dir))

    recovery = load_category_metric(args.benchmarks_csv, "burst_recovery_seconds", {"burst"})
    if recovery:
        for category in sorted(recovery, key=category_sort_key):
            outputs.append(plot_recovery_category(category, recovery[category], args.output_dir))

    if args.latency_csv.exists():
        latency, latency_attempted = load_latency(args.latency_csv)
        if latency:
            outputs.append(plot_latency_tradeoff(latency, args.output_dir))
            outputs.append(plot_latency_tradeoff_scatter(latency, args.output_dir))

    print("Generated charts:")
    for output in outputs:
        try:
            display_path = output.relative_to(ROOT)
        except ValueError:
            display_path = output
        print(f"  {display_path}")


if __name__ == "__main__":
    main()
