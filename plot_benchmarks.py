#!/usr/bin/env python3
"""Plot benchmark results — single best-in-class chart."""

import csv
from collections import defaultdict
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import matplotlib.transforms as mtransforms

CSV_PATH = Path(__file__).parent / "benchmarks_results.csv"
OUTPUT_PATH = Path(__file__).parent / "benchmarks_chart.svg"

FRAMEWORK_STYLES = {
    "repid": {"color": "#2563EB", "marker": "o", "zorder": 6, "lw": 3.0},
    "faststream": {"color": "#F59E0B", "marker": "s", "zorder": 5, "lw": 2.2},
    "dramatiq": {"color": "#10B981", "marker": "^", "zorder": 4, "lw": 2.2},
    "dramatiq_nogt": {
        "color": "#6EE7B7",
        "marker": "^",
        "zorder": 4,
        "lw": 1.8,
        "ls": "--",
    },
    "taskiq": {"color": "#EF4444", "marker": "D", "zorder": 3, "lw": 2.2},
    "celery": {"color": "#8B5CF6", "marker": "v", "zorder": 2, "lw": 2.2},
    "celery_nogt": {
        "color": "#C4B5FD",
        "marker": "v",
        "zorder": 2,
        "lw": 1.8,
        "ls": "--",
    },
}

FRAMEWORK_DISPLAY = {
    "celery_nogt": "celery w/o green threads",
    "dramatiq_nogt": "dramatiq w/o green threads",
}

CONCURRENCY = 2000
WORKERS = 8


def _theoretical_max(sleep_time: float) -> float:
    return CONCURRENCY * WORKERS * (1.0 / sleep_time)


plt.rcParams.update(
    {
        "figure.facecolor": "none",
        "axes.facecolor": "none",
        "axes.spines.top": False,
        "axes.spines.right": False,
        "axes.linewidth": 0.9,
        "axes.edgecolor": "#CBD5E1",
        "grid.color": "#EEF2F7",
        "grid.linewidth": 0.8,
        "font.family": "sans-serif",
        "font.size": 11,
        "xtick.color": "#64748B",
        "ytick.color": "#64748B",
        "axes.labelcolor": "#475569",
    }
)


def load_data(path: Path) -> dict[str, dict[float, list[float]]]:
    data: dict[str, dict[float, list[float]]] = defaultdict(lambda: defaultdict(list))
    with path.open(newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            data[row["framework"]][float(row["sleep_time"])].append(
                float(row["throughput_msg_per_sec"])
            )
    return data


def _mean(vals: list[float]) -> float:
    return sum(vals) / len(vals)


def _std(vals: list[float]) -> float:
    m = _mean(vals)
    return (sum((v - m) ** 2 for v in vals) / len(vals)) ** 0.5


def _fmt_k(x: float, _=None) -> str:
    """Compact axis labels: 31, 102, 1K, 10K, 50K."""
    if x >= 1_000:
        v = x / 1_000
        return f"{v:.0f}K" if v == int(v) else f"{v:.1f}K"
    return f"{x:.0f}"


def main() -> None:
    data = load_data(CSV_PATH)
    frameworks = sorted(data.keys(), key=lambda f: -_mean(list(data[f].values())[0]))
    sleep_times = sorted({st for fw in data.values() for st in fw})
    THEO_KEY = "__theo__"
    Y_MAX = 60_000

    # ── Figure ────────────────────────────────────────────────────────────────
    fig, ax = plt.subplots(figsize=(13, 11))
    fig.subplots_adjust(left=0.09, right=0.72, top=0.91, bottom=0.08)

    # ── Draw all series ───────────────────────────────────────────────────────
    theo_ys = [_theoretical_max(st) for st in sleep_times]

    for fw in frameworks:
        style = FRAMEWORK_STYLES.get(fw, {})
        color = style.get("color", "#888")
        xs = sorted(data[fw])
        ys = [_mean(data[fw][x]) for x in xs]
        errs = [_std(data[fw][x]) for x in xs]
        ax.fill_between(
            xs,
            [y - e for y, e in zip(ys, errs)],
            [y + e for y, e in zip(ys, errs)],
            color=color,
            alpha=0.13,
            zorder=0,
        )
        if fw == "repid":
            ax.plot(
                xs,
                ys,
                color=color,
                linewidth=12,
                alpha=0.10,
                zorder=style.get("zorder", 1) - 1,
                solid_capstyle="round",
                marker=None,
            )
        ax.plot(
            xs,
            ys,
            color=color,
            linewidth=style.get("lw", 2.2),
            linestyle=style.get("ls", "-"),
            marker=style.get("marker", "o"),
            markersize=8,
            markeredgewidth=1.8,
            markeredgecolor="white",
            zorder=style.get("zorder", 1),
            solid_capstyle="round",
        )

    ax.plot(
        sleep_times,
        theo_ys,
        color="#94A3B8",
        linewidth=1.5,
        linestyle=(0, (6, 4)),
        zorder=0,
    )

    # ── Scales and limits ─────────────────────────────────────────────────────
    ax.set_ylim(0, Y_MAX)
    ax.set_xscale("log")
    ax.set_xticks(sleep_times)
    ax.xaxis.set_major_formatter(mticker.ScalarFormatter())
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(_fmt_k))
    ax.tick_params(axis="both", labelsize=10.5)
    ax.grid(True, axis="y", linestyle="--", alpha=0.55)
    ax.grid(True, axis="x", linestyle="--", alpha=0.35)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    ax.spines["left"].set_color("#CBD5E1")
    ax.spines["bottom"].set_color("#CBD5E1")

    # ── Axes labels ───────────────────────────────────────────────────────────
    ax.set_xlabel("Sleep time per task  (seconds)", labelpad=8, fontsize=12)
    ax.set_ylabel("Throughput  (messages / second)", fontsize=12, color="#475569")

    # ── Axis height in points (needed for display-space deconfliction) ────────
    ax_h_pts = ax.get_position().height * fig.get_figheight() * 72

    # ── Deconfliction: works entirely in display points ───────────────────────
    def _dc_pts(items, base_pts=13, gap_pts=11):
        """Return {key: y_offset_pts} — offset-from-data-point for each label.

        base_pts: minimum clearance above the data point (clears 8-pt marker).
        gap_pts : minimum gap between adjacent label baselines.
        """

        def to_disp(y):
            return y / Y_MAX * ax_h_pts

        ordered = sorted(items, key=lambda t: t[1])
        pos = [to_disp(raw_y) + base_pts for _, raw_y in ordered]
        for i in range(1, len(pos)):
            if pos[i] - pos[i - 1] < gap_pts:
                pos[i] = pos[i - 1] + gap_pts
        return {fw: p - to_disp(raw_y) for (fw, raw_y), p in zip(ordered, pos)}

    def _dc_pts_centered(items, gap_pts=11):
        """Deconflict labels centered on each data point, pushing upward only."""

        def to_disp(y):
            return y / Y_MAX * ax_h_pts

        ordered = sorted(items, key=lambda t: t[1])
        pos = [to_disp(raw_y) for _, raw_y in ordered]
        for i in range(1, len(pos)):
            if pos[i] - pos[i - 1] < gap_pts:
                pos[i] = pos[i - 1] + gap_pts
        return {fw: p - to_disp(raw_y) for (fw, raw_y), p in zip(ordered, pos)}

    # ── Per-column value labels ───────────────────────────────────────────────
    for st in sleep_times:
        items = [(fw, _mean(data[fw][st])) for fw in frameworks if st in data[fw]]
        tv = _theoretical_max(st)
        if 0 <= tv <= Y_MAX:
            items.append((THEO_KEY, tv))
        if not items:
            continue
        is_last = st == max(sleep_times)
        adj = _dc_pts_centered(items) if is_last else _dc_pts(items)
        for fw, raw_y in items:
            color = (
                "#94A3B8"
                if fw == THEO_KEY
                else FRAMEWORK_STYLES.get(fw, {}).get("color", "#888")
            )
            ax.annotate(
                _fmt_k(raw_y),
                xy=(st, raw_y),
                xytext=(4, adj[fw]),
                textcoords="offset points",
                ha="left",
                va="center" if is_last else "bottom",
                fontsize=7.5,
                color=color,
                alpha=0.85,
                fontweight="bold" if fw == "repid" else "normal",
                annotation_clip=True,
            )

    # ── Right-side labels sorted/displayed by peak throughput ────────────────
    last_x = max(sleep_times)
    min_sleep = min(sleep_times)
    celery_peak = _mean(data["celery"][min_sleep])

    def _fw_terminus(fw):
        fw_xs = sorted(data[fw].keys())
        x = fw_xs[-1]
        return x, _mean(data[fw][x])

    # Each item: (fw, terminus_x, terminus_y, peak_y)
    def _peak_y(fw):
        return (
            _mean(data[fw][min_sleep]) if min_sleep in data[fw] else _fw_terminus(fw)[1]
        )

    final_items = [(fw, *_fw_terminus(fw), _peak_y(fw)) for fw in frameworks]
    theo_peak = _theoretical_max(min_sleep)
    final_items.append((THEO_KEY, last_x, _theoretical_max(last_x), theo_peak))

    gap_data = 11 / ax_h_pts * Y_MAX  # used for connector threshold
    # Sort by peak_y, space labels evenly with a fixed 16pt gap
    label_gap_pts = 16
    r_ordered = sorted(final_items, key=lambda t: t[3])
    n = len(r_ordered)
    total_pts = label_gap_pts * (n - 1)
    center_pts = 0.5 * ax_h_pts
    start_pts = center_pts - total_pts / 2
    adj_final = {
        fw: (start_pts + i * label_gap_pts) / ax_h_pts * Y_MAX
        for i, (fw, _, _, _) in enumerate(r_ordered)
    }

    conn_x = last_x * 1.025
    for fw, term_x, term_y, peak_y in final_items:
        is_theo = fw == THEO_KEY
        color = (
            "#94A3B8" if is_theo else FRAMEWORK_STYLES.get(fw, {}).get("color", "#888")
        )
        disp_y = adj_final[fw]
        # Dotted horizontal extension for series that stopped before last_x
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
        # Vertical connector from terminus to label position
        if abs(disp_y - term_y) > gap_data * 0.1:
            ax.plot(
                [conn_x, conn_x],
                [term_y, disp_y],
                color=color,
                lw=0.8,
                alpha=0.55,
                clip_on=False,
            )
        fw_label = FRAMEWORK_DISPLAY.get(fw, fw)
        if is_theo:
            label = f"  theoretical max   {_fmt_k(peak_y)}/s"
        elif fw == "celery":
            label = f"  {fw_label}   {_fmt_k(peak_y)}/s  \u00b7  baseline"
        else:
            ratio = peak_y / celery_peak
            ratio_str = f"{ratio:.1f}" if ratio < 10 else f"{ratio:.0f}"
            label = f"  {fw_label}   {_fmt_k(peak_y)}/s  \u00b7  {ratio_str}\u00d7 vs celery"
        # Text starts after the swatch (swatch: 22..52pt, text: 58pt)
        ax.annotate(
            label,
            xy=(last_x, disp_y),
            xytext=(58, 0),
            textcoords="offset points",
            fontsize=9.5,
            fontweight="bold" if fw == "repid" else "normal",
            color=color,
            va="center",
            annotation_clip=False,
        )
        # Legend swatch: 40pt line centered vertically on the label row
        style = FRAMEWORK_STYLES.get(fw, {})
        anchor_display = ax.transData.transform((last_x, disp_y))
        x0 = anchor_display[0] + 22
        x1 = x0 + 30
        y0 = y1 = anchor_display[1]
        inv = ax.transData.inverted()
        p0 = inv.transform((x0, y0))
        p1 = inv.transform((x1, y1))
        ax.plot(
            [p0[0], p1[0]],
            [p0[1], p1[1]],
            color=color,
            linewidth=style.get("lw", 2.2) if not is_theo else 1.5,
            linestyle=style.get("ls", "-") if not is_theo else (0, (6, 4)),
            marker=style.get("marker", None) if not is_theo else None,
            markersize=5,
            markeredgewidth=1.2,
            markeredgecolor="white",
            clip_on=False,
            zorder=10,
        )

    # ── Workload hints ────────────────────────────────────────────────────────
    ax.text(
        0.01,
        -0.08,
        "\u2190 lighter workload",
        ha="left",
        va="top",
        fontsize=8.5,
        color="#94A3B8",
        fontstyle="italic",
        transform=ax.transAxes,
    )
    ax.text(
        0.99,
        -0.08,
        "heavier workload \u2192",
        ha="right",
        va="top",
        fontsize=8.5,
        color="#94A3B8",
        fontstyle="italic",
        transform=ax.transAxes,
    )

    # ── Title block ───────────────────────────────────────────────────────────
    cx = (0.09 + 0.72) / 2
    fig.text(
        cx,
        0.975,
        "Task-Queue Framework Throughput Benchmark",
        ha="center",
        va="top",
        fontsize=17,
        fontweight="bold",
        color="#0F172A",
    )
    fig.text(
        cx,
        0.935,
        "Messages processed per second across I/O-bound workloads  \u00b7  mean \u00b1 \u03c3 of 5 runs  \u00b7  higher is better",
        ha="center",
        va="top",
        fontsize=10.5,
        color="#64748B",
    )
    fig.text(
        cx,
        0.012,
        "Sleep time simulates I/O-bound task duration. All frameworks tested under identical workloads.",
        ha="center",
        va="bottom",
        fontsize=8.5,
        color="#94A3B8",
    )

    fig.savefig(OUTPUT_PATH, format="svg", transparent=True)
    print(f"Chart saved to {OUTPUT_PATH}")
    plt.show()


if __name__ == "__main__":
    main()
