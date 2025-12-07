#!/usr/bin/env python3
"""
Generate paper figures for compression benchmarks.

Usage:
    uv run scripts/paper_plots.py

Generates:
    - compression_ratio_combined.png
    - compression_speed_combined.png
    - decompression_speed_combined.png
    - compression_time_combined.png
    - decompression_time_combined.png
"""

import json
from pathlib import Path

import matplotlib
import matplotlib.pyplot as plt

matplotlib.use("Agg")

# Paper-friendly settings
plt.rcParams.update(
    {
        "font.size": 12,
        "axes.titlesize": 14,
        "axes.labelsize": 12,
        "xtick.labelsize": 10,
        "ytick.labelsize": 10,
        "legend.fontsize": 10,
        "lines.linewidth": 2.5,
        "lines.markersize": 8,
    }
)


def load_benchmark(path: Path) -> dict:
    with open(path) as f:
        return json.load(f)


def get_series_configs_otel():
    """Series configs for OTel datasets."""
    return [
        # OpenZL first (solid lines)
        ("otlp_metrics", "openzl", "Proto + OpenZL", "#d62728", "s", "-"),
        ("otlp_traces", "openzl", "Proto + OpenZL", "#d62728", "s", "-"),
        # zstd (dotted lines)
        ("otlp_metrics", "zstd", "Proto + zstd", "#2ca02c", "o", ":"),
        ("otlp_traces", "zstd", "Proto + zstd", "#2ca02c", "o", ":"),
        # OTAP variants
        ("otap", "zstd", "Arrow + zstd", "#1f77b4", "^", ":"),
        ("otapnodict", "zstd", "Arrow (No Dict) + zstd", "#9467bd", "v", ":"),
    ]


def get_series_configs_tpch():
    """Series configs for TPC-H datasets."""
    return [
        # OpenZL first (solid lines)
        ("tpch_proto", "openzl", "Proto + OpenZL", "#d62728", "D", "-"),
        # zstd (dotted lines)
        ("tpch_proto", "zstd", "Proto + zstd", "#2ca02c", "o", ":"),
        # Arrow
        ("arrow", "zstd", "Arrow + zstd", "#1f77b4", "p", ":"),
        ("arrownodict", "zstd", "Arrow (No Dict) + zstd", "#9467bd", "v", ":"),
    ]


def get_proto_baseline(results, dataset):
    """Get Proto raw bytes lookup by batch size."""
    baseline = {}
    for r in results:
        if r["dataset"] == dataset:
            if r["compressor"] in ("otlp_metrics", "otlp_traces", "tpch_proto"):
                baseline[r["batch_size"]] = r["total_uncompressed_bytes"]
    return baseline


def extract_compression_ratio_series(results, dataset, compressor, method):
    """Extract batch sizes and compression ratios."""
    # Filter results
    filtered = [
        r for r in results if r["dataset"] == dataset and r["compressor"] == compressor
    ]
    filtered.sort(key=lambda x: x["batch_size"])

    # For OTAP/Arrow, use baseline uncompressed bytes
    if compressor in ("otap", "otapnodict", "otapdictperfile"):
        baseline = {}
        for r in results:
            if r["dataset"] == dataset and r["compressor"] in (
                "otlp_metrics",
                "otlp_traces",
            ):
                baseline[r["batch_size"]] = r["total_uncompressed_bytes"]

        batch_sizes = []
        ratios = []
        for r in filtered:
            bs = r["batch_size"]
            if bs in baseline and method in r:
                batch_sizes.append(bs)
                ratios.append(baseline[bs] / r[method]["total_bytes"])
        return batch_sizes, ratios

    elif compressor in ("arrow", "arrownodict", "arrowdictperfile"):
        baseline = {}
        for r in results:
            if r["dataset"] == dataset and r["compressor"] == "tpch_proto":
                baseline[r["batch_size"]] = r["total_uncompressed_bytes"]

        batch_sizes = []
        ratios = []
        for r in filtered:
            bs = r["batch_size"]
            if bs in baseline and method in r:
                batch_sizes.append(bs)
                ratios.append(baseline[bs] / r[method]["total_bytes"])
        return batch_sizes, ratios

    else:
        batch_sizes = []
        ratios = []
        for r in filtered:
            if method in r:
                batch_sizes.append(r["batch_size"])
                ratios.append(r[method]["compression_ratio"])
        return batch_sizes, ratios


def extract_speed_series(results, dataset, compressor, method, time_type):
    """Extract batch sizes and end-to-end speed (Proto raw size / time).

    Uses the pre-calculated throughput and scales by (baseline_bytes / format_bytes)
    to get end-to-end throughput based on Proto raw size.

    Args:
        time_type: 'compression' or 'decompression'
    Returns:
        (batch_sizes, speeds_mbps, stds_mbps)
    """
    filtered = [
        r for r in results if r["dataset"] == dataset and r["compressor"] == compressor
    ]
    filtered.sort(key=lambda x: x["batch_size"])

    # Get Proto baseline
    baseline = get_proto_baseline(results, dataset)

    batch_sizes = []
    speeds = []
    stds = []
    for r in filtered:
        bs = r["batch_size"]
        if bs not in baseline:
            continue
        if method not in r:
            continue

        # Get the pre-calculated throughput (based on format's own uncompressed size)
        throughput_mbps = r[method][time_type]["throughput_mbps"]
        throughput_std_mbps = r[method][time_type]["throughput_std_mbps"]

        # Scale to end-to-end throughput (based on Proto baseline)
        # e2e_speed = format_speed * (baseline_bytes / format_bytes)
        format_bytes = r["total_uncompressed_bytes"]
        scale = baseline[bs] / format_bytes if format_bytes > 0 else 1.0

        e2e_speed = throughput_mbps * scale
        e2e_std = throughput_std_mbps * scale

        batch_sizes.append(bs)
        speeds.append(e2e_speed)
        stds.append(e2e_std)

    return batch_sizes, speeds, stds


def extract_time_series(results, dataset, compressor, method, time_type):
    """Extract batch sizes and time with std dev.

    Args:
        time_type: 'compression' or 'decompression'
    Returns:
        (batch_sizes, times_ms, stds_ms)
    """
    filtered = [
        r for r in results if r["dataset"] == dataset and r["compressor"] == compressor
    ]
    filtered.sort(key=lambda x: x["batch_size"])

    batch_sizes = []
    times = []
    stds = []
    for r in filtered:
        if method not in r:
            continue

        time_ms = r[method][time_type]["avg_ms"]
        time_std_ms = r[method][time_type]["std_ms"]
        batch_sizes.append(r["batch_size"])
        times.append(time_ms)
        stds.append(time_std_ms)

    return batch_sizes, times, stds


def plot_dataset_compression_ratio(ax, results, dataset, configs, title):
    """Plot compression ratio for a single dataset on given axes."""
    plotted_labels = set()

    for compressor, method, label, color, marker, linestyle in configs:
        batch_sizes, ratios = extract_compression_ratio_series(
            results, dataset, compressor, method
        )
        if batch_sizes and label not in plotted_labels:
            ax.plot(
                batch_sizes,
                ratios,
                label=label,
                color=color,
                marker=marker,
                linestyle=linestyle,
                linewidth=2.5,
                markersize=8,
            )
            plotted_labels.add(label)

    ax.set_xscale("log")
    ax.set_xlabel("Batch Size")
    ax.set_ylabel("Compression Ratio")
    ax.set_title(title, fontweight="bold")
    ax.grid(True, alpha=0.3)


def plot_dataset_speed(ax, results, dataset, configs, title, time_type, show_std=True):
    """Plot speed for a single dataset on given axes.

    Args:
        show_std: If True, show shaded region for standard deviation
    """
    import numpy as np

    plotted_labels = set()

    for compressor, method, label, color, marker, linestyle in configs:
        batch_sizes, speeds, stds = extract_speed_series(
            results, dataset, compressor, method, time_type
        )
        if batch_sizes and label not in plotted_labels:
            batch_sizes = np.array(batch_sizes)
            speeds = np.array(speeds)
            stds = np.array(stds)

            # Plot main line
            ax.plot(
                batch_sizes,
                speeds,
                label=label,
                color=color,
                marker=marker,
                linestyle=linestyle,
                linewidth=2.5,
                markersize=8,
            )

            # Add shaded region for std deviation
            if show_std and len(stds) > 0:
                ax.fill_between(
                    batch_sizes, speeds - stds, speeds + stds, color=color, alpha=0.15
                )

            plotted_labels.add(label)

    ax.set_xscale("log")
    ax.set_xlabel("Batch Size")
    ax.set_ylabel(f"{time_type.capitalize()} Speed (MB/s)")
    ax.set_title(title, fontweight="bold")
    ax.grid(True, alpha=0.3)


def plot_dataset_time(ax, results, dataset, configs, title, time_type, show_std=True):
    """Plot time for a single dataset on given axes.

    Args:
        show_std: If True, show shaded region for standard deviation
    """
    import numpy as np

    plotted_labels = set()

    for compressor, method, label, color, marker, linestyle in configs:
        batch_sizes, times, stds = extract_time_series(
            results, dataset, compressor, method, time_type
        )
        if batch_sizes and label not in plotted_labels:
            batch_sizes = np.array(batch_sizes)
            times = np.array(times)
            stds = np.array(stds)

            # Plot main line
            ax.plot(
                batch_sizes,
                times,
                label=label,
                color=color,
                marker=marker,
                linestyle=linestyle,
                linewidth=2.5,
                markersize=8,
            )

            # Add shaded region for std deviation
            if show_std and len(stds) > 0:
                ax.fill_between(
                    batch_sizes, times - stds, times + stds, color=color, alpha=0.15
                )

            plotted_labels.add(label)

    ax.set_xscale("log")
    ax.set_xlabel("Batch Size")
    ax.set_ylabel(f"{time_type.capitalize()} Time (ms)")
    ax.set_title(title, fontweight="bold")
    ax.grid(True, alpha=0.3)


def create_compression_ratio_combined(
    otel_data: dict, tpch_data: dict, output_dir: Path
):
    """Create combined compression ratio figure with shared legend."""
    fig, axes = plt.subplots(1, 5, figsize=(20, 4))

    otel_results = otel_data["results"]
    tpch_results = tpch_data["results"]

    otel_configs = get_series_configs_otel()
    tpch_configs = get_series_configs_tpch()

    # Plot each dataset
    datasets = [
        (otel_results, "hipstershop-otelmetrics", otel_configs, "Hipstershop Metrics"),
        (otel_results, "hipstershop-oteltraces", otel_configs, "Hipstershop Traces"),
        (otel_results, "astronomy-otelmetrics", otel_configs, "Astronomy Metrics"),
        (otel_results, "astronomy-oteltraces", otel_configs, "Astronomy Traces"),
        (tpch_results, "tpch-lineitem", tpch_configs, "TPC-H LineItem"),
    ]

    for ax, (results, dataset, configs, title) in zip(axes, datasets):
        plot_dataset_compression_ratio(ax, results, dataset, configs, title)

    # Remove individual legends and create shared legend
    # Get handles and labels from first plot (labels are unified across all)
    handles, labels = axes[0].get_legend_handles_labels()

    # Create shared legend at the bottom
    fig.legend(
        handles,
        labels,
        loc="lower center",
        ncol=4,
        bbox_to_anchor=(0.5, -0.08),
        frameon=True,
        fontsize=11,
    )

    plt.tight_layout()
    plt.subplots_adjust(bottom=0.22)

    output_file = output_dir / "compression_ratio_combined.png"
    plt.savefig(output_file, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"Generated: {output_file}")


def create_speed_combined(
    otel_data: dict, tpch_data: dict, output_dir: Path, time_type: str
):
    """Create combined speed figure with shared legend.

    Args:
        time_type: 'compression' or 'decompression'
    """
    fig, axes = plt.subplots(1, 5, figsize=(20, 4))

    otel_results = otel_data["results"]
    tpch_results = tpch_data["results"]

    otel_configs = get_series_configs_otel()
    tpch_configs = get_series_configs_tpch()

    # Plot each dataset
    datasets = [
        (otel_results, "hipstershop-otelmetrics", otel_configs, "Hipstershop Metrics"),
        (otel_results, "hipstershop-oteltraces", otel_configs, "Hipstershop Traces"),
        (otel_results, "astronomy-otelmetrics", otel_configs, "Astronomy Metrics"),
        (otel_results, "astronomy-oteltraces", otel_configs, "Astronomy Traces"),
        (tpch_results, "tpch-lineitem", tpch_configs, "TPC-H LineItem"),
    ]

    for ax, (results, dataset, configs, title) in zip(axes, datasets):
        plot_dataset_speed(ax, results, dataset, configs, title, time_type)

    # Remove individual legends and create shared legend
    handles, labels = axes[0].get_legend_handles_labels()

    # Create shared legend at the bottom
    fig.legend(
        handles,
        labels,
        loc="lower center",
        ncol=4,
        bbox_to_anchor=(0.5, -0.08),
        frameon=True,
        fontsize=11,
    )

    plt.tight_layout()
    plt.subplots_adjust(bottom=0.22)

    output_file = output_dir / f"{time_type}_speed_combined.png"
    plt.savefig(output_file, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"Generated: {output_file}")


def create_time_combined(
    otel_data: dict, tpch_data: dict, output_dir: Path, time_type: str
):
    """Create combined time figure with shared legend.

    Args:
        time_type: 'compression' or 'decompression'
    """
    fig, axes = plt.subplots(1, 5, figsize=(20, 4))

    otel_results = otel_data["results"]
    tpch_results = tpch_data["results"]

    otel_configs = get_series_configs_otel()
    tpch_configs = get_series_configs_tpch()

    # Plot each dataset
    datasets = [
        (otel_results, "hipstershop-otelmetrics", otel_configs, "Hipstershop Metrics"),
        (otel_results, "hipstershop-oteltraces", otel_configs, "Hipstershop Traces"),
        (otel_results, "astronomy-otelmetrics", otel_configs, "Astronomy Metrics"),
        (otel_results, "astronomy-oteltraces", otel_configs, "Astronomy Traces"),
        (tpch_results, "tpch-lineitem", tpch_configs, "TPC-H LineItem"),
    ]

    for ax, (results, dataset, configs, title) in zip(axes, datasets):
        plot_dataset_time(ax, results, dataset, configs, title, time_type)

    # Remove individual legends and create shared legend
    handles, labels = axes[0].get_legend_handles_labels()

    # Create shared legend at the bottom
    fig.legend(
        handles,
        labels,
        loc="lower center",
        ncol=4,
        bbox_to_anchor=(0.5, -0.08),
        frameon=True,
        fontsize=11,
    )

    plt.tight_layout()
    plt.subplots_adjust(bottom=0.22)

    output_file = output_dir / f"{time_type}_time_combined.png"
    plt.savefig(output_file, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"Generated: {output_file}")


def main():
    script_dir = Path(__file__).parent
    repo_root = script_dir.parent
    data_dir = repo_root / "data"

    otel_json = data_dir / "benchmark_otel_zstd9_iter3.json"
    tpch_json = data_dir / "benchmark_tpch_zstd9_iter3.json"
    output_dir = data_dir / "paper_plots"
    output_dir.mkdir(exist_ok=True)

    if not otel_json.exists():
        print(f"Error: {otel_json} not found")
        return

    if not tpch_json.exists():
        print(f"Error: {tpch_json} not found")
        return

    otel_data = load_benchmark(otel_json)
    tpch_data = load_benchmark(tpch_json)
    print(f"Loaded {len(otel_data['results'])} OTel results")
    print(f"Loaded {len(tpch_data['results'])} TPC-H results")

    create_compression_ratio_combined(otel_data, tpch_data, output_dir)
    create_speed_combined(otel_data, tpch_data, output_dir, "compression")
    create_speed_combined(otel_data, tpch_data, output_dir, "decompression")
    create_time_combined(otel_data, tpch_data, output_dir, "compression")
    create_time_combined(otel_data, tpch_data, output_dir, "decompression")

    print(f"\nAll plots saved to: {output_dir}")


if __name__ == "__main__":
    main()
