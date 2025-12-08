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

# Paper-friendly settings with larger fonts
plt.rcParams.update(
    {
        "font.size": 14,
        "axes.titlesize": 16,
        "axes.labelsize": 14,
        "xtick.labelsize": 12,
        "ytick.labelsize": 12,
        "legend.fontsize": 11,
        "lines.linewidth": 2.5,
        "lines.markersize": 10,
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
        # OTAP variants: Arrow -> -delta dict -> -column dedup -> -sort -> -dict
        ("otap", "zstd", "Arrow + zstd", "#1f77b4", "^", ":"),
        ("otapdictperfile", "zstd", "Arrow (-delta dict)", "#e377c2", "P", ":"),
        ("otapnodedup", "zstd", "Arrow (-column dedup)", "#ff7f0e", "d", ":"),
        ("otapnosort", "zstd", "Arrow (-sort)", "#17becf", "x", ":"),
        ("otapnodict", "zstd", "Arrow (-dict)", "#9467bd", "v", ":"),
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
        ("arrownodict", "zstd", "Arrow (-dict)", "#9467bd", "v", ":"),
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
    if compressor in (
        "otap",
        "otapnodict",
        "otapdictperfile",
        "otapnosort",
        "otapnodedup",
    ):
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
    # Collect handles/labels from all axes to include all series
    all_handles = {}
    for ax in axes:
        handles, labels = ax.get_legend_handles_labels()
        for h, l in zip(handles, labels):
            if l not in all_handles:
                all_handles[l] = h
    handles = list(all_handles.values())
    labels = list(all_handles.keys())

    # Create shared legend at the bottom (single row)
    fig.legend(
        handles,
        labels,
        loc="lower center",
        ncol=len(labels),
        bbox_to_anchor=(0.5, -0.08),
        frameon=True,
        fontsize=11,
    )

    plt.tight_layout()
    plt.subplots_adjust(bottom=0.20)

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
    # Collect handles/labels from all axes to include all series
    all_handles = {}
    for ax in axes:
        handles, labels = ax.get_legend_handles_labels()
        for h, l in zip(handles, labels):
            if l not in all_handles:
                all_handles[l] = h
    handles = list(all_handles.values())
    labels = list(all_handles.keys())

    # Create shared legend at the bottom (single row)
    fig.legend(
        handles,
        labels,
        loc="lower center",
        ncol=len(labels),
        bbox_to_anchor=(0.5, -0.08),
        frameon=True,
        fontsize=11,
    )

    plt.tight_layout()
    plt.subplots_adjust(bottom=0.20)

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
    # Collect handles/labels from all axes to include all series
    all_handles = {}
    for ax in axes:
        handles, labels = ax.get_legend_handles_labels()
        for h, l in zip(handles, labels):
            if l not in all_handles:
                all_handles[l] = h
    handles = list(all_handles.values())
    labels = list(all_handles.keys())

    # Create shared legend at the bottom (single row)
    fig.legend(
        handles,
        labels,
        loc="lower center",
        ncol=len(labels),
        bbox_to_anchor=(0.5, -0.08),
        frameon=True,
        fontsize=11,
    )

    plt.tight_layout()
    plt.subplots_adjust(bottom=0.20)

    output_file = output_dir / f"{time_type}_time_combined.png"
    plt.savefig(output_file, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"Generated: {output_file}")


def extract_format_efficiency_series(results, dataset, compressor):
    """Extract Format Efficiency = Proto_Uncompressed / Format_Uncompressed.

    Values > 1 mean format is smaller than Proto (deduplication wins).
    Values < 1 mean format is larger than Proto (overhead wins).
    """
    baseline = get_proto_baseline(results, dataset)
    if not baseline:
        return [], []

    filtered = [
        r for r in results if r["dataset"] == dataset and r["compressor"] == compressor
    ]
    filtered.sort(key=lambda x: x["batch_size"])

    batch_sizes = []
    efficiencies = []
    for r in filtered:
        bs = r["batch_size"]
        if bs in baseline:
            efficiency = baseline[bs] / r["total_uncompressed_bytes"]
            batch_sizes.append(bs)
            efficiencies.append(efficiency)

    return batch_sizes, efficiencies


def extract_algorithm_effectiveness_series(results, dataset, compressor, method="zstd"):
    """Extract Algorithm Effectiveness = Format_Uncompressed / Compressed.

    Measures how well the compression algorithm compresses the format's data.

    Args:
        method: 'zstd' or 'openzl'
    """
    filtered = [
        r for r in results if r["dataset"] == dataset and r["compressor"] == compressor
    ]
    filtered.sort(key=lambda x: x["batch_size"])

    batch_sizes = []
    effectiveness = []
    for r in filtered:
        if method in r:
            ratio = r["total_uncompressed_bytes"] / r[method]["total_bytes"]
            batch_sizes.append(r["batch_size"])
            effectiveness.append(ratio)

    return batch_sizes, effectiveness


def extract_final_cr_series(results, dataset, compressor, method="zstd"):
    """Extract Final CR = Proto_Uncompressed / Compressed.

    This equals Format_Efficiency × Algorithm_Effectiveness.

    Args:
        method: 'zstd' or 'openzl'
    """
    baseline = get_proto_baseline(results, dataset)
    if not baseline:
        return [], []

    filtered = [
        r for r in results if r["dataset"] == dataset and r["compressor"] == compressor
    ]
    filtered.sort(key=lambda x: x["batch_size"])

    batch_sizes = []
    crs = []
    for r in filtered:
        bs = r["batch_size"]
        if bs in baseline and method in r:
            cr = baseline[bs] / r[method]["total_bytes"]
            batch_sizes.append(bs)
            crs.append(cr)

    return batch_sizes, crs


def plot_cr_decomposition(
    ax, results, dataset, configs, title, metric_type, use_log_y=True
):
    """Plot Format Efficiency, Algorithm Effectiveness, or Final CR.

    Args:
        metric_type: 'format_efficiency', 'algorithm_effectiveness', or 'final_cr'
        configs: list of (compressor, method, label, color, marker, linestyle)
    """
    plotted_labels = set()

    for compressor, method, label, color, marker, linestyle in configs:
        if metric_type == "format_efficiency":
            batch_sizes, values = extract_format_efficiency_series(
                results, dataset, compressor
            )
        elif metric_type == "algorithm_effectiveness":
            batch_sizes, values = extract_algorithm_effectiveness_series(
                results, dataset, compressor, method
            )
        elif metric_type == "final_cr":
            batch_sizes, values = extract_final_cr_series(
                results, dataset, compressor, method
            )
        else:
            continue

        if batch_sizes and label not in plotted_labels:
            ax.plot(
                batch_sizes,
                values,
                label=label,
                color=color,
                marker=marker,
                linestyle=linestyle,
                linewidth=2.5,
                markersize=8,
            )
            plotted_labels.add(label)

    ax.set_xscale("log")
    if use_log_y:
        ax.set_yscale("log")

    # Add reference line at y=1 for format efficiency
    if metric_type == "format_efficiency":
        ax.axhline(y=1.0, color="gray", linestyle="--", alpha=0.5, label="_nolegend_")

    ax.set_xlabel("Batch Size")
    ylabel = {
        "format_efficiency": "Format Efficiency\n(Proto/Format Uncomp)",
        "algorithm_effectiveness": "Algorithm Effectiveness\n(Uncomp/Compressed)",
        "final_cr": "Final Compression Ratio\n(Proto Uncomp/Compressed)",
    }[metric_type]
    ax.set_ylabel(ylabel)
    ax.set_title(title, fontweight="bold")
    ax.grid(True, alpha=0.3, which="both")


def create_cr_decomposition_combined(
    otel_data: dict, tpch_data: dict, output_dir: Path
):
    """Create figure showing compression ratio decomposition.

    Shows three rows:
    1. Format Efficiency (Proto_Uncomp / Format_Uncomp)
    2. Algorithm Effectiveness (Format_Uncomp / Compressed) - includes zstd and openzl
    3. Final CR = Format Efficiency × Algorithm Effectiveness
    """
    fig, axes = plt.subplots(3, 5, figsize=(20, 10))

    otel_results = otel_data["results"]
    tpch_results = tpch_data["results"]

    otel_configs = get_series_configs_otel()
    tpch_configs = get_series_configs_tpch()

    # Define datasets
    datasets = [
        (otel_results, "hipstershop-otelmetrics", otel_configs, "Hipstershop Metrics"),
        (otel_results, "hipstershop-oteltraces", otel_configs, "Hipstershop Traces"),
        (otel_results, "astronomy-otelmetrics", otel_configs, "Astronomy Metrics"),
        (otel_results, "astronomy-oteltraces", otel_configs, "Astronomy Traces"),
        (tpch_results, "tpch-lineitem", tpch_configs, "TPC-H LineItem"),
    ]

    # Row 0: Format Efficiency
    for i, (results, dataset, configs, title) in enumerate(datasets):
        plot_cr_decomposition(
            axes[0, i], results, dataset, configs, title, "format_efficiency"
        )

    # Row 1: Algorithm Effectiveness
    for i, (results, dataset, configs, _) in enumerate(datasets):
        plot_cr_decomposition(
            axes[1, i], results, dataset, configs, "", "algorithm_effectiveness"
        )

    # Row 2: Final CR (product)
    for i, (results, dataset, configs, _) in enumerate(datasets):
        plot_cr_decomposition(axes[2, i], results, dataset, configs, "", "final_cr")

    # Collect handles/labels from all axes
    all_handles = {}
    for row in axes:
        for ax in row:
            handles, labels = ax.get_legend_handles_labels()
            for h, l in zip(handles, labels):
                if l not in all_handles:
                    all_handles[l] = h
    handles = list(all_handles.values())
    labels = list(all_handles.keys())

    # Create shared legend at the bottom
    fig.legend(
        handles,
        labels,
        loc="lower center",
        ncol=len(labels),
        bbox_to_anchor=(0.5, -0.02),
        frameon=True,
        fontsize=10,
    )

    # Add row labels
    fig.text(
        0.02,
        0.78,
        "Format\nEfficiency",
        fontsize=11,
        fontweight="bold",
        rotation=90,
        va="center",
        ha="center",
    )
    fig.text(
        0.02,
        0.50,
        "Algorithm\nEffectiveness",
        fontsize=11,
        fontweight="bold",
        rotation=90,
        va="center",
        ha="center",
    )
    fig.text(
        0.02,
        0.22,
        "Final CR\n(Product)",
        fontsize=11,
        fontweight="bold",
        rotation=90,
        va="center",
        ha="center",
    )

    plt.tight_layout()
    plt.subplots_adjust(bottom=0.10, left=0.08, hspace=0.25)

    output_file = output_dir / "cr_decomposition_combined.png"
    plt.savefig(output_file, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"Generated: {output_file}")


def main():
    script_dir = Path(__file__).parent
    repo_root = script_dir.parent
    data_dir = repo_root / "data"

    # Try combined file first, fall back to separate files
    all_json = data_dir / "benchmark_all_zstd9_iter3.json"
    otel_json = data_dir / "benchmark_otel_zstd9_iter3.json"
    tpch_json = data_dir / "benchmark_tpch_zstd9_iter3.json"
    output_dir = data_dir / "paper_plots"
    output_dir.mkdir(exist_ok=True)

    if all_json.exists():
        # Use combined file
        all_data = load_benchmark(all_json)
        print(f"Loaded {len(all_data['results'])} results from combined file")
        otel_data = all_data
        tpch_data = all_data
    else:
        # Fall back to separate files
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
    create_cr_decomposition_combined(otel_data, tpch_data, output_dir)

    print(f"\nAll plots saved to: {output_dir}")


if __name__ == "__main__":
    main()
