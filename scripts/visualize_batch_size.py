#!/usr/bin/env python3
"""
Visualize batch size compression benchmark results.

Generates plots showing compression ratio, compression time, and decompression time
vs batch size for different compression methods (zstd, OpenZL) and formats (OTLP, OTAP).
"""

import argparse
import json
from pathlib import Path
from typing import Dict, List, Tuple

import matplotlib
import matplotlib.pyplot as plt

matplotlib.use("Agg")  # Non-interactive backend


def load_results(input_path: Path) -> List[Dict]:
    """Load benchmark results from JSON file."""
    with open(input_path) as f:
        data = json.load(f)
        if isinstance(data, dict) and "results" in data:
            return data["results"]
        return data


def group_by_dataset(results: List[Dict]) -> Dict[str, List[Dict]]:
    """Group results by dataset name."""
    grouped = {}
    for result in results:
        dataset = result["dataset"]
        if dataset not in grouped:
            grouped[dataset] = []
        grouped[dataset].append(result)
    return grouped


def extract_compression_ratio_series(
    dataset_results: List[Dict], compressor: str, method: str
) -> Tuple[List[int], List[float]]:
    """
    Extract batch sizes and compression ratios for a specific compressor/method combo.

    For OTAP, the ratio is computed as otlp_raw / otap_compressed to show
    compression relative to the original OTLP format.

    Args:
        dataset_results: All results for a dataset
        compressor: 'otap', 'otlp_metrics', or 'otlp_traces'
        method: 'zstd', 'openzl', or 'raw' (uncompressed)

    Returns:
        (batch_sizes, ratios) sorted by batch size
    """
    filtered = [r for r in dataset_results if r["compressor"] == compressor]
    filtered.sort(key=lambda x: x["batch_size"])

    # For OTAP variants and otlpdict variants, we need to use OTLP uncompressed bytes as the baseline
    if compressor in ("otap", "otapnodict", "otapdictperfile", "otlpmetricsdict", "otlptracesdict"):
        # Build lookup of OTLP uncompressed bytes by batch_size
        otlp_baseline = {}
        for r in dataset_results:
            if r["compressor"] in ("otlp_metrics", "otlp_traces"):
                otlp_baseline[r["batch_size"]] = r["total_uncompressed_bytes"]

        batch_sizes = []
        ratios = []
        for r in filtered:
            batch_size = r["batch_size"]
            if batch_size in otlp_baseline:
                batch_sizes.append(batch_size)
                if method == "raw":
                    # raw = otlp_raw / uncompressed
                    ratios.append(otlp_baseline[batch_size] / r["total_uncompressed_bytes"])
                else:
                    compressed_bytes = r[method]["total_bytes"]
                    ratios.append(otlp_baseline[batch_size] / compressed_bytes)
        return batch_sizes, ratios
    # For Arrow variants, use tpch_proto uncompressed bytes as the baseline
    elif compressor in ("arrow", "arrownodict", "arrowdictperfile"):
        proto_baseline = {}
        for r in dataset_results:
            if r["compressor"] == "tpch_proto":
                proto_baseline[r["batch_size"]] = r["total_uncompressed_bytes"]

        batch_sizes = []
        ratios = []
        for r in filtered:
            batch_size = r["batch_size"]
            if batch_size in proto_baseline:
                batch_sizes.append(batch_size)
                if method == "raw":
                    # Arrow raw = proto_raw / arrow_uncompressed
                    ratios.append(proto_baseline[batch_size] / r["total_uncompressed_bytes"])
                else:
                    compressed_bytes = r[method]["total_bytes"]
                    ratios.append(proto_baseline[batch_size] / compressed_bytes)
        return batch_sizes, ratios
    else:
        batch_sizes = [r["batch_size"] for r in filtered]
        ratios = [r[method]["compression_ratio"] for r in filtered]
        return batch_sizes, ratios


def extract_time_series(
    dataset_results: List[Dict], compressor: str, method: str, time_type: str
) -> Tuple[List[int], List[float], List[float]]:
    """
    Extract batch sizes, times, and std devs for a specific compressor/method/time combo.

    Args:
        dataset_results: All results for a dataset
        compressor: 'otap', 'otlp_metrics', or 'otlp_traces'
        method: 'zstd' or 'openzl'
        time_type: 'compression' or 'decompression'

    Returns:
        (batch_sizes, times_ms, stds_ms) sorted by batch size
    """
    filtered = [r for r in dataset_results if r["compressor"] == compressor]
    filtered.sort(key=lambda x: x["batch_size"])

    batch_sizes = [r["batch_size"] for r in filtered]
    times = [r[method][time_type]["avg_ms"] for r in filtered]
    stds = [r[method][time_type]["std_ms"] for r in filtered]

    return batch_sizes, times, stds


def extract_throughput_series(
    dataset_results: List[Dict], compressor: str, method: str, time_type: str
) -> Tuple[List[int], List[float], List[float]]:
    """
    Extract batch sizes, throughputs, and std devs.

    Args:
        dataset_results: All results for a dataset
        compressor: 'otap', 'otlp_metrics', or 'otlp_traces'
        method: 'zstd' or 'openzl'
        time_type: 'compression' or 'decompression'

    Returns:
        (batch_sizes, throughputs_mbps, stds_mbps) sorted by batch size
    """
    filtered = [r for r in dataset_results if r["compressor"] == compressor]
    filtered.sort(key=lambda x: x["batch_size"])

    batch_sizes = [r["batch_size"] for r in filtered]
    throughputs = [r[method][time_type]["throughput_mbps"] for r in filtered]
    stds = [r[method][time_type]["throughput_std_mbps"] for r in filtered]

    return batch_sizes, throughputs, stds


def extract_e2e_speed_series(
    dataset_results: List[Dict], compressor: str, method: str, time_type: str
) -> Tuple[List[int], List[float]]:
    """
    Extract batch sizes and end-to-end speed (raw baseline size / time).

    For OTAP/OTLP variants: uses raw OTLP size as baseline.
    For Arrow/Proto variants: uses raw Proto size as baseline.

    Args:
        dataset_results: All results for a dataset
        compressor: 'otap', 'otlp_metrics', etc.
        method: 'zstd' or 'openzl'
        time_type: 'compression' or 'decompression'

    Returns:
        (batch_sizes, speeds_mbps) sorted by batch size
    """
    filtered = [r for r in dataset_results if r["compressor"] == compressor]
    filtered.sort(key=lambda x: x["batch_size"])

    # Determine baseline type
    is_tpch = compressor in ("tpch_proto", "arrow", "arrownodict", "arrowdictperfile")

    # Build baseline lookup
    baseline_by_batch = {}
    for r in dataset_results:
        if is_tpch:
            if r["compressor"] == "tpch_proto":
                baseline_by_batch[r["batch_size"]] = r["total_uncompressed_bytes"]
        else:
            if r["compressor"] in ("otlp_metrics", "otlp_traces"):
                baseline_by_batch[r["batch_size"]] = r["total_uncompressed_bytes"]

    batch_sizes = []
    speeds = []
    for r in filtered:
        batch_size = r["batch_size"]
        if batch_size not in baseline_by_batch:
            # For baseline formats (otlp_metrics, otlp_traces, tpch_proto), use own size
            if compressor in ("otlp_metrics", "otlp_traces", "tpch_proto"):
                baseline_bytes = r["total_uncompressed_bytes"]
            else:
                continue
        else:
            baseline_bytes = baseline_by_batch[batch_size]

        time_ms = r[method][time_type]["avg_ms"]
        if time_ms > 0:
            # speed = baseline_bytes / time_ms / 1000 to get MB/s
            speed_mbps = baseline_bytes / time_ms / 1000
            batch_sizes.append(batch_size)
            speeds.append(speed_mbps)

    return batch_sizes, speeds


def get_series_configs():
    """
    Return series configurations for plotting time/throughput.

    Each config: (compressor, method, label, color, marker, linestyle)
    Note: OTAP variants (nodict, dictperfile) only show zstd, not OpenZL.
    Note: TPC-H Arrow variants only show zstd, not OpenZL.

    Color scheme (unified across OTAP/Arrow):
    - Native (incremental dict): blue
    - nodict: purple
    - dictperfile: cyan
    Line style: OpenZL = solid ('-'), others = dotted (':')
    """
    return [
        # OpenZL first (solid lines) - appears at top of legend
        ("otlp_metrics", "openzl", "OTLP + OpenZL", "#d62728", "s", "-"),  # red
        ("otlp_traces", "openzl", "OTLP + OpenZL", "#d62728", "s", "-"),
        ("otlpmetricsdict", "openzl", "OTLP (dict) + OpenZL", "#ff7f0e", "h", "-"),  # orange
        ("otlptracesdict", "openzl", "OTLP (dict) + OpenZL", "#ff7f0e", "h", "-"),
        ("tpch_proto", "openzl", "Proto + OpenZL", "#d62728", "D", "-"),
        # zstd (dotted lines)
        ("otlp_metrics", "zstd", "OTLP + zstd", "#2ca02c", "o", ":"),  # green
        ("otlp_traces", "zstd", "OTLP + zstd", "#2ca02c", "o", ":"),
        ("otlpmetricsdict", "zstd", "OTLP (dict) + zstd", "#8c564b", "h", ":"),  # brown
        ("otlptracesdict", "zstd", "OTLP (dict) + zstd", "#8c564b", "h", ":"),
        ("tpch_proto", "zstd", "Proto + zstd", "#2ca02c", "D", ":"),
        # Column formats (dotted for zstd)
        ("otap", "zstd", "OTAP (delta dict) + zstd", "#1f77b4", "^", ":"),  # blue
        ("otapnodict", "zstd", "OTAP (no dict) + zstd", "#9467bd", "v", ":"),  # purple
        ("otapdictperfile", "zstd", "OTAP (dict/batch) + zstd", "#17becf", ">", ":"),  # cyan
        ("arrow", "zstd", "Arrow (delta dict) + zstd", "#1f77b4", "p", ":"),
        ("arrownodict", "zstd", "Arrow (no dict) + zstd", "#9467bd", "P", ":"),
        ("arrowdictperfile", "zstd", "Arrow (dict/batch) + zstd", "#17becf", "*", ":"),
    ]


def get_compression_ratio_series_configs():
    """
    Return series configurations for compression ratio plots.

    Each config: (compressor, method, label, color, marker, linestyle)
    method can be 'zstd', 'openzl', or 'raw' (uncompressed)
    Note: OTAP variants (nodict, dictperfile) only show zstd, not OpenZL.
    Note: TPC-H Arrow variants only show zstd, not OpenZL.

    Color scheme (unified across OTAP/Arrow):
    - Native (incremental dict): blue
    - nodict: purple
    - dictperfile: cyan
    Line style: OpenZL = solid ('-'), others = dotted (':')
    """
    return [
        # OpenZL first (solid lines) - appears at top of legend
        ("otlp_metrics", "openzl", "OTLP + OpenZL", "#d62728", "s", "-"),  # red
        ("otlp_traces", "openzl", "OTLP + OpenZL", "#d62728", "s", "-"),
        ("otlpmetricsdict", "openzl", "OTLP (dict) + OpenZL", "#ff7f0e", "h", "-"),  # orange
        ("otlptracesdict", "openzl", "OTLP (dict) + OpenZL", "#ff7f0e", "h", "-"),
        ("tpch_proto", "openzl", "Proto + OpenZL", "#d62728", "D", "-"),
        # zstd (dotted lines)
        ("otlp_metrics", "zstd", "OTLP + zstd", "#2ca02c", "o", ":"),  # green
        ("otlp_traces", "zstd", "OTLP + zstd", "#2ca02c", "o", ":"),
        ("otlpmetricsdict", "zstd", "OTLP (dict) + zstd", "#8c564b", "h", ":"),  # brown
        ("otlptracesdict", "zstd", "OTLP (dict) + zstd", "#8c564b", "h", ":"),
        ("tpch_proto", "zstd", "Proto + zstd", "#2ca02c", "D", ":"),
        # Column formats (dotted for zstd)
        ("otap", "zstd", "OTAP (delta dict) + zstd", "#1f77b4", "^", ":"),  # blue
        ("otapnodict", "zstd", "OTAP (no dict) + zstd", "#9467bd", "v", ":"),  # purple
        ("otapdictperfile", "zstd", "OTAP (dict/batch) + zstd", "#17becf", ">", ":"),  # cyan
        ("arrow", "zstd", "Arrow (delta dict) + zstd", "#1f77b4", "p", ":"),
        ("arrownodict", "zstd", "Arrow (no dict) + zstd", "#9467bd", "P", ":"),
        ("arrowdictperfile", "zstd", "Arrow (dict/batch) + zstd", "#17becf", "*", ":"),
    ]


def plot_compression_ratio(
    dataset_name: str, dataset_results: List[Dict], output_dir: Path
):
    """Create compression ratio vs batch size plot."""
    _, ax = plt.subplots(figsize=(10, 6))

    for compressor, method, label, color, marker, linestyle in get_compression_ratio_series_configs():
        batch_sizes, ratios = extract_compression_ratio_series(
            dataset_results, compressor, method
        )
        if batch_sizes:
            ax.plot(
                batch_sizes,
                ratios,
                label=label,
                color=color,
                marker=marker,
                linestyle=linestyle,
                linewidth=2,
                markersize=8,
            )

    ax.set_xlabel("Batch Size", fontsize=12)
    ax.set_ylabel("Compression Ratio", fontsize=12)
    ax.set_title(f"{dataset_name} - Compression Ratio vs Batch Size", fontsize=14)
    ax.set_xscale("log")
    ax.grid(True, alpha=0.3)
    ax.legend(loc="best", fontsize=10, handlelength=3)

    output_file = output_dir / f"{dataset_name}_compression_ratio.png"
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches="tight")
    plt.close()

    print(f"Saved: {output_file}")


def plot_time(
    dataset_name: str,
    dataset_results: List[Dict],
    output_dir: Path,
    time_type: str,
):
    """Create compression/decompression time vs batch size plot with error bars."""
    _, ax = plt.subplots(figsize=(10, 6))

    for compressor, method, label, color, marker, linestyle in get_series_configs():
        batch_sizes, times, stds = extract_time_series(
            dataset_results, compressor, method, time_type
        )
        if batch_sizes:
            ax.errorbar(
                batch_sizes,
                times,
                yerr=stds,
                label=label,
                color=color,
                marker=marker,
                linestyle=linestyle,
                linewidth=2,
                markersize=8,
                capsize=4,
            )

    ax.set_xlabel("Batch Size", fontsize=12)
    ax.set_ylabel(f"{time_type.capitalize()} Time (ms)", fontsize=12)
    ax.set_title(
        f"{dataset_name} - {time_type.capitalize()} Time vs Batch Size", fontsize=14
    )
    ax.set_xscale("log")
    ax.grid(True, alpha=0.3)
    ax.legend(loc="best", fontsize=10, handlelength=3)

    output_file = output_dir / f"{dataset_name}_{time_type}_time.png"
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches="tight")
    plt.close()

    print(f"Saved: {output_file}")


def plot_speed(
    dataset_name: str,
    dataset_results: List[Dict],
    output_dir: Path,
    time_type: str,
):
    """Create compression/decompression speed vs batch size plot."""
    _, ax = plt.subplots(figsize=(10, 6))

    for compressor, method, label, color, marker, linestyle in get_series_configs():
        batch_sizes, speeds = extract_e2e_speed_series(
            dataset_results, compressor, method, time_type
        )
        if batch_sizes:
            ax.plot(
                batch_sizes,
                speeds,
                label=label,
                color=color,
                marker=marker,
                linestyle=linestyle,
                linewidth=2,
                markersize=8,
            )

    ax.set_xlabel("Batch Size", fontsize=12)
    ax.set_ylabel(f"{time_type.capitalize()} Speed (MB/s, vs raw baseline)", fontsize=12)
    ax.set_title(
        f"{dataset_name} - {time_type.capitalize()} Speed vs Batch Size", fontsize=14
    )
    ax.set_xscale("log")
    ax.grid(True, alpha=0.3)
    ax.legend(loc="best", fontsize=10, handlelength=3)

    output_file = output_dir / f"{dataset_name}_{time_type}_speed.png"
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches="tight")
    plt.close()

    print(f"Saved: {output_file}")


def plot_throughput(
    dataset_name: str,
    dataset_results: List[Dict],
    output_dir: Path,
    time_type: str,
):
    """Create throughput vs batch size plot with error bars."""
    _, ax = plt.subplots(figsize=(10, 6))

    for compressor, method, label, color, marker, linestyle in get_series_configs():
        batch_sizes, throughputs, stds = extract_throughput_series(
            dataset_results, compressor, method, time_type
        )
        if batch_sizes:
            ax.errorbar(
                batch_sizes,
                throughputs,
                yerr=stds,
                label=label,
                color=color,
                marker=marker,
                linestyle=linestyle,
                linewidth=2,
                markersize=8,
                capsize=4,
            )

    ax.set_xlabel("Batch Size", fontsize=12)
    ax.set_ylabel(f"{time_type.capitalize()} Throughput (MB/s)", fontsize=12)
    ax.set_title(
        f"{dataset_name} - {time_type.capitalize()} Throughput vs Batch Size",
        fontsize=14,
    )
    ax.set_xscale("log")
    ax.grid(True, alpha=0.3)
    ax.legend(loc="best", fontsize=10, handlelength=3)

    output_file = output_dir / f"{dataset_name}_{time_type}_throughput.png"
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches="tight")
    plt.close()

    print(f"Saved: {output_file}")


def plot_dataset(dataset_name: str, dataset_results: List[Dict], output_dir: Path):
    """Create all plots for a single dataset."""
    plot_compression_ratio(dataset_name, dataset_results, output_dir)
    plot_time(dataset_name, dataset_results, output_dir, "compression")
    plot_time(dataset_name, dataset_results, output_dir, "decompression")
    plot_speed(dataset_name, dataset_results, output_dir, "compression")
    plot_speed(dataset_name, dataset_results, output_dir, "decompression")
    plot_throughput(dataset_name, dataset_results, output_dir, "compression")
    plot_throughput(dataset_name, dataset_results, output_dir, "decompression")


def main():
    parser = argparse.ArgumentParser(
        description="Visualize batch size compression benchmark results"
    )
    parser.add_argument(
        "input",
        nargs="?",
        default="../data/benchmark_results.json",
        help="Path to input JSON file (default: ../data/benchmark_results.json)",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Output directory for plots (default: {input}-plots)",
    )

    args = parser.parse_args()

    # Resolve paths relative to script location
    script_dir = Path(__file__).parent
    input_path = (script_dir / args.input).resolve()

    # Default output dir is {input_path}-plots (without .json extension)
    if args.output_dir is None:
        output_dir = input_path.with_suffix("").with_name(input_path.stem + "-plots")
    else:
        output_dir = (script_dir / args.output_dir).resolve()

    if not input_path.exists():
        print(f"Error: Input file not found: {input_path}")
        return 1

    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Loading results from: {input_path}")
    results = load_results(input_path)
    print(f"Loaded {len(results)} results")

    grouped = group_by_dataset(results)
    print(f"Found {len(grouped)} datasets: {list(grouped.keys())}")

    for dataset_name, dataset_results in grouped.items():
        print(f"Processing {dataset_name}...")
        plot_dataset(dataset_name, dataset_results, output_dir)

    print(f"\nAll plots saved to: {output_dir}")
    return 0


if __name__ == "__main__":
    exit(main())
