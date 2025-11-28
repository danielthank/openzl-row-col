#!/usr/bin/env python3
"""
Visualize batch size compression benchmark results.

Generates plots showing compression ratio vs batch size for different
compression methods (zstd, OpenZL) and formats (OTLP, OTAP).
"""

import argparse
import json
from pathlib import Path
from typing import Callable, Dict, List

import matplotlib
import matplotlib.pyplot as plt

matplotlib.use("Agg")  # Non-interactive backend


def load_results(input_path: Path) -> List[Dict]:
    """Load benchmark results from JSON file."""
    with open(input_path) as f:
        data = json.load(f)
        # Handle both {"results": [...]} and [...] formats
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


def extract_series(
    dataset_results: List[Dict],
    format: str,
    value_fn: Callable[[Dict, float], float],
) -> tuple:
    """
    Extract batch sizes and values for a specific format.

    Args:
        dataset_results: All results for a dataset
        format: 'otlp' or 'otap'
        value_fn: Function (result, baseline_bytes) -> value

    Returns:
        (batch_sizes, values) sorted by batch size
    """

    # Filter for specific format (handle both "format" and "compressor" fields)
    def matches_format(r):
        compressor = r.get("format") or r.get("compressor", "")
        if format == "otlp":
            return compressor.startswith("otlp")
        return compressor == format

    filtered = [r for r in dataset_results if matches_format(r)]

    # Sort by batch size
    filtered.sort(key=lambda x: x["batch_size"])

    # Extract data
    batch_sizes = [r["batch_size"] for r in filtered]

    # Get OTLP baseline (uncompressed) sizes by batch_size for ratio calculations
    otlp_baseline = {}
    for r in dataset_results:
        compressor = r.get("format") or r.get("compressor", "")
        if compressor.startswith("otlp"):
            batch_size = r["batch_size"]
            otlp_baseline[batch_size] = r["total_uncompressed_bytes"]

    # Calculate values using the provided function
    values = []
    for r in filtered:
        batch_size = r["batch_size"]
        baseline_bytes = otlp_baseline.get(batch_size, r["total_uncompressed_bytes"])
        values.append(value_fn(r, baseline_bytes))

    return batch_sizes, values


def plot_compression_ratio(
    dataset_name: str, dataset_results: List[Dict], output_dir: Path
):
    """
    Create a plot for a single dataset showing compression ratios.
    """
    _, ax = plt.subplots(figsize=(10, 6))

    # Define the 5 series to plot: (format, value_fn, label, color, marker, linestyle)
    series_configs = [
        ("otap", lambda r, b: b / r["total_openzl_bytes"], "OTAP + OpenZL", "red", "D", "-"),
        ("otap", lambda r, b: b / r["total_zstd_bytes"], "OTAP + zstd", "blue", "^", "-"),
        ("otap", lambda r, b: b / r["total_uncompressed_bytes"], "OTAP raw", "gray", "x", ":"),
        ("otlp", lambda r, b: b / r["total_openzl_bytes"], "OTLP + OpenZL", "red", "s", "--"),
        ("otlp", lambda r, b: b / r["total_zstd_bytes"], "OTLP + zstd", "blue", "o", "--"),
    ]

    for format_type, value_fn, label, color, marker, linestyle in series_configs:
        batch_sizes, ratios = extract_series(dataset_results, format_type, value_fn)
        if batch_sizes:  # Only plot if we have data
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

    # Formatting
    ax.set_xlabel("Batch Size", fontsize=12)
    ax.set_ylabel("Compression Ratio", fontsize=12)
    ax.set_title(f"{dataset_name} - Compression Ratio vs Batch Size", fontsize=14)
    ax.set_xscale("log")  # Log scale for batch size
    ax.grid(True, alpha=0.3)
    ax.legend(loc="best", fontsize=10)

    # Save plot
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
    """
    Create a plot for a single dataset showing compression or decompression times.

    Args:
        dataset_name: Name of the dataset
        dataset_results: All results for the dataset
        output_dir: Directory to save plots
        time_type: 'compression' or 'decompression'
    """
    _, ax = plt.subplots(figsize=(10, 6))

    # Helper to create time extraction lambda
    def time_fn(field: str) -> Callable[[Dict, float], float]:
        return lambda r, _: r[field] * r["num_payloads"] / 1000.0

    # Define the 4 series to plot: (format, value_fn, label, color, marker, linestyle)
    series_configs = [
        ("otap", time_fn(f"avg_openzl_{time_type}_time_ms"), "OTAP + OpenZL", "red", "D", "-"),
        ("otap", time_fn(f"avg_zstd_{time_type}_time_ms"), "OTAP + zstd", "blue", "^", "-"),
        ("otlp", time_fn(f"avg_openzl_{time_type}_time_ms"), "OTLP + OpenZL", "red", "s", "--"),
        ("otlp", time_fn(f"avg_zstd_{time_type}_time_ms"), "OTLP + zstd", "blue", "o", "--"),
    ]

    for format_type, value_fn, label, color, marker, linestyle in series_configs:
        batch_sizes, times = extract_series(dataset_results, format_type, value_fn)
        if batch_sizes:  # Only plot if we have data
            ax.plot(
                batch_sizes,
                times,
                label=label,
                color=color,
                marker=marker,
                linestyle=linestyle,
                linewidth=2,
                markersize=8,
            )

    # Formatting
    ax.set_xlabel("Batch Size", fontsize=12)
    ax.set_ylabel(f"{time_type.capitalize()} Time (s)", fontsize=12)
    ax.set_title(
        f"{dataset_name} - {time_type.capitalize()} Time vs Batch Size", fontsize=14
    )
    ax.set_xscale("log")  # Log scale for batch size
    ax.grid(True, alpha=0.3)
    ax.legend(loc="best", fontsize=10)

    # Save plot
    output_file = output_dir / f"{dataset_name}_{time_type}_time.png"
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches="tight")
    plt.close()

    print(f"Saved: {output_file}")


def plot_dataset(dataset_name: str, dataset_results: List[Dict], output_dir: Path):
    """
    Create all plots for a single dataset.
    """
    plot_compression_ratio(dataset_name, dataset_results, output_dir)
    plot_time(dataset_name, dataset_results, output_dir, "compression")
    plot_time(dataset_name, dataset_results, output_dir, "decompression")


def main():
    parser = argparse.ArgumentParser(
        description="Visualize batch size compression benchmark results"
    )
    parser.add_argument(
        "input",
        nargs="?",
        default="../data/batch_size_results.json",
        help="Path to input JSON file (default: ../data/batch_size_results.json)",
    )
    parser.add_argument(
        "--output-dir",
        default="../data/plots",
        help="Output directory for plots (default: ../data/plots)",
    )

    args = parser.parse_args()

    # Resolve paths relative to script location
    script_dir = Path(__file__).parent
    input_path = (script_dir / args.input).resolve()
    output_dir = (script_dir / args.output_dir).resolve()

    # Validate input
    if not input_path.exists():
        print(f"Error: Input file not found: {input_path}")
        return 1

    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)

    # Load and process data
    print(f"Loading results from: {input_path}")
    results = load_results(input_path)

    print(f"Loaded {len(results)} results")

    # Group by dataset and plot
    grouped = group_by_dataset(results)

    print(f"Found {len(grouped)} datasets: {list(grouped.keys())}")

    for dataset_name, dataset_results in grouped.items():
        print(f"Processing {dataset_name}...")
        plot_dataset(dataset_name, dataset_results, output_dir)

    print(f"\nAll plots saved to: {output_dir}")
    return 0


if __name__ == "__main__":
    exit(main())
