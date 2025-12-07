#!/usr/bin/env python3
"""
Generate presentation graphs for compression benchmarks.

Usage:
    uv run scripts/presentation_plots.py

Generates:
    - graph1_otel_compression_ratio.png
    - graph2_tpch_compression_ratio.png
    - graph3_otel_improvement_ratio.png
    - graph4_otel_speed_vs_ratio.png
"""

import json
from dataclasses import dataclass
from pathlib import Path

import matplotlib.pyplot as plt


@dataclass
class DataPoint:
    batch_size: int
    compression_ratio: float
    compression_throughput: float  # MB/s
    decompression_throughput: float  # MB/s
    # For end-to-end throughput calculation
    uncompressed_bytes: int = 0
    compression_time_ms: float = 0.0
    decompression_time_ms: float = 0.0


def load_benchmark(path: Path) -> dict:
    with open(path) as f:
        return json.load(f)


def get_otlp_baseline_bytes(
    results: list[dict], dataset: str, batch_size: int
) -> int | None:
    """Get the raw OTLP uncompressed bytes for a dataset/batch_size."""
    for r in results:
        if (
            r["dataset"] == dataset
            and r["batch_size"] == batch_size
            and r["compressor"] in ("otlp_metrics", "otlp_traces")
        ):
            return r["total_uncompressed_bytes"]
    return None


def get_proto_baseline_bytes(
    results: list[dict], dataset: str, batch_size: int
) -> int | None:
    """Get the raw Proto uncompressed bytes for a dataset/batch_size."""
    for r in results:
        if (
            r["dataset"] == dataset
            and r["batch_size"] == batch_size
            and r["compressor"] == "tpch_proto"
        ):
            return r["total_uncompressed_bytes"]
    return None


def extract_series(
    results: list[dict],
    dataset: str,
    compressor: str,
    compression_type: str,  # "zstd" or "openzl"
    baseline_fn=None,  # Function to get baseline bytes
) -> list[DataPoint]:
    """Extract a series of data points for plotting."""
    points = []

    for r in results:
        if r["dataset"] != dataset or r["compressor"] != compressor:
            continue

        comp_data = r.get(compression_type)
        if comp_data is None:
            continue

        batch_size = r["batch_size"]
        uncompressed_bytes = r["total_uncompressed_bytes"]

        # Calculate compression ratio based on baseline
        if baseline_fn:
            baseline_bytes = baseline_fn(results, dataset, batch_size)
            if baseline_bytes is None:
                continue
            compression_ratio = baseline_bytes / comp_data["total_bytes"]
        else:
            compression_ratio = comp_data["compression_ratio"]

        # Throughput in MB/s (based on format's own uncompressed size)
        compression_throughput = comp_data["compression"]["throughput_mbps"]
        decompression_throughput = comp_data["decompression"]["throughput_mbps"]

        # Timing in ms
        compression_time_ms = comp_data["compression"]["avg_ms"]
        decompression_time_ms = comp_data["decompression"]["avg_ms"]

        points.append(
            DataPoint(
                batch_size=batch_size,
                compression_ratio=compression_ratio,
                compression_throughput=compression_throughput,
                decompression_throughput=decompression_throughput,
                uncompressed_bytes=uncompressed_bytes,
                compression_time_ms=compression_time_ms,
                decompression_time_ms=decompression_time_ms,
            )
        )

    # Sort by batch size
    points.sort(key=lambda p: p.batch_size)
    return points


def plot_graph1(otel_data: dict, output_dir: Path):
    """Graph 1: astronomy-otelmetrics compression ratio comparison."""
    results = otel_data["results"]
    dataset = "astronomy-otelmetrics"

    # Extract series (all use OTLP baseline)
    otlp_zstd = extract_series(results, dataset, "otlp_metrics", "zstd")
    otlp_openzl = extract_series(results, dataset, "otlp_metrics", "openzl")
    otap_zstd = extract_series(
        results, dataset, "otap", "zstd", baseline_fn=get_otlp_baseline_bytes
    )

    fig, ax = plt.subplots(figsize=(10, 6))

    # Color scheme:
    # - OpenZL: solid lines ('-')
    # - zstd: dotted lines (':')
    # Plot OpenZL first so it appears at top of legend
    if otlp_openzl:
        ax.plot(
            [p.batch_size for p in otlp_openzl],
            [p.compression_ratio for p in otlp_openzl],
            color="red",
            marker="s",
            linestyle="-",
            label="OTLP + OpenZL",
            linewidth=2,
            markersize=8,
        )

    if otlp_zstd:
        ax.plot(
            [p.batch_size for p in otlp_zstd],
            [p.compression_ratio for p in otlp_zstd],
            color="green",
            marker="o",
            linestyle=":",
            label="OTLP + zstd",
            linewidth=2,
            markersize=8,
        )

    if otap_zstd:
        ax.plot(
            [p.batch_size for p in otap_zstd],
            [p.compression_ratio for p in otap_zstd],
            color="blue",
            marker="^",
            linestyle=":",
            label="OTAP + zstd",
            linewidth=2,
            markersize=8,
        )

    ax.set_xscale("log")
    ax.set_xlabel("Batch Size", fontsize=12)
    ax.set_ylabel("Compression Ratio (vs raw OTLP)", fontsize=12)
    ax.set_title("Astronomy OTel Metrics: Compression Ratio Comparison", fontsize=14)
    ax.legend(fontsize=11, handlelength=3)
    ax.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig(output_dir / "graph1_otel_compression_ratio.png", dpi=150)
    plt.close()
    print("Generated: graph1_otel_compression_ratio.png")


def plot_graph2(tpch_data: dict, output_dir: Path):
    """Graph 2: TPC-H LineItem compression ratio comparison."""
    results = tpch_data["results"]
    dataset = "tpch-lineitem"

    # Extract series (all use Proto baseline)
    proto_zstd = extract_series(results, dataset, "tpch_proto", "zstd")
    proto_openzl = extract_series(results, dataset, "tpch_proto", "openzl")
    arrow_zstd = extract_series(
        results, dataset, "arrow", "zstd", baseline_fn=get_proto_baseline_bytes
    )

    fig, ax = plt.subplots(figsize=(10, 6))

    # Color scheme:
    # - OpenZL: solid lines ('-')
    # - zstd: dotted lines (':')
    # Plot OpenZL first so it appears at top of legend
    if proto_openzl:
        ax.plot(
            [p.batch_size for p in proto_openzl],
            [p.compression_ratio for p in proto_openzl],
            color="red",
            marker="D",
            linestyle="-",
            label="Proto + OpenZL",
            linewidth=2,
            markersize=8,
        )

    if proto_zstd:
        ax.plot(
            [p.batch_size for p in proto_zstd],
            [p.compression_ratio for p in proto_zstd],
            color="green",
            marker="D",
            linestyle=":",
            label="Proto + zstd",
            linewidth=2,
            markersize=8,
        )

    if arrow_zstd:
        ax.plot(
            [p.batch_size for p in arrow_zstd],
            [p.compression_ratio for p in arrow_zstd],
            color="blue",
            marker="p",
            linestyle=":",
            label="Arrow + zstd",
            linewidth=2,
            markersize=8,
        )

    ax.set_xscale("log")
    ax.set_xlabel("Batch Size", fontsize=12)
    ax.set_ylabel("Compression Ratio (vs raw Proto)", fontsize=12)
    ax.set_title("TPC-H LineItem: Compression Ratio Comparison", fontsize=14)
    ax.legend(fontsize=11, handlelength=3)
    ax.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig(output_dir / "graph2_tpch_compression_ratio.png", dpi=150)
    plt.close()
    print("Generated: graph2_tpch_compression_ratio.png")


def plot_graph3(tpch_data: dict, output_dir: Path):
    """Graph 3: TPC-H LineItem OpenZL improvement ratio."""
    results = tpch_data["results"]
    dataset = "tpch-lineitem"

    # Get series for comparison
    proto_zstd = extract_series(results, dataset, "tpch_proto", "zstd")
    proto_openzl = extract_series(results, dataset, "tpch_proto", "openzl")
    arrow_zstd = extract_series(
        results, dataset, "arrow", "zstd", baseline_fn=get_proto_baseline_bytes
    )

    # Build lookup dicts
    proto_zstd_by_batch = {p.batch_size: p.compression_ratio for p in proto_zstd}
    proto_openzl_by_batch = {p.batch_size: p.compression_ratio for p in proto_openzl}
    arrow_zstd_by_batch = {p.batch_size: p.compression_ratio for p in arrow_zstd}

    # Calculate improvement: Proto + OpenZL over Proto + zstd
    batch_sizes_1 = []
    improvements_1 = []
    for bs in sorted(proto_zstd_by_batch.keys()):
        if bs in proto_openzl_by_batch:
            batch_sizes_1.append(bs)
            improvements_1.append(proto_openzl_by_batch[bs] / proto_zstd_by_batch[bs])

    # Calculate improvement: Proto + OpenZL over Arrow + zstd
    batch_sizes_2 = []
    improvements_2 = []
    for bs in sorted(arrow_zstd_by_batch.keys()):
        if bs in proto_openzl_by_batch:
            batch_sizes_2.append(bs)
            improvements_2.append(proto_openzl_by_batch[bs] / arrow_zstd_by_batch[bs])

    fig, ax = plt.subplots(figsize=(10, 6))

    # Both lines show OpenZL improvement, use solid lines
    ax.plot(
        batch_sizes_1,
        improvements_1,
        color="green",
        marker="D",
        linestyle="-",
        linewidth=2,
        markersize=8,
        label="Proto+OpenZL over Proto+zstd",
    )
    ax.plot(
        batch_sizes_2,
        improvements_2,
        color="blue",
        marker="p",
        linestyle="-",
        linewidth=2,
        markersize=8,
        label="Proto+OpenZL over Arrow+zstd",
    )
    ax.axhline(y=1.0, color="gray", linestyle="--", alpha=0.7, label="No improvement")

    ax.set_xscale("log")
    ax.set_xlabel("Batch Size", fontsize=12)
    ax.set_ylabel("Improvement Ratio", fontsize=12)
    ax.set_title("TPC-H LineItem: OpenZL Improvement Ratios", fontsize=14)
    ax.legend(fontsize=11, handlelength=3)
    ax.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig(output_dir / "graph3_tpch_improvement_ratio.png", dpi=150)
    plt.close()
    print("Generated: graph3_tpch_improvement_ratio.png")


def plot_graph4(otel_data: dict, output_dir: Path):
    """Graph 4: astronomy-otelmetrics OpenZL improvement ratio."""
    results = otel_data["results"]
    dataset = "astronomy-otelmetrics"

    # Get series for comparison
    otlp_zstd = extract_series(results, dataset, "otlp_metrics", "zstd")
    otlp_openzl = extract_series(results, dataset, "otlp_metrics", "openzl")
    otap_zstd = extract_series(
        results, dataset, "otap", "zstd", baseline_fn=get_otlp_baseline_bytes
    )

    # Build lookup dicts
    otlp_zstd_by_batch = {p.batch_size: p.compression_ratio for p in otlp_zstd}
    otlp_openzl_by_batch = {p.batch_size: p.compression_ratio for p in otlp_openzl}
    otap_zstd_by_batch = {p.batch_size: p.compression_ratio for p in otap_zstd}

    # Calculate improvement: OTLP + OpenZL over OTLP + zstd
    batch_sizes_1 = []
    improvements_1 = []
    for bs in sorted(otlp_zstd_by_batch.keys()):
        if bs in otlp_openzl_by_batch:
            batch_sizes_1.append(bs)
            improvements_1.append(otlp_openzl_by_batch[bs] / otlp_zstd_by_batch[bs])

    # Calculate improvement: OTLP + OpenZL over OTAP + zstd
    batch_sizes_2 = []
    improvements_2 = []
    for bs in sorted(otap_zstd_by_batch.keys()):
        if bs in otlp_openzl_by_batch:
            batch_sizes_2.append(bs)
            improvements_2.append(otlp_openzl_by_batch[bs] / otap_zstd_by_batch[bs])

    fig, ax = plt.subplots(figsize=(10, 6))

    # Both lines show OpenZL improvement, use solid lines
    ax.plot(
        batch_sizes_1,
        improvements_1,
        color="green",
        marker="o",
        linestyle="-",
        linewidth=2,
        markersize=8,
        label="OTLP+OpenZL over OTLP+zstd",
    )
    ax.plot(
        batch_sizes_2,
        improvements_2,
        color="blue",
        marker="^",
        linestyle="-",
        linewidth=2,
        markersize=8,
        label="OTLP+OpenZL over OTAP+zstd",
    )
    ax.axhline(y=1.0, color="gray", linestyle="--", alpha=0.7, label="No improvement")

    ax.set_xscale("log")
    ax.set_xlabel("Batch Size", fontsize=12)
    ax.set_ylabel("Improvement Ratio", fontsize=12)
    ax.set_title("Astronomy OTel Metrics: OpenZL Improvement Ratios", fontsize=14)
    ax.legend(fontsize=11, handlelength=3)
    ax.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig(output_dir / "graph4_otel_improvement_ratio.png", dpi=150)
    plt.close()
    print("Generated: graph4_otel_improvement_ratio.png")


def calc_e2e_throughput(point: DataPoint, baseline_bytes: int) -> tuple[float, float]:
    """Calculate end-to-end throughput based on OTLP baseline bytes.

    Returns (compression_throughput_mbps, decompression_throughput_mbps)
    """
    # throughput = baseline_bytes / time_ms * 1000 / 1e6 = baseline_bytes / time_ms / 1000
    comp_throughput = (
        baseline_bytes / point.compression_time_ms / 1000
        if point.compression_time_ms > 0
        else 0
    )
    decomp_throughput = (
        baseline_bytes / point.decompression_time_ms / 1000
        if point.decompression_time_ms > 0
        else 0
    )
    return comp_throughput, decomp_throughput


def plot_graph5(tpch_data: dict, output_dir: Path):
    """Graph 5: Speed vs Compression ratio tradeoff (2 subplots).

    Uses end-to-end throughput: Proto raw size / time for all formats.
    """
    results = tpch_data["results"]
    dataset = "tpch-lineitem"

    # Extract series
    proto_zstd = extract_series(results, dataset, "tpch_proto", "zstd")
    proto_openzl = extract_series(results, dataset, "tpch_proto", "openzl")
    arrow_zstd = extract_series(
        results, dataset, "arrow", "zstd", baseline_fn=get_proto_baseline_bytes
    )

    # Build Proto baseline lookup
    proto_baseline_by_batch = {p.batch_size: p.uncompressed_bytes for p in proto_zstd}

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

    # Helper to plot series with end-to-end throughput
    def plot_series(series, label, color, marker, linestyle, ax_comp, ax_decomp):
        comp_speeds = []
        decomp_speeds = []
        ratios = []
        for p in series:
            baseline = proto_baseline_by_batch.get(p.batch_size, p.uncompressed_bytes)
            comp_tp, decomp_tp = calc_e2e_throughput(p, baseline)
            comp_speeds.append(comp_tp)
            decomp_speeds.append(decomp_tp)
            ratios.append(p.compression_ratio)

        ax_comp.plot(
            comp_speeds,
            ratios,
            color=color,
            marker=marker,
            linestyle=linestyle,
            label=label,
            linewidth=2,
            markersize=8,
        )
        ax_decomp.plot(
            decomp_speeds,
            ratios,
            color=color,
            marker=marker,
            linestyle=linestyle,
            label=label,
            linewidth=2,
            markersize=8,
        )

    # Plot all series with consistent color scheme
    # Line style: OpenZL = solid ('-'), zstd = dotted (':')
    # Plot OpenZL first so it appears at top of legend
    if proto_openzl:
        plot_series(proto_openzl, "Proto + OpenZL", "red", "D", "-", ax1, ax2)
    if proto_zstd:
        plot_series(proto_zstd, "Proto + zstd", "green", "D", ":", ax1, ax2)
    if arrow_zstd:
        plot_series(arrow_zstd, "Arrow + zstd", "blue", "p", ":", ax1, ax2)

    ax1.set_xlabel("Compression Speed (MB/s, vs raw Proto)", fontsize=12)
    ax1.set_ylabel("Compression Ratio (vs raw Proto)", fontsize=12)
    ax1.set_title("Compression Speed vs Ratio", fontsize=14)
    ax1.legend(fontsize=10, handlelength=3)
    ax1.grid(True, alpha=0.3)

    ax2.set_xlabel("Decompression Speed (MB/s, vs raw Proto)", fontsize=12)
    ax2.set_ylabel("Compression Ratio (vs raw Proto)", fontsize=12)
    ax2.set_title("Decompression Speed vs Ratio", fontsize=14)
    ax2.legend(fontsize=10, handlelength=3)
    ax2.grid(True, alpha=0.3)

    plt.suptitle(
        "TPC-H LineItem: Speed vs Compression Tradeoff (End-to-End)",
        fontsize=14,
        y=1.02,
    )
    plt.tight_layout()
    plt.savefig(
        output_dir / "graph5_tpch_speed_vs_ratio.png", dpi=150, bbox_inches="tight"
    )
    plt.close()
    print("Generated: graph5_tpch_speed_vs_ratio.png")


def main():
    # Paths
    script_dir = Path(__file__).parent
    repo_root = script_dir.parent
    data_dir = repo_root / "data"
    output_dir = data_dir / "presentation_plots"
    output_dir.mkdir(exist_ok=True)

    # Load data
    otel_path = data_dir / "benchmark_otel_zstd9_iter3.json"
    tpch_path = data_dir / "benchmark_tpch_zstd9_iter3.json"

    if not otel_path.exists():
        print(f"Error: {otel_path} not found")
        return

    otel_data = load_benchmark(otel_path)
    print(f"Loaded {len(otel_data['results'])} OTel results")

    tpch_data = None
    if tpch_path.exists():
        tpch_data = load_benchmark(tpch_path)
        print(f"Loaded {len(tpch_data['results'])} TPC-H results")
    else:
        print(f"Warning: {tpch_path} not found, skipping TPC-H graphs")

    # Generate graphs
    plot_graph1(otel_data, output_dir)

    if tpch_data:
        plot_graph2(tpch_data, output_dir)
        plot_graph3(tpch_data, output_dir)

    plot_graph4(otel_data, output_dir)

    if tpch_data:
        plot_graph5(tpch_data, output_dir)

    print(f"\nAll graphs saved to: {output_dir}")


if __name__ == "__main__":
    main()
