//! Summary table generator for benchmark results

use anyhow::{Context, Result};
use clap::Parser;
use serde::Deserialize;
use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "summary")]
#[command(about = "Generate summary tables from benchmark results")]
struct Args {
    /// Input JSON file
    #[arg(default_value = "data/benchmark_otel_zstd9_iter3.json")]
    input: PathBuf,

    /// Batch size to display
    #[arg(long, default_value = "50000")]
    batch_size: usize,
}

#[derive(Debug, Deserialize)]
struct BenchmarkSuite {
    results: Vec<BenchmarkResult>,
}

#[derive(Debug, Deserialize)]
struct BenchmarkResult {
    dataset: String,
    batch_size: usize,
    compressor: String,
    #[allow(dead_code)]
    num_payloads: usize,
    total_data_points: usize,
    total_uncompressed_bytes: usize,
    zstd: CompressionResult,
    openzl: Option<CompressionResult>,
}

#[derive(Debug, Deserialize)]
struct CompressionResult {
    total_bytes: usize,
    compression_ratio: f64,
}

struct TableRow {
    format_name: String,
    uncompressed_bytes: usize,
    uncompressed_ratio: f64,
    uncompressed_bytes_per_pt: f64,
    zstd_bytes: usize,
    zstd_ratio: f64,
    zstd_bytes_per_pt: f64,
    openzl_bytes: Option<usize>,
    openzl_ratio: Option<f64>,
    openzl_bytes_per_pt: Option<f64>,
}

fn main() -> Result<()> {
    let args = Args::parse();

    let content = std::fs::read_to_string(&args.input)
        .with_context(|| format!("Failed to read {}", args.input.display()))?;

    let suite: BenchmarkSuite = serde_json::from_str(&content)
        .with_context(|| "Failed to parse JSON")?;

    // Group results by (dataset, batch_size, compressor)
    let mut results_map: HashMap<(String, usize, String), &BenchmarkResult> = HashMap::new();
    for result in &suite.results {
        results_map.insert(
            (result.dataset.clone(), result.batch_size, result.compressor.clone()),
            result,
        );
    }

    // Define the 4 tables we want
    let tables = [
        ("Hipstershop Metrics", "hipstershop-otelmetrics", "otlp_metrics"),
        ("Hipstershop Traces", "hipstershop-oteltraces", "otlp_traces"),
        ("Astronomy Metrics", "astronomy-otelmetrics", "otlp_metrics"),
        ("Astronomy Traces", "astronomy-oteltraces", "otlp_traces"),
    ];

    for (title, dataset, otlp_compressor) in tables {
        println!();
        println!("{} (Batch = {})", title, args.batch_size);
        println!("{}", "=".repeat(120));

        // Print header
        println!(
            "{:<20} {:>15} {:>8} {:>12}   {:>15} {:>8} {:>12}   {:>15} {:>8} {:>12}",
            "",
            "Uncompressed", "", "",
            "Zstd", "", "",
            "OpenZL", "", ""
        );
        println!(
            "{:<20} {:>15} {:>8} {:>12}   {:>15} {:>8} {:>12}   {:>15} {:>8} {:>12}",
            "Format",
            "Bytes", "Ratio", "Bytes/pt",
            "Bytes", "Ratio", "Bytes/pt",
            "Bytes", "Ratio", "Bytes/pt"
        );
        println!("{}", "-".repeat(120));

        // Get OTLP row
        if let Some(otlp) = results_map.get(&(dataset.to_string(), args.batch_size, otlp_compressor.to_string())) {
            let row = build_row("OTLP", otlp);
            print_row(&row);
        }

        // Get OTAP row
        if let Some(otap) = results_map.get(&(dataset.to_string(), args.batch_size, "otap".to_string())) {
            // For OTAP, we need the OTLP baseline for proper ratio calculation
            let otlp_baseline = results_map
                .get(&(dataset.to_string(), args.batch_size, otlp_compressor.to_string()))
                .map(|r| r.total_uncompressed_bytes);

            let row = build_row_with_baseline("OTAP", otap, otlp_baseline);
            print_row(&row);
        }

        // Get OTLPdict row
        let dict_compressor = if otlp_compressor == "otlp_metrics" {
            "otlpmetricsdict"
        } else {
            "otlptracesdict"
        };

        if let Some(dict) = results_map.get(&(dataset.to_string(), args.batch_size, dict_compressor.to_string())) {
            // For dict, we need the OTLP baseline for proper ratio calculation
            let otlp_baseline = results_map
                .get(&(dataset.to_string(), args.batch_size, otlp_compressor.to_string()))
                .map(|r| r.total_uncompressed_bytes);

            let row = build_row_with_baseline("OTLPdict", dict, otlp_baseline);
            print_row(&row);
        }
    }

    // TPC-H tables
    let tpch_tables = [
        ("TPC-H LineItem", "tpch-lineitem"),
        ("TPC-H Orders", "tpch-orders"),
    ];

    for (title, dataset) in tpch_tables {
        // Check if we have any results for this dataset
        let has_results = results_map.keys().any(|(d, bs, _)| d == dataset && *bs == args.batch_size);
        if !has_results {
            continue;
        }

        println!();
        println!("{} (Batch = {})", title, args.batch_size);
        println!("{}", "=".repeat(120));

        // Print header
        println!(
            "{:<20} {:>15} {:>8} {:>12}   {:>15} {:>8} {:>12}   {:>15} {:>8} {:>12}",
            "",
            "Uncompressed", "", "",
            "Zstd", "", "",
            "OpenZL", "", ""
        );
        println!(
            "{:<20} {:>15} {:>8} {:>12}   {:>15} {:>8} {:>12}   {:>15} {:>8} {:>12}",
            "Format",
            "Bytes", "Ratio", "Bytes/row",
            "Bytes", "Ratio", "Bytes/row",
            "Bytes", "Ratio", "Bytes/row"
        );
        println!("{}", "-".repeat(120));

        // Get Proto row (baseline)
        if let Some(proto) = results_map.get(&(dataset.to_string(), args.batch_size, "tpch_proto".to_string())) {
            let row = build_row("Proto", proto);
            print_row(&row);
        }

        // Get Arrow row
        if let Some(arrow) = results_map.get(&(dataset.to_string(), args.batch_size, "arrow".to_string())) {
            let proto_baseline = results_map
                .get(&(dataset.to_string(), args.batch_size, "tpch_proto".to_string()))
                .map(|r| r.total_uncompressed_bytes);

            let row = build_row_with_baseline("Arrow", arrow, proto_baseline);
            print_row(&row);
        }
    }

    println!();
    Ok(())
}

fn build_row(format_name: &str, result: &BenchmarkResult) -> TableRow {
    let total_data_points = result.total_data_points as f64;

    TableRow {
        format_name: format_name.to_string(),
        uncompressed_bytes: result.total_uncompressed_bytes,
        uncompressed_ratio: 1.0,
        uncompressed_bytes_per_pt: result.total_uncompressed_bytes as f64 / total_data_points,
        zstd_bytes: result.zstd.total_bytes,
        zstd_ratio: result.zstd.compression_ratio,
        zstd_bytes_per_pt: result.zstd.total_bytes as f64 / total_data_points,
        openzl_bytes: result.openzl.as_ref().map(|o| o.total_bytes),
        openzl_ratio: result.openzl.as_ref().map(|o| o.compression_ratio),
        openzl_bytes_per_pt: result.openzl.as_ref().map(|o| o.total_bytes as f64 / total_data_points),
    }
}

fn build_row_with_baseline(format_name: &str, result: &BenchmarkResult, otlp_baseline: Option<usize>) -> TableRow {
    let total_data_points = result.total_data_points as f64;
    let baseline = otlp_baseline.unwrap_or(result.total_uncompressed_bytes) as f64;

    TableRow {
        format_name: format_name.to_string(),
        uncompressed_bytes: result.total_uncompressed_bytes,
        uncompressed_ratio: baseline / result.total_uncompressed_bytes as f64,
        uncompressed_bytes_per_pt: result.total_uncompressed_bytes as f64 / total_data_points,
        zstd_bytes: result.zstd.total_bytes,
        zstd_ratio: baseline / result.zstd.total_bytes as f64,
        zstd_bytes_per_pt: result.zstd.total_bytes as f64 / total_data_points,
        openzl_bytes: result.openzl.as_ref().map(|o| o.total_bytes),
        openzl_ratio: result.openzl.as_ref().map(|o| baseline / o.total_bytes as f64),
        openzl_bytes_per_pt: result.openzl.as_ref().map(|o| o.total_bytes as f64 / total_data_points),
    }
}

fn print_row(row: &TableRow) {
    let openzl_bytes = row.openzl_bytes
        .map(|b| format!("{:>15}", format_bytes(b)))
        .unwrap_or_else(|| format!("{:>15}", "-"));
    let openzl_ratio = row.openzl_ratio
        .map(|r| format!("{:>7.2}x", r))
        .unwrap_or_else(|| format!("{:>8}", "-"));
    let openzl_bpt = row.openzl_bytes_per_pt
        .map(|b| format!("{:>12.1}", b))
        .unwrap_or_else(|| format!("{:>12}", "-"));

    println!(
        "{:<20} {:>15} {:>7.2}x {:>12.1}   {:>15} {:>7.2}x {:>12.1}   {} {} {}",
        row.format_name,
        format_bytes(row.uncompressed_bytes),
        row.uncompressed_ratio,
        row.uncompressed_bytes_per_pt,
        format_bytes(row.zstd_bytes),
        row.zstd_ratio,
        row.zstd_bytes_per_pt,
        openzl_bytes,
        openzl_ratio,
        openzl_bpt,
    );
}

fn format_bytes(bytes: usize) -> String {
    if bytes >= 1_000_000_000 {
        format!("{:.2} GB", bytes as f64 / 1_000_000_000.0)
    } else if bytes >= 1_000_000 {
        format!("{:.2} MB", bytes as f64 / 1_000_000.0)
    } else if bytes >= 1_000 {
        format!("{:.2} KB", bytes as f64 / 1_000.0)
    } else {
        format!("{} B", bytes)
    }
}
