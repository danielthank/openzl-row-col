//! Benchmark suite for OpenZL compression on OpenTelemetry data
//!
//! This benchmarks compression performance using the openzl library directly
//! (no external process calls) for accurate CPU time measurements.

mod discovery;
mod openzl;
mod stats;
mod zstd;

use anyhow::{Context, Result};
use clap::Parser;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::path::PathBuf;

use crate::discovery::{
    discover_batch_directories, discover_compressors, parse_batch_dir_name, read_metadata,
    read_payloads,
};
use crate::openzl::OpenZLBenchmark;
use crate::stats::TimingStats;
use crate::zstd::ZstdBenchmark;

/// OpenZL Benchmark Suite
#[derive(Parser, Debug)]
#[command(name = "benchmark")]
#[command(about = "Benchmark OpenZL compression on OpenTelemetry data")]
struct Args {
    /// Zstd compression level (1-22)
    #[arg(long, default_value = "9")]
    zstd_level: i32,

    /// Number of iterations to run entire dataset
    #[arg(long, default_value = "3")]
    iterations: usize,

    /// Data directory containing batch directories
    #[arg(long, default_value = "data/generated/test/")]
    data_dir: PathBuf,

    /// Compressor directory containing trained.zlc files
    #[arg(long, default_value = "data")]
    compressor_dir: PathBuf,

    /// Filter datasets to benchmark: 'all', 'otel', 'tpch'
    #[arg(long, default_value = "all")]
    dataset: String,

    /// Number of threads to use for parallel benchmarking
    #[arg(long, default_value = "8")]
    threads: usize,
}

/// Check if a dataset should be included based on the filter
fn should_include_dataset(dataset_name: &str, filter: &str) -> bool {
    match filter {
        "all" => true,
        "otel" => dataset_name.contains("otel"),
        "tpch" => dataset_name.starts_with("tpch-"),
        _ => {
            eprintln!(
                "Unknown dataset filter: {}. Valid options: all, otel, tpch",
                filter
            );
            std::process::exit(1);
        }
    }
}

/// Compression result with timing statistics
#[derive(Debug, Serialize, Deserialize)]
struct CompressionResult {
    total_bytes: usize,
    compression_ratio: f64,
    compression: TimingStats,
    decompression: TimingStats,
}

/// Complete benchmark result for a dataset
#[derive(Debug, Serialize, Deserialize)]
struct BenchmarkResult {
    dataset: String,
    batch_size: usize,
    compressor: String,
    num_payloads: usize,
    total_data_points: usize,
    iterations: usize,
    total_uncompressed_bytes: usize,
    zstd_level: i32,
    zstd: CompressionResult,
    #[serde(skip_serializing_if = "Option::is_none")]
    openzl: Option<CompressionResult>,
}

#[derive(Debug, Serialize, Deserialize)]
struct BenchmarkSuite {
    results: Vec<BenchmarkResult>,
}

fn print_comparison_table(results: &[BenchmarkResult]) {
    use std::collections::HashMap;

    println!();
    println!("===== Compression Benchmarks =====");

    // Group results by (dataset, batch_size)
    let mut grouped: HashMap<(String, usize), Vec<&BenchmarkResult>> = HashMap::new();
    for result in results {
        let key = (result.dataset.clone(), result.batch_size);
        grouped.entry(key).or_default().push(result);
    }

    // Get sorted keys
    let mut keys: Vec<_> = grouped.keys().cloned().collect();
    keys.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));

    for (dataset, batch_size) in keys {
        println!();
        println!(
            "{} (batch_size={}, iterations={})",
            dataset,
            batch_size,
            results[0].iterations
        );
        println!(
            "{:<12} {:>12} {:>8} {:>12} {:>12} {:>12} {:>12}",
            "Format", "Uncomp", "Ratio", "Comp(ms)", "Decomp(ms)", "Throughput", "Std"
        );
        println!("{}", "-".repeat(84));

        let group_results = grouped.get(&(dataset.clone(), batch_size)).unwrap();

        for result in group_results {
            let format_name = match result.compressor.as_str() {
                "otlp_metrics" | "otlp_traces" => "OTLP",
                "otap" => "OTAP",
                _ => &result.compressor,
            };

            // Zstd row
            println!(
                "{:<12} {:>12} {:>8.2}x {:>9.2}±{:<5.2} {:>9.2}±{:<5.2} {:>8.1} MB/s ±{:.1}",
                format!("{} zstd", format_name),
                result.total_uncompressed_bytes,
                result.zstd.compression_ratio,
                result.zstd.compression.avg_ms,
                result.zstd.compression.std_ms,
                result.zstd.decompression.avg_ms,
                result.zstd.decompression.std_ms,
                result.zstd.compression.throughput_mbps,
                result.zstd.compression.throughput_std_mbps,
            );

            // OpenZL row (only if available)
            if let Some(ref openzl) = result.openzl {
                println!(
                    "{:<12} {:>12} {:>8.2}x {:>9.2}±{:<5.2} {:>9.2}±{:<5.2} {:>8.1} MB/s ±{:.1}",
                    format!("{} openzl", format_name),
                    result.total_uncompressed_bytes,
                    openzl.compression_ratio,
                    openzl.compression.avg_ms,
                    openzl.compression.std_ms,
                    openzl.decompression.avg_ms,
                    openzl.decompression.std_ms,
                    openzl.compression.throughput_mbps,
                    openzl.compression.throughput_std_mbps,
                );
            }
        }
    }

    println!();
}

fn main() -> Result<()> {
    let args = Args::parse();

    // Configure Rayon thread pool
    rayon::ThreadPoolBuilder::new()
        .num_threads(args.threads)
        .build_global()
        .context("Failed to initialize thread pool")?;

    println!("OpenZL Benchmark Suite");
    println!("======================");
    println!("Zstd level: {}", args.zstd_level);
    println!("Iterations: {}", args.iterations);
    println!("Threads: {}", args.threads);
    println!("Dataset filter: {}", args.dataset);
    println!();

    if !args.data_dir.exists() {
        anyhow::bail!(
            "Data directory not found: {}",
            args.data_dir.display()
        );
    }

    // Discover all batch directories
    let batch_dirs = discover_batch_directories(&args.data_dir)?;
    println!("Found {} batch directories", batch_dirs.len());

    // Discover trained compressors
    let compressors = discover_compressors(&args.compressor_dir)?;
    println!("Found {} trained compressors", compressors.len());
    println!();

    // Run benchmarks
    let dataset_filter = &args.dataset;
    let results: Vec<BenchmarkResult> = batch_dirs
        .par_iter()
        .filter(|batch_dir| {
            let dir_name = batch_dir.file_name().unwrap().to_str().unwrap();
            if let Ok(batch_info) = parse_batch_dir_name(dir_name) {
                should_include_dataset(&batch_info.dataset, dataset_filter)
            } else {
                false
            }
        })
        .filter_map(|batch_dir| {
            let dir_name = batch_dir.file_name().unwrap().to_str().unwrap();

            // Parse directory name to get dataset, format, and batch_size
            let batch_info = parse_batch_dir_name(dir_name).ok()?;

            // Check if this is a zstd-only format (OTAP, Arrow - no OpenZL)
            if let Some(format_name) = batch_info.zstd_only_format_name() {
                match run_zstd_only_benchmark(
                    batch_dir,
                    &batch_info,
                    format_name,
                    args.zstd_level,
                    args.iterations,
                ) {
                    Ok(result) => {
                        println!(
                            "[{}] {} @ batch_size={} (zstd-only)",
                            result.dataset, result.compressor, result.batch_size,
                        );
                        return Some(result);
                    }
                    Err(e) => {
                        eprintln!(
                            "Warning: Benchmark failed for {} with {}: {}",
                            batch_dir.display(),
                            format_name,
                            e
                        );
                        return None;
                    }
                }
            }

            // Proto formats: run both zstd and OpenZL
            let compressor_name = batch_info.compressor_name()?;

            // Get the trained compressor file to load (e.g., all OTAP variants use "otap")
            let trained_compressor = batch_info.trained_compressor_name()?;
            let compressor_path = compressors
                .iter()
                .find(|(name, _)| name == trained_compressor)
                .map(|(_, path)| path)?;

            match run_benchmark(
                batch_dir,
                &batch_info,
                compressor_name,
                compressor_path,
                args.zstd_level,
                args.iterations,
            ) {
                Ok(result) => {
                    println!(
                        "[{}] {} @ batch_size={}",
                        result.dataset, result.compressor, result.batch_size,
                    );
                    Some(result)
                }
                Err(e) => {
                    eprintln!(
                        "Warning: Benchmark failed for {} with {}: {}",
                        batch_dir.display(),
                        compressor_name,
                        e
                    );
                    None
                }
            }
        })
        .collect();

    println!();
    println!("Completed {} benchmarks", results.len());

    // Print comparison table
    print_comparison_table(&results);

    // Write results to JSON
    let suite = BenchmarkSuite { results };
    std::fs::create_dir_all("data")?;
    let output_filename = format!(
        "data/benchmark_{}_zstd{}_iter{}.json",
        args.dataset, args.zstd_level, args.iterations
    );
    let output_file = File::create(&output_filename)?;
    serde_json::to_writer_pretty(output_file, &suite)?;
    println!("Results written to {}", output_filename);

    Ok(())
}

/// Run benchmark with both zstd and OpenZL
fn run_benchmark(
    batch_dir: &std::path::Path,
    batch_info: &crate::discovery::BatchDirInfo,
    compressor_name: &str,
    compressor_path: &std::path::Path,
    zstd_level: i32,
    iterations: usize,
) -> Result<BenchmarkResult> {
    let dataset = &batch_info.dataset;
    let batch_size = batch_info.batch_size;

    // Load compressor bytes
    let compressor_bytes =
        std::fs::read(compressor_path).context("Failed to load compressor")?;

    // Read metadata for data point count
    let metadata = read_metadata(batch_dir)?;
    let total_data_points = metadata.total_data_points;

    // Read all payloads
    let payloads = read_payloads(batch_dir)?;
    let num_payloads = payloads.len();

    if num_payloads == 0 {
        anyhow::bail!("No payloads found in {}", batch_dir.display());
    }

    let total_uncompressed_bytes: usize = payloads.iter().map(|p| p.len()).sum();

    // Run Zstd benchmark (includes roundtrip verification)
    let zstd_benchmark = ZstdBenchmark::new(zstd_level);
    let zstd_result = zstd_benchmark.run(&payloads, iterations)?;

    // Run OpenZL benchmark (includes roundtrip verification)
    let mut openzl_benchmark = OpenZLBenchmark::new(&compressor_bytes, compressor_name)?;
    let openzl_result = openzl_benchmark.run(&payloads, iterations)?;

    // Calculate statistics
    let zstd_compression_ratio =
        total_uncompressed_bytes as f64 / zstd_result.total_compressed_bytes as f64;
    let openzl_compression_ratio =
        total_uncompressed_bytes as f64 / openzl_result.total_compressed_bytes as f64;

    Ok(BenchmarkResult {
        dataset: dataset.clone(),
        batch_size,
        compressor: compressor_name.to_string(),
        num_payloads,
        total_data_points,
        iterations,
        total_uncompressed_bytes,
        zstd_level,
        zstd: CompressionResult {
            total_bytes: zstd_result.total_compressed_bytes,
            compression_ratio: zstd_compression_ratio,
            compression: TimingStats::from_times(
                &zstd_result.compression_times,
                total_uncompressed_bytes,
            ),
            decompression: TimingStats::from_times(
                &zstd_result.decompression_times,
                total_uncompressed_bytes,
            ),
        },
        openzl: Some(CompressionResult {
            total_bytes: openzl_result.total_compressed_bytes,
            compression_ratio: openzl_compression_ratio,
            compression: TimingStats::from_times(
                &openzl_result.compression_times,
                total_uncompressed_bytes,
            ),
            decompression: TimingStats::from_times(
                &openzl_result.decompression_times,
                total_uncompressed_bytes,
            ),
        }),
    })
}

/// Run benchmark with zstd only (for Arrow formats without OpenZL support)
fn run_zstd_only_benchmark(
    batch_dir: &std::path::Path,
    batch_info: &crate::discovery::BatchDirInfo,
    compressor_name: &str,
    zstd_level: i32,
    iterations: usize,
) -> Result<BenchmarkResult> {
    let dataset = &batch_info.dataset;
    let batch_size = batch_info.batch_size;

    // Read metadata for data point count
    let metadata = read_metadata(batch_dir)?;
    let total_data_points = metadata.total_data_points;

    // Read all payloads
    let payloads = read_payloads(batch_dir)?;
    let num_payloads = payloads.len();

    if num_payloads == 0 {
        anyhow::bail!("No payloads found in {}", batch_dir.display());
    }

    let total_uncompressed_bytes: usize = payloads.iter().map(|p| p.len()).sum();

    // Run Zstd benchmark only
    let zstd_benchmark = ZstdBenchmark::new(zstd_level);
    let zstd_result = zstd_benchmark.run(&payloads, iterations)?;

    let zstd_compression_ratio =
        total_uncompressed_bytes as f64 / zstd_result.total_compressed_bytes as f64;

    Ok(BenchmarkResult {
        dataset: dataset.clone(),
        batch_size,
        compressor: compressor_name.to_string(),
        num_payloads,
        total_data_points,
        iterations,
        total_uncompressed_bytes,
        zstd_level,
        zstd: CompressionResult {
            total_bytes: zstd_result.total_compressed_bytes,
            compression_ratio: zstd_compression_ratio,
            compression: TimingStats::from_times(
                &zstd_result.compression_times,
                total_uncompressed_bytes,
            ),
            decompression: TimingStats::from_times(
                &zstd_result.decompression_times,
                total_uncompressed_bytes,
            ),
        },
        openzl: None,
    })
}
