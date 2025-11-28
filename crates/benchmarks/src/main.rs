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
    discover_batch_directories, discover_compressors, get_compressor_for_data,
    parse_batch_dir_name, read_payloads,
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
    #[arg(long, default_value = "4")]
    zstd_level: i32,

    /// Number of iterations to run entire dataset
    #[arg(long, default_value = "3")]
    iterations: usize,

    /// Data directory containing batch directories
    #[arg(long, default_value = "data/generated/")]
    data_dir: PathBuf,

    /// Compressor directory containing trained.zlc files
    #[arg(long, default_value = "data")]
    compressor_dir: PathBuf,
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
    iterations: usize,
    total_uncompressed_bytes: usize,
    zstd_level: i32,
    zstd: CompressionResult,
    openzl: CompressionResult,
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

            // OpenZL row
            println!(
                "{:<12} {:>12} {:>8.2}x {:>9.2}±{:<5.2} {:>9.2}±{:<5.2} {:>8.1} MB/s ±{:.1}",
                format!("{} openzl", format_name),
                result.total_uncompressed_bytes,
                result.openzl.compression_ratio,
                result.openzl.compression.avg_ms,
                result.openzl.compression.std_ms,
                result.openzl.decompression.avg_ms,
                result.openzl.decompression.std_ms,
                result.openzl.compression.throughput_mbps,
                result.openzl.compression.throughput_std_mbps,
            );
        }
    }

    println!();
}

fn main() -> Result<()> {
    let args = Args::parse();

    println!("OpenZL Benchmark Suite");
    println!("======================");
    println!("Zstd level: {}", args.zstd_level);
    println!("Iterations: {}", args.iterations);
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
    let results: Vec<BenchmarkResult> = batch_dirs
        .par_iter()
        .filter_map(|batch_dir| {
            let dir_name = batch_dir.file_name().unwrap().to_str().unwrap();

            // Choose the right compressor for this data format
            let compressor_name = get_compressor_for_data(dir_name)?;
            let compressor_path = compressors
                .iter()
                .find(|(name, _)| name == compressor_name)
                .map(|(_, path)| path)?;

            match run_benchmark(
                batch_dir,
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
    let output_file = File::create("data/benchmark_results.json")?;
    serde_json::to_writer_pretty(output_file, &suite)?;
    println!("Results written to data/benchmark_results.json");

    Ok(())
}

fn run_benchmark(
    batch_dir: &std::path::Path,
    compressor_name: &str,
    compressor_path: &std::path::Path,
    zstd_level: i32,
    iterations: usize,
) -> Result<BenchmarkResult> {
    // Parse batch info from directory name
    let dir_name = batch_dir.file_name().unwrap().to_str().unwrap();
    let (dataset, batch_size) = parse_batch_dir_name(dir_name)?;

    // Load compressor bytes
    let compressor_bytes =
        std::fs::read(compressor_path).context("Failed to load compressor")?;

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
        dataset,
        batch_size,
        compressor: compressor_name.to_string(),
        num_payloads,
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
        openzl: CompressionResult {
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
        },
    })
}
