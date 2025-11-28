//! Benchmark suite for OpenZL compression on OpenTelemetry data
//!
//! This benchmarks compression performance using the openzl library directly
//! (no external process calls) for accurate CPU time measurements.

use anyhow::{Context, Result};
use openzl::{CCtx, Compressor, DCtx};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

#[derive(Debug, Serialize, Deserialize)]
struct BenchmarkResult {
    dataset: String,
    batch_size: usize,
    compressor: String,
    num_payloads: usize,

    // Raw metrics
    total_uncompressed_bytes: usize,

    // Zstd metrics
    total_zstd_bytes: usize,
    zstd_compression_ratio: f64,
    avg_zstd_compression_time_ms: f64,
    avg_zstd_decompression_time_ms: f64,
    zstd_throughput_mbps: f64,

    // OpenZL metrics
    total_openzl_bytes: usize,
    openzl_compression_ratio: f64,
    avg_openzl_compression_time_ms: f64,
    avg_openzl_decompression_time_ms: f64,
    openzl_throughput_mbps: f64,
}

#[derive(Debug, Serialize, Deserialize)]
struct BenchmarkSuite {
    results: Vec<BenchmarkResult>,
}

fn print_comparison_table(results: &[BenchmarkResult]) {
    use std::collections::HashMap;

    println!();
    println!("===== Batch Size Compression Benchmarks");

    // Group results by (dataset, batch_size)
    let mut grouped: HashMap<(String, usize), Vec<&BenchmarkResult>> = HashMap::new();
    for result in results {
        let key = (result.dataset.clone(), result.batch_size);
        grouped.entry(key).or_insert_with(Vec::new).push(result);
    }

    // Get sorted keys
    let mut keys: Vec<_> = grouped.keys().cloned().collect();
    keys.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));

    for (dataset, batch_size) in keys {
        println!();
        println!("{} (batch size={})", dataset, batch_size);
        println!("             Uncompressed                     Zstd Compressed                              OpenZL Compressed");
        println!("                Bytes     Ratio          Bytes  Ratio    Comp(s) Decomp(s)          Bytes  Ratio    Comp(s) Decomp(s)");

        let mut group_results = grouped.get(&(dataset.clone(), batch_size)).unwrap().clone();

        // Sort by format to ensure OTLP comes before OTAP
        group_results.sort_by(|a, b| {
            let a_is_otlp = a.compressor.starts_with("otlp");
            let b_is_otlp = b.compressor.starts_with("otlp");
            match (a_is_otlp, b_is_otlp) {
                (true, false) => std::cmp::Ordering::Less,    // OTLP before OTAP
                (false, true) => std::cmp::Ordering::Greater, // OTAP after OTLP
                _ => a.compressor.cmp(&b.compressor),         // Same type, sort alphabetically
            }
        });

        // Calculate baseline from first result (for ratio calculation)
        let baseline_bytes = group_results[0].total_uncompressed_bytes;

        for result in group_results {
            let format_name = match result.compressor.as_str() {
                "otlp_metrics" | "otlp_traces" => "OTLP",
                "otap" => "OTAP",
                _ => &result.compressor,
            };

            let raw_ratio = baseline_bytes as f64 / result.total_uncompressed_bytes as f64;
            let zstd_ratio = baseline_bytes as f64 / result.total_zstd_bytes as f64;
            let openzl_ratio = baseline_bytes as f64 / result.total_openzl_bytes as f64;

            // Total CPU time in seconds (avg time per payload * num payloads / 1000)
            let zstd_comp_s = result.avg_zstd_compression_time_ms * result.num_payloads as f64 / 1000.0;
            let zstd_decomp_s = result.avg_zstd_decompression_time_ms * result.num_payloads as f64 / 1000.0;
            let openzl_comp_s = result.avg_openzl_compression_time_ms * result.num_payloads as f64 / 1000.0;
            let openzl_decomp_s = result.avg_openzl_decompression_time_ms * result.num_payloads as f64 / 1000.0;

            println!(
                "{:<12} {:9} {:5.2}          {:6} {:6.2}    {:6.2}   {:6.2}          {:6} {:6.2}    {:6.2}   {:6.2}",
                format_name,
                result.total_uncompressed_bytes,
                raw_ratio,
                result.total_zstd_bytes,
                zstd_ratio,
                zstd_comp_s,
                zstd_decomp_s,
                result.total_openzl_bytes,
                openzl_ratio,
                openzl_comp_s,
                openzl_decomp_s,
            );
        }
    }

    println!();
}

fn main() -> Result<()> {
    println!("OpenZL Benchmark Suite");
    println!("======================");
    println!();

    let batch_data_dir = PathBuf::from("data/generated");
    let compressor_dir = PathBuf::from("data");

    if !batch_data_dir.exists() {
        anyhow::bail!(
            "Batch data directory not found: {}. Run rebatch first.",
            batch_data_dir.display()
        );
    }

    // Discover all batch directories
    let batch_dirs = discover_batch_directories(&batch_data_dir)?;
    println!("Found {} batch directories", batch_dirs.len());

    // Discover trained compressors
    let compressors = discover_compressors(&compressor_dir)?;
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

            match run_benchmark(batch_dir, compressor_name, compressor_path) {
                Ok(result) => {
                    println!(
                        "[{}] {} @ batch_size={}",
                        result.dataset,
                        result.compressor,
                        result.batch_size,
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
    let output_file = File::create("data/batch_size_results.json")?;
    serde_json::to_writer_pretty(output_file, &suite)?;
    println!("Results written to data/batch_size_results.json");

    Ok(())
}

fn discover_batch_directories(testdata_dir: &Path) -> Result<Vec<PathBuf>> {
    let mut dirs = Vec::new();

    for entry in std::fs::read_dir(testdata_dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.is_dir() {
            // Check if directory contains payload files
            let has_payloads = std::fs::read_dir(&path)?
                .filter_map(|e| e.ok())
                .any(|e| {
                    e.file_name()
                        .to_str()
                        .map(|s| s.starts_with("payload_") && s.ends_with(".bin"))
                        .unwrap_or(false)
                });

            if has_payloads {
                dirs.push(path);
            }
        }
    }

    dirs.sort();
    Ok(dirs)
}

fn discover_compressors(data_dir: &Path) -> Result<Vec<(String, PathBuf)>> {
    let mut compressors = Vec::new();

    // Look for trained.zlc files in subdirectories
    for schema in ["otap", "otlp_metrics", "otlp_traces"] {
        let schema_dir = data_dir.join(schema);
        let compressor_path = schema_dir.join("trained.zlc");

        if compressor_path.exists() {
            compressors.push((schema.to_string(), compressor_path));
        }
    }

    Ok(compressors)
}

fn get_compressor_for_data(dir_name: &str) -> Option<&'static str> {
    // Automatically choose the right compressor based on data format
    // Directory format: {dataset}-{signal}-{format}-{batchsize}
    // Examples: astronomy-otelmetrics-otap-100, astronomy-oteltraces-otlp-50

    if dir_name.contains("-otap-") {
        // OTAP format data -> use OTAP compressor
        Some("otap")
    } else if dir_name.contains("otelmetrics") && dir_name.contains("-otlp-") {
        // OTLP metrics data -> use OTLP metrics compressor
        Some("otlp_metrics")
    } else if dir_name.contains("oteltraces") && dir_name.contains("-otlp-") {
        // OTLP traces data -> use OTLP traces compressor
        Some("otlp_traces")
    } else {
        // Unknown format, skip
        None
    }
}

fn run_benchmark(
    batch_dir: &Path,
    compressor_name: &str,
    compressor_path: &Path,
) -> Result<BenchmarkResult> {
    // Parse batch info from directory name
    let dir_name = batch_dir.file_name().unwrap().to_str().unwrap();
    let (dataset, batch_size) = parse_batch_dir_name(dir_name)?;

    // Load compressor
    let compressor = Arc::new(
        Compressor::load_from_file(compressor_path)
            .context("Failed to load compressor")?,
    );

    // Read all payloads
    let payloads = read_payloads(batch_dir)?;
    let num_payloads = payloads.len();

    if num_payloads == 0 {
        anyhow::bail!("No payloads found in {}", batch_dir.display());
    }

    // Benchmark Zstd compression
    let (zstd_compressed_payloads, zstd_compression_times) =
        benchmark_zstd_compression(&payloads)?;

    // Benchmark Zstd decompression
    let (_zstd_decompressed_payloads, zstd_decompression_times) =
        benchmark_zstd_decompression(&zstd_compressed_payloads)?;

    // Benchmark OpenZL compression
    let (openzl_compressed_payloads, openzl_compression_times) =
        benchmark_compression(&payloads, compressor.clone())?;

    // Benchmark OpenZL decompression and verify roundtrip
    let (openzl_decompressed_payloads, openzl_decompression_times) =
        benchmark_decompression(&openzl_compressed_payloads)?;

    // Verify roundtrip for all payloads
    for (idx, (original, decompressed)) in payloads.iter().zip(openzl_decompressed_payloads.iter()).enumerate() {
        if original != decompressed {
            anyhow::bail!(
                "Roundtrip verification failed for payload {}: original {} bytes, decompressed {} bytes",
                idx,
                original.len(),
                decompressed.len()
            );
        }
    }

    // Calculate statistics
    let total_uncompressed_bytes: usize = payloads.iter().map(|p| p.len()).sum();

    // Zstd statistics
    let total_zstd_bytes: usize = zstd_compressed_payloads.iter().map(|p| p.len()).sum();
    let zstd_compression_ratio = total_uncompressed_bytes as f64 / total_zstd_bytes as f64;
    let avg_zstd_compression_time_ms = zstd_compression_times.iter().sum::<f64>() / num_payloads as f64;
    let avg_zstd_decompression_time_ms = zstd_decompression_times.iter().sum::<f64>() / num_payloads as f64;
    let zstd_throughput_mbps = (total_uncompressed_bytes as f64 / 1_000_000.0)
        / (zstd_compression_times.iter().sum::<f64>() / 1000.0);

    // OpenZL statistics
    let total_openzl_bytes: usize = openzl_compressed_payloads.iter().map(|p| p.len()).sum();
    let openzl_compression_ratio = total_uncompressed_bytes as f64 / total_openzl_bytes as f64;
    let avg_openzl_compression_time_ms = openzl_compression_times.iter().sum::<f64>() / num_payloads as f64;
    let avg_openzl_decompression_time_ms = openzl_decompression_times.iter().sum::<f64>() / num_payloads as f64;
    let openzl_throughput_mbps = (total_uncompressed_bytes as f64 / 1_000_000.0)
        / (openzl_compression_times.iter().sum::<f64>() / 1000.0);

    Ok(BenchmarkResult {
        dataset,
        batch_size,
        compressor: compressor_name.to_string(),
        num_payloads,
        total_uncompressed_bytes,
        total_zstd_bytes,
        zstd_compression_ratio,
        avg_zstd_compression_time_ms,
        avg_zstd_decompression_time_ms,
        zstd_throughput_mbps,
        total_openzl_bytes,
        openzl_compression_ratio,
        avg_openzl_compression_time_ms,
        avg_openzl_decompression_time_ms,
        openzl_throughput_mbps,
    })
}

fn parse_batch_dir_name(dir_name: &str) -> Result<(String, usize)> {
    // Format: astronomy-otelmetrics-otlp-100
    // Parse as: dataset=astronomy-otelmetrics, batch_size=100
    let parts: Vec<&str> = dir_name.rsplitn(2, '-').collect();
    if parts.len() != 2 {
        anyhow::bail!("Invalid directory name format: {}", dir_name);
    }

    let batch_size = parts[0]
        .parse::<usize>()
        .context("Failed to parse batch size")?;

    // Remove the format suffix (otlp or otap) from dataset
    let dataset_with_format = parts[1];
    let dataset = if dataset_with_format.ends_with("-otlp") {
        &dataset_with_format[..dataset_with_format.len() - 5]
    } else if dataset_with_format.ends_with("-otap") {
        &dataset_with_format[..dataset_with_format.len() - 5]
    } else {
        dataset_with_format
    };

    Ok((dataset.to_string(), batch_size))
}

fn read_payloads(batch_dir: &Path) -> Result<Vec<Vec<u8>>> {
    let mut payloads = Vec::new();
    let mut entries: Vec<_> = std::fs::read_dir(batch_dir)?
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name()
                .to_str()
                .map(|s| s.starts_with("payload_") && s.ends_with(".bin"))
                .unwrap_or(false)
        })
        .collect();

    entries.sort_by_key(|e| e.file_name());

    for entry in entries {
        let payload = std::fs::read(entry.path())?;
        payloads.push(payload);
    }

    Ok(payloads)
}

fn benchmark_zstd_compression(
    payloads: &[Vec<u8>],
) -> Result<(Vec<Vec<u8>>, Vec<f64>)> {
    let mut compressed_payloads = Vec::with_capacity(payloads.len());
    let mut times = Vec::with_capacity(payloads.len());

    for payload in payloads {
        let start = Instant::now();
        let compressed = zstd::encode_all(payload.as_slice(), 4)
            .context("Failed to compress with zstd")?;
        let elapsed = start.elapsed();

        compressed_payloads.push(compressed);
        times.push(elapsed.as_secs_f64() * 1000.0); // Convert to milliseconds
    }

    Ok((compressed_payloads, times))
}

fn benchmark_zstd_decompression(
    compressed_payloads: &[Vec<u8>],
) -> Result<(Vec<Vec<u8>>, Vec<f64>)> {
    let mut decompressed_payloads = Vec::with_capacity(compressed_payloads.len());
    let mut times = Vec::with_capacity(compressed_payloads.len());

    for compressed in compressed_payloads {
        let start = Instant::now();
        let decompressed = zstd::decode_all(compressed.as_slice())
            .context("Failed to decompress with zstd")?;
        let elapsed = start.elapsed();

        decompressed_payloads.push(decompressed);
        times.push(elapsed.as_secs_f64() * 1000.0); // Convert to milliseconds
    }

    Ok((decompressed_payloads, times))
}

fn benchmark_compression(
    payloads: &[Vec<u8>],
    compressor: Arc<Compressor>,
) -> Result<(Vec<Vec<u8>>, Vec<f64>)> {
    let mut compressed_payloads = Vec::with_capacity(payloads.len());
    let mut times = Vec::with_capacity(payloads.len());

    for (idx, payload) in payloads.iter().enumerate() {
        // Create compression context
        let mut cctx = CCtx::new()?;
        cctx.attach_compressor(compressor.clone())?;

        // Benchmark compression using metadata API (required for trained compressors)
        let start = Instant::now();
        let compressed = cctx.compress_with_metadata(payload, 0)
            .with_context(|| format!("Failed to compress payload {} ({} bytes)", idx, payload.len()))?;
        let elapsed = start.elapsed();

        compressed_payloads.push(compressed);
        times.push(elapsed.as_secs_f64() * 1000.0); // Convert to milliseconds
    }

    Ok((compressed_payloads, times))
}

fn benchmark_decompression(compressed_payloads: &[Vec<u8>]) -> Result<(Vec<Vec<u8>>, Vec<f64>)> {
    let mut decompressed_payloads = Vec::with_capacity(compressed_payloads.len());
    let mut times = Vec::with_capacity(compressed_payloads.len());

    for compressed in compressed_payloads {
        // Create decompression context
        let mut dctx = DCtx::new()?;

        // Benchmark decompression
        let start = Instant::now();
        let decompressed = dctx.decompress(compressed)?;
        let elapsed = start.elapsed();

        decompressed_payloads.push(decompressed);
        times.push(elapsed.as_secs_f64() * 1000.0); // Convert to milliseconds
    }

    Ok((decompressed_payloads, times))
}
