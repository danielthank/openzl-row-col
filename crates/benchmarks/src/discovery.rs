//! Batch directory and compressor discovery utilities

use anyhow::{Context, Result};
use std::path::{Path, PathBuf};

/// Discover batch directories containing payload files
pub fn discover_batch_directories(testdata_dir: &Path) -> Result<Vec<PathBuf>> {
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

/// Discover trained compressors in the data directory
pub fn discover_compressors(data_dir: &Path) -> Result<Vec<(String, PathBuf)>> {
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

/// Get the appropriate compressor name for a data directory
pub fn get_compressor_for_data(dir_name: &str) -> Option<&'static str> {
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

/// Parse batch directory name into dataset and batch size
pub fn parse_batch_dir_name(dir_name: &str) -> Result<(String, usize)> {
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

/// Read all payload files from a batch directory
pub fn read_payloads(batch_dir: &Path) -> Result<Vec<Vec<u8>>> {
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
