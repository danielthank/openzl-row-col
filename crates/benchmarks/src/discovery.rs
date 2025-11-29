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

/// Parsed batch directory info
#[derive(Debug, Clone)]
pub struct BatchDirInfo {
    /// Dataset name, e.g., "astronomy-otelmetrics"
    pub dataset: String,
    /// Format name, e.g., "otap", "otapnodict", "otlp"
    pub format: String,
    /// Batch size, e.g., 1000
    pub batch_size: usize,
}

impl BatchDirInfo {
    /// Get the compressor name to use for this batch
    /// Returns the format for OTAP variants, or otlp_metrics/otlp_traces for OTLP
    pub fn compressor_name(&self) -> Option<&str> {
        match self.format.as_str() {
            "otap" | "otapnodict" | "otapdictperfile" => Some(&self.format),
            "otlp" if self.dataset.contains("otelmetrics") => Some("otlp_metrics"),
            "otlp" if self.dataset.contains("oteltraces") => Some("otlp_traces"),
            _ => None,
        }
    }

    /// Get the trained compressor file to load
    /// All OTAP variants use the same trained "otap" compressor
    pub fn trained_compressor_name(&self) -> Option<&'static str> {
        match self.format.as_str() {
            "otap" | "otapnodict" | "otapdictperfile" => Some("otap"),
            "otlp" if self.dataset.contains("otelmetrics") => Some("otlp_metrics"),
            "otlp" if self.dataset.contains("oteltraces") => Some("otlp_traces"),
            _ => None,
        }
    }
}

/// Parse batch directory name into components
/// Format: {dataset}-{signal}-{format}-{batchsize}
/// Example: astronomy-otelmetrics-otapdictperfile-1000
pub fn parse_batch_dir_name(dir_name: &str) -> Result<BatchDirInfo> {
    // Split from the right: last part is batch_size, second-to-last is format
    let parts: Vec<&str> = dir_name.rsplitn(3, '-').collect();
    // parts[0] = batch_size, parts[1] = format, parts[2] = rest (dataset-signal)

    if parts.len() < 3 {
        anyhow::bail!("Invalid directory name format: {}", dir_name);
    }

    let batch_size = parts[0]
        .parse::<usize>()
        .context("Failed to parse batch size")?;

    Ok(BatchDirInfo {
        dataset: parts[2].to_string(),
        format: parts[1].to_string(),
        batch_size,
    })
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
