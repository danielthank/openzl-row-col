//! Batch directory and compressor discovery utilities

use anyhow::{Context, Result};
use serde::Deserialize;
use std::path::{Path, PathBuf};

/// Metadata for a batch directory (from Go rebatch program)
#[derive(Debug, Deserialize)]
pub struct BatchMetadata {
    pub total_data_points: usize,
    #[allow(dead_code)]
    pub num_payloads: usize,
}

/// Read metadata.json from a batch directory
pub fn read_metadata(batch_dir: &Path) -> Result<BatchMetadata> {
    let metadata_path = batch_dir.join("metadata.json");
    let content = std::fs::read_to_string(&metadata_path)
        .with_context(|| format!("Failed to read {}", metadata_path.display()))?;
    let metadata: BatchMetadata = serde_json::from_str(&content)
        .with_context(|| format!("Failed to parse {}", metadata_path.display()))?;
    Ok(metadata)
}

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
    // Note: Only protobuf-based formats are supported
    // OTAP uses Arrow IPC format, which is zstd-only
    // otlpmetricsdict/otlptracesdict use the same schema as otlp_metrics/otlp_traces
    for schema in ["otlp_metrics", "otlp_traces", "tpch_proto"] {
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
    /// Returns otlp_metrics/otlp_traces for OTLP formats, or tpch_proto for TPC-H proto format.
    /// Returns None for zstd-only formats (OTAP, Arrow, otapnodict, otapdictperfile).
    pub fn compressor_name(&self) -> Option<&str> {
        match self.format.as_str() {
            // OTLP formats (all use same protobuf schema)
            "otlp" if self.dataset.contains("otelmetrics") => Some("otlp_metrics"),
            "otlp" if self.dataset.contains("oteltraces") => Some("otlp_traces"),
            // OTLP with dictionary-encoded attribute keys (same schema as regular OTLP)
            "otlpmetricsdict" => Some("otlp_metrics"),
            "otlptracesdict" => Some("otlp_traces"),
            // TPC-H proto format
            "proto" if self.dataset.starts_with("tpch-") => Some("tpch_proto"),
            // OTAP uses Arrow IPC, which is zstd-only
            _ => None,
        }
    }

    /// Get the trained compressor file to load
    /// Returns None for zstd-only formats (OTAP, Arrow, otapnodict, otapdictperfile).
    pub fn trained_compressor_name(&self) -> Option<&'static str> {
        match self.format.as_str() {
            // OTLP formats (all use same protobuf schema)
            "otlp" if self.dataset.contains("otelmetrics") => Some("otlp_metrics"),
            "otlp" if self.dataset.contains("oteltraces") => Some("otlp_traces"),
            // OTLP with dictionary-encoded attribute keys (same schema as regular OTLP)
            "otlpmetricsdict" => Some("otlp_metrics"),
            "otlptracesdict" => Some("otlp_traces"),
            // TPC-H proto format
            "proto" if self.dataset.starts_with("tpch-") => Some("tpch_proto"),
            // OTAP uses Arrow IPC, which is zstd-only
            _ => None,
        }
    }

    /// Check if this format is zstd-only (no OpenZL benchmark)
    pub fn is_zstd_only_format(&self) -> bool {
        // Arrow formats for TPC-H
        let is_arrow = matches!(self.format.as_str(), "arrow" | "arrownodict" | "arrowdictperfile")
            && self.dataset.starts_with("tpch-");
        // OTAP format uses Arrow IPC (zstd-only)
        let is_otap = self.format == "otap";
        // OTAP variants (zstd-only)
        let is_otap_variant = matches!(self.format.as_str(), "otapnodict" | "otapdictperfile" | "otapnosort" | "otapnodedup");
        is_arrow || is_otap || is_otap_variant
    }

    /// Get the format name for zstd-only benchmarks
    /// Returns the format name for Arrow/OTAP formats (zstd-only, no OpenZL).
    pub fn zstd_only_format_name(&self) -> Option<&str> {
        if self.is_zstd_only_format() {
            Some(&self.format)
        } else {
            None
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
