//! Zstd compression/decompression benchmarks

use anyhow::{Context, Result};
use std::time::Instant;

/// Zstd benchmark configuration
pub struct ZstdBenchmark {
    level: i32,
}

/// Results from running zstd benchmarks
pub struct ZstdResult {
    /// Total compressed size in bytes
    pub total_compressed_bytes: usize,
    /// Total compression time per iteration in milliseconds
    pub compression_times: Vec<f64>,
    /// Total decompression time per iteration in milliseconds
    pub decompression_times: Vec<f64>,
}

impl ZstdBenchmark {
    /// Create a new Zstd benchmark with the specified compression level
    pub fn new(level: i32) -> Self {
        Self { level }
    }

    /// Run the benchmark on the given payloads
    ///
    /// # Arguments
    /// * `payloads` - The payloads to compress/decompress
    /// * `iterations` - Number of times to run the entire dataset
    ///
    /// # Returns
    /// Results including timing information
    pub fn run(&self, payloads: &[Vec<u8>], iterations: usize) -> Result<ZstdResult> {
        let mut compression_times = Vec::with_capacity(iterations);
        let mut decompression_times = Vec::with_capacity(iterations);
        let mut total_compressed_bytes = 0;

        for iter in 0..iterations {
            // Compression pass
            let (compressed, comp_time) = self.compress_all(payloads)?;

            // Store compressed size from first iteration
            if iter == 0 {
                total_compressed_bytes = compressed.iter().map(|p| p.len()).sum();
            }

            compression_times.push(comp_time);

            // Decompression pass
            let (decompressed, decomp_time) = self.decompress_all(&compressed)?;
            decompression_times.push(decomp_time);

            // Verify roundtrip on first iteration
            // if iter == 0 {
            //     Self::verify_roundtrip(payloads, &decompressed)?;
            // }
        }

        Ok(ZstdResult {
            total_compressed_bytes,
            compression_times,
            decompression_times,
        })
    }

    /// Verify roundtrip: decompressed data matches original
    fn verify_roundtrip(original: &[Vec<u8>], decompressed: &[Vec<u8>]) -> Result<()> {
        for (idx, (orig, decomp)) in original.iter().zip(decompressed.iter()).enumerate() {
            if orig != decomp {
                anyhow::bail!(
                    "Zstd roundtrip failed for payload {}: original {} bytes, decompressed {} bytes",
                    idx,
                    orig.len(),
                    decomp.len()
                );
            }
        }
        Ok(())
    }

    /// Compress all payloads and return total time in milliseconds
    fn compress_all(&self, payloads: &[Vec<u8>]) -> Result<(Vec<Vec<u8>>, f64)> {
        let mut compressed = Vec::with_capacity(payloads.len());
        let mut total_time = 0.0;

        for payload in payloads {
            let start = Instant::now();
            let result = zstd::encode_all(payload.as_slice(), self.level)
                .context("Failed to compress with zstd")?;
            total_time += start.elapsed().as_secs_f64() * 1000.0;
            compressed.push(result);
        }

        Ok((compressed, total_time))
    }

    /// Decompress all payloads and return decompressed data and total time in milliseconds
    fn decompress_all(&self, compressed: &[Vec<u8>]) -> Result<(Vec<Vec<u8>>, f64)> {
        let mut decompressed = Vec::with_capacity(compressed.len());
        let mut total_time = 0.0;

        for payload in compressed {
            let start = Instant::now();
            let result = zstd::decode_all(payload.as_slice())
                .context("Failed to decompress with zstd")?;
            total_time += start.elapsed().as_secs_f64() * 1000.0;
            decompressed.push(result);
        }

        Ok((decompressed, total_time))
    }
}
