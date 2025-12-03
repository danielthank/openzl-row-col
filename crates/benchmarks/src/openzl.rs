//! OpenZL proto-aware compression/decompression benchmarks

use anyhow::{Context, Result};
use openzl::proto::{compare, ProtoSchema};
use openzl::{ProtoDeserializer, ProtoSerializer};
use std::time::Instant;

/// OpenZL benchmark configuration
pub struct OpenZLBenchmark {
    serializer: ProtoSerializer,
    deserializer: ProtoDeserializer,
    schema: ProtoSchema,
}

/// Results from running OpenZL benchmarks
pub struct OpenZLResult {
    /// Total compressed size in bytes
    pub total_compressed_bytes: usize,
    /// Total compression time per iteration in milliseconds
    pub compression_times: Vec<f64>,
    /// Total decompression time per iteration in milliseconds
    pub decompression_times: Vec<f64>,
}

impl OpenZLBenchmark {
    /// Create a new OpenZL benchmark with the specified compressor
    pub fn new(compressor_bytes: &[u8], compressor_name: &str) -> Result<Self> {
        let schema = ProtoSchema::from_compressor_name(compressor_name)
            .ok_or_else(|| anyhow::anyhow!("Unknown compressor type: {}", compressor_name))?;

        let serializer = ProtoSerializer::with_compressor(compressor_bytes)
            .context("Failed to create proto serializer")?;
        let deserializer =
            ProtoDeserializer::new().context("Failed to create proto deserializer")?;

        Ok(Self {
            serializer,
            deserializer,
            schema,
        })
    }

    /// Run the benchmark on the given payloads
    ///
    /// # Arguments
    /// * `payloads` - The payloads to compress/decompress
    /// * `iterations` - Number of times to run the entire dataset
    ///
    /// # Returns
    /// Results including timing information
    pub fn run(&mut self, payloads: &[Vec<u8>], iterations: usize) -> Result<OpenZLResult> {
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
            let (_decompressed, decomp_time) = self.decompress_all(&compressed)?;
            decompression_times.push(decomp_time);
        }

        Ok(OpenZLResult {
            total_compressed_bytes,
            compression_times,
            decompression_times,
        })
    }

    /// Verify that decompressed payloads match originals semantically
    #[allow(dead_code)]
    fn verify_roundtrip(&self, original: &[Vec<u8>], decompressed: &[Vec<u8>]) -> Result<()> {
        for (idx, (orig, decomp)) in original.iter().zip(decompressed.iter()).enumerate() {
            if !compare(orig, decomp, self.schema) {
                anyhow::bail!(
                    "Roundtrip verification failed for payload {}: original {} bytes, decompressed {} bytes",
                    idx,
                    orig.len(),
                    decomp.len()
                );
            }
        }

        Ok(())
    }

    /// Compress all payloads and return total time in milliseconds
    fn compress_all(&mut self, payloads: &[Vec<u8>]) -> Result<(Vec<Vec<u8>>, f64)> {
        let mut compressed = Vec::with_capacity(payloads.len());
        let mut total_time = 0.0;

        for (idx, payload) in payloads.iter().enumerate() {
            let start = Instant::now();
            let result = self
                .serializer
                .compress(payload, self.schema)
                .with_context(|| {
                    format!(
                        "Failed to compress payload {} ({} bytes)",
                        idx,
                        payload.len()
                    )
                })?;
            total_time += start.elapsed().as_secs_f64() * 1000.0;
            compressed.push(result);
        }

        Ok((compressed, total_time))
    }

    /// Decompress all payloads and return total time in milliseconds
    fn decompress_all(&mut self, compressed: &[Vec<u8>]) -> Result<(Vec<Vec<u8>>, f64)> {
        let mut decompressed = Vec::with_capacity(compressed.len());
        let mut total_time = 0.0;

        for payload in compressed {
            let start = Instant::now();
            let result = self.deserializer.decompress(payload, self.schema)?;
            total_time += start.elapsed().as_secs_f64() * 1000.0;
            decompressed.push(result);
        }

        Ok((decompressed, total_time))
    }
}
