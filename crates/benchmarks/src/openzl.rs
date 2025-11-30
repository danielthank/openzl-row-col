//! OpenZL proto-aware compression/decompression benchmarks

use anyhow::{Context, Result};
use openzl::proto::{
    compare_otap, compare_otlp_metrics, compare_otlp_traces, compare_tpch_proto,
};
use openzl::{ProtoDeserializer, ProtoSerializer};
use std::time::Instant;

/// Proto message type for compression/decompression dispatch
#[derive(Debug, Clone, Copy)]
enum ProtoType {
    OtlpMetrics,
    OtlpTraces,
    Otap,
    TpchProto,
}

impl ProtoType {
    /// Determine proto type from compressor name
    fn from_compressor_name(name: &str) -> Option<Self> {
        match name {
            // OTel formats
            "otlp_metrics" => Some(Self::OtlpMetrics),
            "otlp_traces" => Some(Self::OtlpTraces),
            "otap" | "otapnodict" | "otapdictperfile" => Some(Self::Otap),
            // TPC-H proto format (Arrow doesn't use proto-aware compression)
            "tpch_proto" => Some(Self::TpchProto),
            _ => None,
        }
    }
}

/// OpenZL benchmark configuration
pub struct OpenZLBenchmark {
    serializer: ProtoSerializer,
    deserializer: ProtoDeserializer,
    proto_type: ProtoType,
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
        let proto_type = ProtoType::from_compressor_name(compressor_name)
            .ok_or_else(|| anyhow::anyhow!("Unknown compressor type: {}", compressor_name))?;

        let serializer = ProtoSerializer::with_compressor(compressor_bytes)
            .context("Failed to create proto serializer")?;
        let deserializer =
            ProtoDeserializer::new().context("Failed to create proto deserializer")?;

        Ok(Self {
            serializer,
            deserializer,
            proto_type,
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
            let (decompressed, decomp_time) = self.decompress_all(&compressed)?;
            decompression_times.push(decomp_time);

            // Verify roundtrip on first iteration
            // if iter == 0 {
            //     self.verify_roundtrip(payloads, &decompressed)?;
            // }
        }

        Ok(OpenZLResult {
            total_compressed_bytes,
            compression_times,
            decompression_times,
        })
    }

    /// Verify that decompressed payloads match originals semantically
    fn verify_roundtrip(&self, original: &[Vec<u8>], decompressed: &[Vec<u8>]) -> Result<()> {
        for (idx, (orig, decomp)) in original.iter().zip(decompressed.iter()).enumerate() {
            let equal = match self.proto_type {
                ProtoType::OtlpMetrics => compare_otlp_metrics(orig, decomp),
                ProtoType::OtlpTraces => compare_otlp_traces(orig, decomp),
                ProtoType::Otap => compare_otap(orig, decomp),
                ProtoType::TpchProto => compare_tpch_proto(orig, decomp),
            };

            if !equal {
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
            let result = match self.proto_type {
                ProtoType::OtlpMetrics => self.serializer.compress_otlp_metrics(payload),
                ProtoType::OtlpTraces => self.serializer.compress_otlp_traces(payload),
                ProtoType::Otap => self.serializer.compress_otap(payload),
                ProtoType::TpchProto => self.serializer.compress_tpch_proto(payload),
            }
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
            let result = match self.proto_type {
                ProtoType::OtlpMetrics => self.deserializer.decompress_otlp_metrics(payload),
                ProtoType::OtlpTraces => self.deserializer.decompress_otlp_traces(payload),
                ProtoType::Otap => self.deserializer.decompress_otap(payload),
                ProtoType::TpchProto => self.deserializer.decompress_tpch_proto(payload),
            }?;
            total_time += start.elapsed().as_secs_f64() * 1000.0;
            decompressed.push(result);
        }

        Ok((decompressed, total_time))
    }
}
