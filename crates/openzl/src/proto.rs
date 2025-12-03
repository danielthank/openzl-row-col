//! Proto-aware compression using fast C++ protobuf reflection

use crate::{compress_bound, OpenZLError, Result};
use openzl_sys as ffi;
use std::ffi::{c_void, CStr};
use std::ptr::NonNull;

/// Proto schema identifier matching C enum ZL_ProtoSchema
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProtoSchema {
    OtlpMetrics = 0,
    OtlpTraces = 1,
    Otap = 2,
    Tpch = 3,
    OtlpMetricsDict = 4,
}

impl ProtoSchema {
    /// Create schema from compressor name
    pub fn from_compressor_name(name: &str) -> Option<Self> {
        match name {
            "otlp_metrics" => Some(Self::OtlpMetrics),
            "otlp_traces" => Some(Self::OtlpTraces),
            "otlpmetricsdict" => Some(Self::OtlpMetricsDict),
            "otap" | "otapnodict" | "otapdictperfile" => Some(Self::Otap),
            "tpch_proto" => Some(Self::Tpch),
            _ => None,
        }
    }
}

/// Get the last error message from the proto compression library
fn get_last_proto_error() -> String {
    unsafe {
        let ptr = ffi::ZL_Proto_getLastError();
        if ptr.is_null() {
            return String::new();
        }
        CStr::from_ptr(ptr).to_string_lossy().into_owned()
    }
}

/// Proto-aware serializer using fast C++ protobuf reflection
pub struct ProtoSerializer {
    ptr: NonNull<ffi::ZL_ProtoSerializer>,
}

impl ProtoSerializer {
    /// Create a new proto serializer with the generic compression graph
    pub fn new() -> Result<Self> {
        let ptr = unsafe { ffi::ZL_ProtoSerializer_create() };
        NonNull::new(ptr)
            .map(|ptr| Self { ptr })
            .ok_or(OpenZLError::CompressorCreateFailed)
    }

    /// Create a proto serializer with a trained compressor
    pub fn with_compressor(compressor_bytes: &[u8]) -> Result<Self> {
        let ptr = unsafe {
            ffi::ZL_ProtoSerializer_createWithCompressor(
                compressor_bytes.as_ptr() as *const c_void,
                compressor_bytes.len(),
            )
        };
        NonNull::new(ptr)
            .map(|ptr| Self { ptr })
            .ok_or(OpenZLError::CompressorCreateFailed)
    }

    /// Compress proto bytes using schema-aware compression
    ///
    /// Takes raw protobuf bytes and returns compressed bytes using
    /// proto-aware type-split compression.
    pub fn compress(&mut self, proto_bytes: &[u8], schema: ProtoSchema) -> Result<Vec<u8>> {
        let bound = compress_bound(proto_bytes.len());
        let mut dst = vec![0u8; bound];

        let written = unsafe {
            ffi::ZL_ProtoSerializer_compress(
                self.ptr.as_ptr(),
                dst.as_mut_ptr() as *mut c_void,
                dst.len(),
                proto_bytes.as_ptr() as *const c_void,
                proto_bytes.len(),
                schema as ffi::ZL_ProtoSchema,
            )
        };

        if written == 0 {
            return Err(OpenZLError::CompressionFailed(0));
        }
        dst.truncate(written);
        Ok(dst)
    }
}

impl Drop for ProtoSerializer {
    fn drop(&mut self) {
        unsafe { ffi::ZL_ProtoSerializer_free(self.ptr.as_ptr()) }
    }
}

unsafe impl Send for ProtoSerializer {}

/// Proto-aware deserializer using fast C++ protobuf reflection
pub struct ProtoDeserializer {
    ptr: NonNull<ffi::ZL_ProtoDeserializer>,
}

impl ProtoDeserializer {
    /// Create a new proto deserializer
    pub fn new() -> Result<Self> {
        let ptr = unsafe { ffi::ZL_ProtoDeserializer_create() };
        NonNull::new(ptr)
            .map(|ptr| Self { ptr })
            .ok_or(OpenZLError::DCtxCreateFailed)
    }

    /// Decompress proto bytes using schema-aware decompression
    ///
    /// Takes compressed bytes and returns the original protobuf bytes.
    pub fn decompress(&mut self, compressed: &[u8], schema: ProtoSchema) -> Result<Vec<u8>> {
        // Use a large buffer - compression ratios can exceed 100x
        let capacity = compressed.len() * 200;
        let mut dst = vec![0u8; capacity];

        let written = unsafe {
            ffi::ZL_ProtoDeserializer_decompress(
                self.ptr.as_ptr(),
                dst.as_mut_ptr() as *mut c_void,
                dst.len(),
                compressed.as_ptr() as *const c_void,
                compressed.len(),
                schema as ffi::ZL_ProtoSchema,
            )
        };

        if written == 0 {
            let error_msg = get_last_proto_error();
            if !error_msg.is_empty() {
                eprintln!("Proto decompression error: {}", error_msg);
            }
            return Err(OpenZLError::DecompressionFailed(0));
        }
        dst.truncate(written);
        Ok(dst)
    }
}

impl Drop for ProtoDeserializer {
    fn drop(&mut self) {
        unsafe { ffi::ZL_ProtoDeserializer_free(self.ptr.as_ptr()) }
    }
}

unsafe impl Send for ProtoDeserializer {}

/// Compare two proto messages for semantic equality
///
/// This parses both byte arrays as protobuf messages and compares them
/// semantically rather than byte-for-byte (since protobuf serialization
/// is not deterministic).
pub fn compare(proto1: &[u8], proto2: &[u8], schema: ProtoSchema) -> bool {
    let result = unsafe {
        ffi::ZL_Proto_compare(
            proto1.as_ptr() as *const c_void,
            proto1.len(),
            proto2.as_ptr() as *const c_void,
            proto2.len(),
            schema as ffi::ZL_ProtoSchema,
        )
    };
    result == 1
}

#[cfg(test)]
mod tests {
    use super::*;

    fn read_payload_files(payload_dir: &str) -> Vec<std::fs::DirEntry> {
        let mut entries: Vec<_> = std::fs::read_dir(payload_dir)
            .expect("Failed to read payload directory")
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.file_name()
                    .to_str()
                    .map(|s| s.starts_with("payload_") && s.ends_with(".bin"))
                    .unwrap_or(false)
            })
            .collect();
        entries.sort_by_key(|e| e.file_name());
        entries
    }

    #[test]
    fn test_astronomy_otelmetrics_otlp_compression_ratio() {
        let compressor_path = "testdata/otlp_metrics/trained.zlc";
        let payload_dir = "testdata/otlp_metrics/astronomy-50000";

        if !std::path::Path::new(compressor_path).exists() {
            panic!("Compressor not found: {}", compressor_path);
        }
        if !std::path::Path::new(payload_dir).exists() {
            panic!("Payload directory not found: {}", payload_dir);
        }

        let compressor_bytes =
            std::fs::read(compressor_path).expect("Failed to read compressor");
        let mut serializer = ProtoSerializer::with_compressor(&compressor_bytes)
            .expect("Failed to create serializer");
        let mut deserializer =
            ProtoDeserializer::new().expect("Failed to create deserializer");

        let entries = read_payload_files(payload_dir);
        assert!(!entries.is_empty(), "No payload files found");
        println!("Found {} payload files", entries.len());

        let mut total_uncompressed: usize = 0;
        let mut total_compressed: usize = 0;

        for entry in &entries {
            let proto_bytes = std::fs::read(entry.path()).expect("Failed to read payload");
            let compressed = serializer
                .compress(&proto_bytes, ProtoSchema::OtlpMetrics)
                .expect("Compression failed");
            let decompressed = deserializer
                .decompress(&compressed, ProtoSchema::OtlpMetrics)
                .expect("Decompression failed");

            assert!(
                compare(&proto_bytes, &decompressed, ProtoSchema::OtlpMetrics),
                "Roundtrip failed for {:?}",
                entry.file_name()
            );

            println!(
                "{:?}: {} -> {} bytes",
                entry.file_name(),
                proto_bytes.len(),
                compressed.len()
            );

            total_uncompressed += proto_bytes.len();
            total_compressed += compressed.len();
        }

        let compression_ratio = total_uncompressed as f64 / total_compressed as f64;
        println!(
            "Total: {} -> {} bytes, ratio: {:.2}x",
            total_uncompressed, total_compressed, compression_ratio
        );

        // Expected ratio ~66.78 (within 1%)
        let expected_ratio = 66.78;
        let tolerance = 0.01;
        assert!(
            compression_ratio >= expected_ratio * (1.0 - tolerance)
                && compression_ratio <= expected_ratio * (1.0 + tolerance),
            "Compression ratio {:.2}x outside expected range",
            compression_ratio
        );
    }

    #[test]
    fn test_hipstershop_oteltraces_otlp_compression_ratio() {
        let compressor_path = "testdata/otlp_traces/trained.zlc";
        let payload_dir = "testdata/otlp_traces/hipstershop-50000";

        if !std::path::Path::new(compressor_path).exists() {
            panic!("Compressor not found: {}", compressor_path);
        }
        if !std::path::Path::new(payload_dir).exists() {
            panic!("Payload directory not found: {}", payload_dir);
        }

        let compressor_bytes =
            std::fs::read(compressor_path).expect("Failed to read compressor");
        let mut serializer = ProtoSerializer::with_compressor(&compressor_bytes)
            .expect("Failed to create serializer");
        let mut deserializer =
            ProtoDeserializer::new().expect("Failed to create deserializer");

        let entries = read_payload_files(payload_dir);
        assert!(!entries.is_empty(), "No payload files found");
        println!("Found {} payload files", entries.len());

        let mut total_uncompressed: usize = 0;
        let mut total_compressed: usize = 0;

        for entry in &entries {
            let proto_bytes = std::fs::read(entry.path()).expect("Failed to read payload");
            let compressed = serializer
                .compress(&proto_bytes, ProtoSchema::OtlpTraces)
                .expect("Compression failed");
            let decompressed = deserializer
                .decompress(&compressed, ProtoSchema::OtlpTraces)
                .expect("Decompression failed");

            assert!(
                compare(&proto_bytes, &decompressed, ProtoSchema::OtlpTraces),
                "Roundtrip failed for {:?}",
                entry.file_name()
            );

            println!(
                "{:?}: {} -> {} bytes",
                entry.file_name(),
                proto_bytes.len(),
                compressed.len()
            );

            total_uncompressed += proto_bytes.len();
            total_compressed += compressed.len();
        }

        let compression_ratio = total_uncompressed as f64 / total_compressed as f64;
        println!(
            "Total: {} -> {} bytes, ratio: {:.2}x",
            total_uncompressed, total_compressed, compression_ratio
        );

        // Expected ratio ~32.47 (within 1%)
        let expected_ratio = 32.47;
        let tolerance = 0.01;
        assert!(
            compression_ratio >= expected_ratio * (1.0 - tolerance)
                && compression_ratio <= expected_ratio * (1.0 + tolerance),
            "Compression ratio {:.2}x outside expected range",
            compression_ratio
        );
    }

    #[test]
    fn test_astronomy_oteltraces_otap_compression_ratio() {
        let compressor_path = "testdata/otap/trained.zlc";
        let payload_dir = "testdata/otap/astronomy-50000";

        if !std::path::Path::new(compressor_path).exists() {
            panic!("Compressor not found: {}", compressor_path);
        }
        if !std::path::Path::new(payload_dir).exists() {
            panic!("Payload directory not found: {}", payload_dir);
        }

        let compressor_bytes =
            std::fs::read(compressor_path).expect("Failed to read compressor");
        let mut serializer = ProtoSerializer::with_compressor(&compressor_bytes)
            .expect("Failed to create serializer");
        let mut deserializer =
            ProtoDeserializer::new().expect("Failed to create deserializer");

        let entries = read_payload_files(payload_dir);
        assert!(!entries.is_empty(), "No payload files found");
        println!("Found {} payload files", entries.len());

        let mut total_uncompressed: usize = 0;
        let mut total_compressed: usize = 0;

        for entry in &entries {
            let proto_bytes = std::fs::read(entry.path()).expect("Failed to read payload");
            let compressed = serializer
                .compress(&proto_bytes, ProtoSchema::Otap)
                .expect("Compression failed");
            let decompressed = deserializer
                .decompress(&compressed, ProtoSchema::Otap)
                .expect("Decompression failed");

            assert!(
                compare(&proto_bytes, &decompressed, ProtoSchema::Otap),
                "Roundtrip failed for {:?}",
                entry.file_name()
            );

            println!(
                "{:?}: {} -> {} bytes",
                entry.file_name(),
                proto_bytes.len(),
                compressed.len()
            );

            total_uncompressed += proto_bytes.len();
            total_compressed += compressed.len();
        }

        let compression_ratio = total_uncompressed as f64 / total_compressed as f64;
        println!(
            "Total: {} -> {} bytes, ratio: {:.2}x",
            total_uncompressed, total_compressed, compression_ratio
        );

        // Expected ratio ~28.12 (within 1%)
        let expected_ratio = 6.1;
        let tolerance = 0.01;
        assert!(
            compression_ratio >= expected_ratio * (1.0 - tolerance)
                && compression_ratio <= expected_ratio * (1.0 + tolerance),
            "Compression ratio {:.2}x outside expected range",
            compression_ratio
        );
    }
}
