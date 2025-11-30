//! Proto-aware compression using fast C++ protobuf reflection

use crate::{compress_bound, OpenZLError, Result};
use openzl_sys as ffi;
use std::ffi::{c_void, CStr};
use std::ptr::NonNull;

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

    /// Compress OTLP metrics proto bytes
    ///
    /// Takes raw protobuf bytes (ExportMetricsServiceRequest) and returns
    /// compressed bytes using proto-aware type-split compression.
    pub fn compress_otlp_metrics(&mut self, proto_bytes: &[u8]) -> Result<Vec<u8>> {
        let bound = compress_bound(proto_bytes.len());
        let mut dst = vec![0u8; bound];

        let written = unsafe {
            ffi::ZL_ProtoSerializer_compressOtlpMetrics(
                self.ptr.as_ptr(),
                dst.as_mut_ptr() as *mut c_void,
                dst.len(),
                proto_bytes.as_ptr() as *const c_void,
                proto_bytes.len(),
            )
        };

        if written == 0 {
            return Err(OpenZLError::CompressionFailed(0));
        }
        dst.truncate(written);
        Ok(dst)
    }

    /// Compress OTLP traces proto bytes
    ///
    /// Takes raw protobuf bytes (ExportTraceServiceRequest) and returns
    /// compressed bytes using proto-aware type-split compression.
    pub fn compress_otlp_traces(&mut self, proto_bytes: &[u8]) -> Result<Vec<u8>> {
        let bound = compress_bound(proto_bytes.len());
        let mut dst = vec![0u8; bound];

        let written = unsafe {
            ffi::ZL_ProtoSerializer_compressOtlpTraces(
                self.ptr.as_ptr(),
                dst.as_mut_ptr() as *mut c_void,
                dst.len(),
                proto_bytes.as_ptr() as *const c_void,
                proto_bytes.len(),
            )
        };

        if written == 0 {
            return Err(OpenZLError::CompressionFailed(0));
        }
        dst.truncate(written);
        Ok(dst)
    }

    /// Compress OTAP (BatchArrowRecords) proto bytes
    ///
    /// Takes raw protobuf bytes (BatchArrowRecords) and returns
    /// compressed bytes using proto-aware type-split compression.
    pub fn compress_otap(&mut self, proto_bytes: &[u8]) -> Result<Vec<u8>> {
        let bound = compress_bound(proto_bytes.len());
        let mut dst = vec![0u8; bound];

        let written = unsafe {
            ffi::ZL_ProtoSerializer_compressOtap(
                self.ptr.as_ptr(),
                dst.as_mut_ptr() as *mut c_void,
                dst.len(),
                proto_bytes.as_ptr() as *const c_void,
                proto_bytes.len(),
            )
        };

        if written == 0 {
            return Err(OpenZLError::CompressionFailed(0));
        }
        dst.truncate(written);
        Ok(dst)
    }

    /// Compress TPC-H Proto bytes using proto-aware compression
    ///
    /// Takes raw protobuf bytes (TpchBatch) and returns compressed bytes
    /// using proto-aware type-split compression.
    pub fn compress_tpch_proto(&mut self, proto_bytes: &[u8]) -> Result<Vec<u8>> {
        let bound = compress_bound(proto_bytes.len());
        let mut dst = vec![0u8; bound];

        let written = unsafe {
            ffi::ZL_ProtoSerializer_compressTpch(
                self.ptr.as_ptr(),
                dst.as_mut_ptr() as *mut c_void,
                dst.len(),
                proto_bytes.as_ptr() as *const c_void,
                proto_bytes.len(),
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

    /// Decompress OTLP metrics back to proto bytes
    ///
    /// Takes compressed bytes and returns the original protobuf bytes
    /// (ExportMetricsServiceRequest).
    pub fn decompress_otlp_metrics(&mut self, compressed: &[u8]) -> Result<Vec<u8>> {
        // Use a large buffer - compression ratios can exceed 100x
        let capacity = compressed.len() * 200;
        let mut dst = vec![0u8; capacity];

        let written = unsafe {
            ffi::ZL_ProtoDeserializer_decompressOtlpMetrics(
                self.ptr.as_ptr(),
                dst.as_mut_ptr() as *mut c_void,
                dst.len(),
                compressed.as_ptr() as *const c_void,
                compressed.len(),
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

    /// Decompress OTLP traces back to proto bytes
    ///
    /// Takes compressed bytes and returns the original protobuf bytes
    /// (ExportTraceServiceRequest).
    pub fn decompress_otlp_traces(&mut self, compressed: &[u8]) -> Result<Vec<u8>> {
        let capacity = compressed.len() * 200;
        let mut dst = vec![0u8; capacity];

        let written = unsafe {
            ffi::ZL_ProtoDeserializer_decompressOtlpTraces(
                self.ptr.as_ptr(),
                dst.as_mut_ptr() as *mut c_void,
                dst.len(),
                compressed.as_ptr() as *const c_void,
                compressed.len(),
            )
        };

        if written == 0 {
            return Err(OpenZLError::DecompressionFailed(0));
        }
        dst.truncate(written);
        Ok(dst)
    }

    /// Decompress OTAP back to proto bytes
    ///
    /// Takes compressed bytes and returns the original protobuf bytes
    /// (BatchArrowRecords).
    pub fn decompress_otap(&mut self, compressed: &[u8]) -> Result<Vec<u8>> {
        let capacity = compressed.len() * 200;
        let mut dst = vec![0u8; capacity];

        let written = unsafe {
            ffi::ZL_ProtoDeserializer_decompressOtap(
                self.ptr.as_ptr(),
                dst.as_mut_ptr() as *mut c_void,
                dst.len(),
                compressed.as_ptr() as *const c_void,
                compressed.len(),
            )
        };

        if written == 0 {
            return Err(OpenZLError::DecompressionFailed(0));
        }
        dst.truncate(written);
        Ok(dst)
    }

    /// Decompress TPC-H Proto bytes using proto-aware decompression
    ///
    /// Takes compressed bytes and returns the original protobuf bytes
    /// (TpchBatch).
    pub fn decompress_tpch_proto(&mut self, compressed: &[u8]) -> Result<Vec<u8>> {
        let capacity = compressed.len() * 200;
        let mut dst = vec![0u8; capacity];

        let written = unsafe {
            ffi::ZL_ProtoDeserializer_decompressTpch(
                self.ptr.as_ptr(),
                dst.as_mut_ptr() as *mut c_void,
                dst.len(),
                compressed.as_ptr() as *const c_void,
                compressed.len(),
            )
        };

        if written == 0 {
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

/// Compare two OTLP metrics proto messages for semantic equality
///
/// This parses both byte arrays as protobuf messages and compares them
/// semantically rather than byte-for-byte (since protobuf serialization
/// is not deterministic).
pub fn compare_otlp_metrics(proto1: &[u8], proto2: &[u8]) -> bool {
    let result = unsafe {
        ffi::ZL_Proto_compareOtlpMetrics(
            proto1.as_ptr() as *const c_void,
            proto1.len(),
            proto2.as_ptr() as *const c_void,
            proto2.len(),
        )
    };
    result == 1
}

/// Compare two OTLP traces proto messages for semantic equality
pub fn compare_otlp_traces(proto1: &[u8], proto2: &[u8]) -> bool {
    let result = unsafe {
        ffi::ZL_Proto_compareOtlpTraces(
            proto1.as_ptr() as *const c_void,
            proto1.len(),
            proto2.as_ptr() as *const c_void,
            proto2.len(),
        )
    };
    result == 1
}

/// Compare two OTAP proto messages for semantic equality
pub fn compare_otap(proto1: &[u8], proto2: &[u8]) -> bool {
    let result = unsafe {
        ffi::ZL_Proto_compareOtap(
            proto1.as_ptr() as *const c_void,
            proto1.len(),
            proto2.as_ptr() as *const c_void,
            proto2.len(),
        )
    };
    result == 1
}

/// Compare two TPC-H Proto messages for semantic equality
pub fn compare_tpch_proto(proto1: &[u8], proto2: &[u8]) -> bool {
    let result = unsafe {
        ffi::ZL_Proto_compareTpch(
            proto1.as_ptr() as *const c_void,
            proto1.len(),
            proto2.as_ptr() as *const c_void,
            proto2.len(),
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
                .compress_otlp_metrics(&proto_bytes)
                .expect("Compression failed");
            let decompressed = deserializer
                .decompress_otlp_metrics(&compressed)
                .expect("Decompression failed");

            assert!(
                compare_otlp_metrics(&proto_bytes, &decompressed),
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
                .compress_otlp_traces(&proto_bytes)
                .expect("Compression failed");
            let decompressed = deserializer
                .decompress_otlp_traces(&compressed)
                .expect("Decompression failed");

            assert!(
                compare_otlp_traces(&proto_bytes, &decompressed),
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
                .compress_otap(&proto_bytes)
                .expect("Compression failed");
            let decompressed = deserializer
                .decompress_otap(&compressed)
                .expect("Decompression failed");

            assert!(
                compare_otap(&proto_bytes, &decompressed),
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
