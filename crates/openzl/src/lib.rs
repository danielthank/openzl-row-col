//! Safe Rust wrapper for OpenZL compression
//!
//! This implementation uses direct FFI bindings to the OpenZL C library for optimal performance.
//!
//! # Architecture Note
//!
//! OpenZL uses a graph-based compression model where users either:
//! 1. Use pre-built profiles/graphs (like `ZL_GRAPH_COMPRESS_GENERIC`)
//! 2. Train custom compressors for specific data formats
//!
//! This wrapper uses simple one-pass compression for rapid prototyping.
//! A future version could support custom trained compressors.

use openzl_sys as ffi;
use std::ffi::c_void;
use std::path::Path;
use std::ptr::NonNull;
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum OpenZLError {
    #[error("Compression failed with error code: {0}")]
    CompressionFailed(u32),

    #[error("Decompression failed with error code: {0}")]
    DecompressionFailed(u32),

    #[error("Failed to get decompressed size, error code: {0}")]
    DecompressSizeFailed(u32),

    #[error("Buffer too small: need {needed} bytes, have {available}")]
    BufferTooSmall { needed: usize, available: usize },

    #[error("Compressor creation failed")]
    CompressorCreateFailed,

    #[error("Compressor deserialization failed with error code: {0}")]
    DeserializationFailed(u32),

    #[error("Failed to create compression context")]
    CCtxCreateFailed,

    #[error("Failed to create decompression context")]
    DCtxCreateFailed,

    #[error("Failed to attach compressor with error code: {0}")]
    AttachCompressorFailed(u32),

    #[error("Failed to set parameter with error code: {0}")]
    SetParameterFailed(u32),

    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, OpenZLError>;

/// Check if a ZL_Report contains an error
#[inline]
unsafe fn is_error(report: ffi::ZL_Report) -> bool {
    // ZL_isError checks if _code != ZL_ErrorCode_no_error
    // SAFETY: Accessing union field _code is safe as it's always initialized
    unsafe { report._code != ffi::ZL_ErrorCode_ZL_ErrorCode_no_error }
}

/// Extract the value from a successful ZL_Report
#[inline]
unsafe fn get_value(report: ffi::ZL_Report) -> usize {
    // SAFETY: _value field is valid when is_error() returns false
    unsafe { report._value._value }
}

/// Extract the error code from a failed ZL_Report
#[inline]
unsafe fn get_error_code(report: ffi::ZL_Report) -> u32 {
    // SAFETY: Accessing union field _code is safe as it's always initialized
    unsafe { report._code }
}

/// Calculate the maximum size needed for compression output
///
/// This is a very conservative upper bound from OpenZL: `(size * 2) + 512 + 8`
#[inline]
fn compress_bound(size: usize) -> usize {
    (size * 2) + 512 + 8
}

/// A loaded/trained OpenZL compressor
///
/// This type represents a compressor that can be:
/// - Created fresh (with default configuration)
/// - Loaded from a .zl file (trained/serialized compressor)
/// - Deserialized from bytes
///
/// The compressor can then be attached to a CCtx for stateful compression.
pub struct Compressor {
    ptr: NonNull<ffi::ZL_Compressor>,
}

impl Compressor {
    /// Create a new compressor with the generic compression graph
    ///
    /// This creates a compressor pre-configured with `ZL_GRAPH_COMPRESS_GENERIC`,
    /// which is a default compression graph that works for any stream type.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use openzl::Compressor;
    /// let compressor = Compressor::new().unwrap();
    /// ```
    pub fn new() -> Result<Self> {
        unsafe {
            let ptr = ffi::ZL_Compressor_create();
            let ptr = NonNull::new(ptr).ok_or(OpenZLError::CompressorCreateFailed)?;

            // Set format version to max
            let report = ffi::ZL_Compressor_setParameter(
                ptr.as_ptr(),
                ffi::ZL_CParam_ZL_CParam_formatVersion,
                ffi::ZL_MAX_FORMAT_VERSION as i32,
            );
            if is_error(report) {
                ffi::ZL_Compressor_free(ptr.as_ptr());
                return Err(OpenZLError::SetParameterFailed(get_error_code(report)));
            }

            // Select the generic compression graph
            let graph_id = ffi::ZL_GraphID {
                gid: ffi::ZL_StandardGraphID_ZL_StandardGraphID_compress_generic,
            };
            let report = ffi::ZL_Compressor_selectStartingGraphID(ptr.as_ptr(), graph_id);
            if is_error(report) {
                ffi::ZL_Compressor_free(ptr.as_ptr());
                return Err(OpenZLError::SetParameterFailed(get_error_code(report)));
            }

            Ok(Compressor { ptr })
        }
    }

    /// Load a compressor from a .zl file
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the .zl file containing a serialized compressor
    ///
    /// # Example
    ///
    /// ```no_run
    /// use openzl::Compressor;
    /// let compressor = Compressor::load_from_file("trained.zl").unwrap();
    /// ```
    pub fn load_from_file(path: impl AsRef<Path>) -> Result<Self> {
        let bytes = std::fs::read(path)?;
        Self::deserialize(&bytes)
    }

    /// Deserialize a compressor from bytes
    ///
    /// # Arguments
    ///
    /// * `bytes` - Serialized compressor data (contents of a .zl file)
    pub fn deserialize(bytes: &[u8]) -> Result<Self> {
        unsafe {
            // Create compressor
            let compressor_ptr = ffi::ZL_Compressor_create();
            if compressor_ptr.is_null() {
                return Err(OpenZLError::CompressorCreateFailed);
            }

            // Create deserializer
            let deserializer = ffi::ZL_CompressorDeserializer_create();
            if deserializer.is_null() {
                ffi::ZL_Compressor_free(compressor_ptr);
                return Err(OpenZLError::CompressorCreateFailed);
            }

            // Deserialize
            let report = ffi::ZL_CompressorDeserializer_deserialize(
                deserializer,
                compressor_ptr,
                bytes.as_ptr() as *const c_void,
                bytes.len(),
            );

            // Free deserializer (no longer needed)
            ffi::ZL_CompressorDeserializer_free(deserializer);

            if is_error(report) {
                ffi::ZL_Compressor_free(compressor_ptr);
                return Err(OpenZLError::DeserializationFailed(get_error_code(report)));
            }

            // Set format version to max for deserialized compressors
            let report = ffi::ZL_Compressor_setParameter(
                compressor_ptr,
                ffi::ZL_CParam_ZL_CParam_formatVersion,
                ffi::ZL_MAX_FORMAT_VERSION as i32,
            );
            if is_error(report) {
                ffi::ZL_Compressor_free(compressor_ptr);
                return Err(OpenZLError::SetParameterFailed(get_error_code(report)));
            }

            Ok(Compressor {
                ptr: NonNull::new_unchecked(compressor_ptr),
            })
        }
    }

    /// Get the raw pointer (for internal use)
    #[inline]
    pub(crate) fn as_ptr(&self) -> *mut ffi::ZL_Compressor {
        self.ptr.as_ptr()
    }
}

// Compressor is Send + Sync because OpenZL compressor objects are thread-safe
unsafe impl Send for Compressor {}
unsafe impl Sync for Compressor {}

impl Drop for Compressor {
    fn drop(&mut self) {
        unsafe {
            ffi::ZL_Compressor_free(self.ptr.as_ptr());
        }
    }
}

/// Compression context for stateful compression
///
/// A compression context allows you to:
/// - Attach a loaded/trained compressor
/// - Set compression parameters
/// - Compress data using the attached compressor
///
/// # Example
///
/// ```no_run
/// use openzl::{Compressor, CCtx, CParam, MAX_FORMAT_VERSION};
/// use std::sync::Arc;
///
/// let compressor = Arc::new(Compressor::load_from_file("trained.zl").unwrap());
/// let mut cctx = CCtx::new().unwrap();
/// cctx.attach_compressor(compressor).unwrap();
/// cctx.set_parameter(CParam::FormatVersion, MAX_FORMAT_VERSION).unwrap();
///
/// let data = b"Hello, world!";
/// let compressed = cctx.compress(data).unwrap();
/// ```
pub struct CCtx {
    ptr: NonNull<ffi::ZL_CCtx>,
    _compressor: Option<Arc<Compressor>>,
}

impl CCtx {
    /// Create a new compression context
    pub fn new() -> Result<Self> {
        unsafe {
            let ptr = ffi::ZL_CCtx_create();
            NonNull::new(ptr)
                .map(|ptr| CCtx {
                    ptr,
                    _compressor: None,
                })
                .ok_or(OpenZLError::CCtxCreateFailed)
        }
    }

    /// Attach a compressor to this context
    ///
    /// The compressor will be referenced and kept alive for the lifetime of this context.
    pub fn attach_compressor(&mut self, compressor: Arc<Compressor>) -> Result<()> {
        unsafe {
            let report = ffi::ZL_CCtx_refCompressor(self.ptr.as_ptr(), compressor.as_ptr());

            if is_error(report) {
                return Err(OpenZLError::AttachCompressorFailed(get_error_code(report)));
            }

            self._compressor = Some(compressor);
            Ok(())
        }
    }

    /// Set a compression parameter
    pub fn set_parameter(&mut self, param: CParam, value: i32) -> Result<()> {
        unsafe {
            let report = ffi::ZL_CCtx_setParameter(self.ptr.as_ptr(), param.to_ffi(), value);

            if is_error(report) {
                return Err(OpenZLError::SetParameterFailed(get_error_code(report)));
            }

            Ok(())
        }
    }

    /// Compress data using the attached compressor (simple mode)
    ///
    /// This method works with generic compressors but may fail with trained compressors
    /// that require metadata. For trained compressors, use `compress_with_metadata` instead.
    ///
    /// # Panics
    ///
    /// Panics if no compressor has been attached via `attach_compressor`.
    pub fn compress(&mut self, data: &[u8]) -> Result<Vec<u8>> {
        if data.is_empty() {
            return Ok(Vec::new());
        }

        unsafe {
            let bound = compress_bound(data.len());
            let mut compressed = vec![0u8; bound];

            let report = ffi::ZL_CCtx_compress(
                self.ptr.as_ptr(),
                compressed.as_mut_ptr() as *mut c_void,
                compressed.len(),
                data.as_ptr() as *const c_void,
                data.len(),
            );

            if is_error(report) {
                // Get detailed error context string from OpenZL
                let error_str_ptr = ffi::ZL_CCtx_getErrorContextString(self.ptr.as_ptr(), report);
                if !error_str_ptr.is_null() {
                    let error_str = std::ffi::CStr::from_ptr(error_str_ptr)
                        .to_string_lossy()
                        .into_owned();
                    eprintln!("OpenZL compression error details:\n{}", error_str);
                }
                return Err(OpenZLError::CompressionFailed(get_error_code(report)));
            }

            let compressed_size = get_value(report);
            compressed.truncate(compressed_size);
            Ok(compressed)
        }
    }

    /// Compress data using typed references with clustering metadata
    ///
    /// This method is required for trained compressors (.zlc files) that use clustering.
    /// It creates a TypedRef with clustering metadata and uses the multi-typed compression API.
    ///
    /// # Parameters
    ///
    /// - `data`: The data to compress
    /// - `clustering_tag`: The clustering tag value (typically 0 for single-input compression)
    ///
    /// # Example
    ///
    /// ```no_run
    /// use openzl::{Compressor, CCtx};
    /// use std::sync::Arc;
    ///
    /// let compressor = Arc::new(Compressor::load_from_file("trained.zlc").unwrap());
    /// let mut cctx = CCtx::new().unwrap();
    /// cctx.attach_compressor(compressor).unwrap();
    ///
    /// let data = b"protobuf binary data...";
    /// let compressed = cctx.compress_with_metadata(data, 0).unwrap();
    /// ```
    pub fn compress_with_metadata(&mut self, data: &[u8], clustering_tag: i32) -> Result<Vec<u8>> {
        if data.is_empty() {
            return Ok(Vec::new());
        }

        unsafe {
            // Create a TypedRef for the data as a serial (byte stream) input
            let typed_ref = ffi::ZL_TypedRef_createSerial(
                data.as_ptr() as *const c_void,
                data.len(),
            );

            if typed_ref.is_null() {
                return Err(OpenZLError::CompressionFailed(0));
            }

            // Set clustering metadata (ZL_CLUSTERING_TAG_METADATA_ID = 0)
            let typed_ref_as_data = typed_ref as *mut ffi::ZL_Data;
            let metadata_report = ffi::ZL_Data_setIntMetadata(
                typed_ref_as_data,
                0, // ZL_CLUSTERING_TAG_METADATA_ID
                clustering_tag,
            );

            if is_error(metadata_report) {
                ffi::ZL_TypedRef_free(typed_ref);
                return Err(OpenZLError::SetParameterFailed(get_error_code(metadata_report)));
            }

            // Allocate output buffer
            let bound = compress_bound(data.len());
            let mut compressed = vec![0u8; bound];

            // Compress using compressMultiTypedRef
            let mut inputs = vec![typed_ref as *const ffi::ZL_Input];
            let report = ffi::ZL_CCtx_compressMultiTypedRef(
                self.ptr.as_ptr(),
                compressed.as_mut_ptr() as *mut c_void,
                compressed.len(),
                inputs.as_mut_ptr(),
                inputs.len(),
            );

            // Clean up TypedRef
            ffi::ZL_TypedRef_free(typed_ref);

            if is_error(report) {
                // Get detailed error context string from OpenZL
                let error_str_ptr = ffi::ZL_CCtx_getErrorContextString(self.ptr.as_ptr(), report);
                if !error_str_ptr.is_null() {
                    let error_str = std::ffi::CStr::from_ptr(error_str_ptr)
                        .to_string_lossy()
                        .into_owned();
                    eprintln!("OpenZL compression error details:\n{}", error_str);
                }
                return Err(OpenZLError::CompressionFailed(get_error_code(report)));
            }

            let compressed_size = report._value._value;
            compressed.truncate(compressed_size);
            Ok(compressed)
        }
    }
}

// CCtx is Send because OpenZL contexts are thread-safe (though not Sync - not shared between threads)
unsafe impl Send for CCtx {}

impl Drop for CCtx {
    fn drop(&mut self) {
        unsafe {
            ffi::ZL_CCtx_free(self.ptr.as_ptr());
        }
    }
}

/// Decompression context
///
/// # Example
///
/// ```no_run
/// use openzl::DCtx;
///
/// # let compressed_data = vec![0u8; 100];
/// let mut dctx = DCtx::new().unwrap();
/// let decompressed = dctx.decompress(&compressed_data).unwrap();
/// ```
pub struct DCtx {
    ptr: NonNull<ffi::ZL_DCtx>,
}

impl DCtx {
    /// Create a new decompression context
    pub fn new() -> Result<Self> {
        unsafe {
            let ptr = ffi::ZL_DCtx_create();
            NonNull::new(ptr)
                .map(|ptr| DCtx { ptr })
                .ok_or(OpenZLError::DCtxCreateFailed)
        }
    }

    /// Decompress data
    pub fn decompress(&mut self, compressed: &[u8]) -> Result<Vec<u8>> {
        if compressed.is_empty() {
            return Ok(Vec::new());
        }

        unsafe {
            // Get decompressed size
            let size_report = ffi::ZL_getDecompressedSize(
                compressed.as_ptr() as *const c_void,
                compressed.len(),
            );

            if is_error(size_report) {
                return Err(OpenZLError::DecompressSizeFailed(get_error_code(
                    size_report,
                )));
            }

            let decompressed_size = get_value(size_report);
            let mut decompressed = vec![0u8; decompressed_size];

            // Decompress
            let report = ffi::ZL_DCtx_decompress(
                self.ptr.as_ptr(),
                decompressed.as_mut_ptr() as *mut c_void,
                decompressed.len(),
                compressed.as_ptr() as *const c_void,
                compressed.len(),
            );

            if is_error(report) {
                return Err(OpenZLError::DecompressionFailed(get_error_code(report)));
            }

            Ok(decompressed)
        }
    }
}

// DCtx is Send because OpenZL contexts are thread-safe (though not Sync - not shared between threads)
unsafe impl Send for DCtx {}

impl Drop for DCtx {
    fn drop(&mut self) {
        unsafe {
            ffi::ZL_DCtx_free(self.ptr.as_ptr());
        }
    }
}

// Constants
pub const MAX_FORMAT_VERSION: i32 = ffi::ZL_MAX_FORMAT_VERSION as i32;

// Parameters
#[derive(Debug, Clone, Copy)]
pub enum CParam {
    FormatVersion,
}

impl CParam {
    fn to_ffi(&self) -> u32 {
        match self {
            CParam::FormatVersion => ffi::ZL_CParam_ZL_CParam_formatVersion,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_compress_decompress() {
        // Test with simple text data
        let data = b"Hello, OpenZL! This is a test of compression and decompression.";

        // Create compressor
        let compressor = Arc::new(Compressor::new().expect("Failed to create compressor"));

        // Create compression context
        let mut cctx = CCtx::new().expect("Failed to create CCtx");
        cctx.attach_compressor(compressor.clone()).expect("Failed to attach compressor");

        // Compress
        let compressed = cctx.compress(data).expect("Compression failed");

        println!("Original size: {} bytes", data.len());
        println!("Compressed size: {} bytes", compressed.len());
        println!("Compression ratio: {:.2}x", data.len() as f64 / compressed.len() as f64);

        // Decompress
        let mut dctx = DCtx::new().expect("Failed to create DCtx");
        let decompressed = dctx.decompress(&compressed).expect("Decompression failed");

        // Verify roundtrip
        assert_eq!(data.as_slice(), decompressed.as_slice(), "Roundtrip failed: data doesn't match");
    }

    #[test]
    fn test_empty_data() {
        let data = b"";

        let compressor = Arc::new(Compressor::new().unwrap());
        let mut cctx = CCtx::new().unwrap();
        cctx.attach_compressor(compressor).unwrap();

        let compressed = cctx.compress(data).unwrap();
        assert!(compressed.is_empty(), "Empty data should compress to empty");

        let mut dctx = DCtx::new().unwrap();
        let decompressed = dctx.decompress(&compressed).unwrap();
        assert!(decompressed.is_empty(), "Empty compressed should decompress to empty");
    }

    #[test]
    fn test_large_repetitive_data() {
        // Create highly compressible data
        let data = vec![b'A'; 10000];

        let compressor = Arc::new(Compressor::new().unwrap());
        let mut cctx = CCtx::new().unwrap();
        cctx.attach_compressor(compressor).unwrap();

        let compressed = cctx.compress(&data).unwrap();

        println!("Repetitive data - Original: {} bytes, Compressed: {} bytes, Ratio: {:.2}x",
                 data.len(), compressed.len(), data.len() as f64 / compressed.len() as f64);

        // Should compress well
        assert!(compressed.len() < data.len(), "Repetitive data should compress");

        let mut dctx = DCtx::new().unwrap();
        let decompressed = dctx.decompress(&compressed).unwrap();

        assert_eq!(data, decompressed);
    }

    #[test]
    fn test_binary_data() {
        // Test with binary data (simulating protobuf-like data)
        let data: Vec<u8> = (0..=255).cycle().take(1024).collect();

        let compressor = Arc::new(Compressor::new().unwrap());
        let mut cctx = CCtx::new().unwrap();
        cctx.attach_compressor(compressor).unwrap();

        let compressed = cctx.compress(&data).unwrap();

        println!("Binary data - Original: {} bytes, Compressed: {} bytes",
                 data.len(), compressed.len());

        let mut dctx = DCtx::new().unwrap();
        let decompressed = dctx.decompress(&compressed).unwrap();

        assert_eq!(data, decompressed);
    }

    #[test]
    fn test_loaded_compressor() {
        // This test verifies that we can load a compressor from disk
        // It will be skipped if the test file doesn't exist

        let test_compressor_path = "data/otap/trained.zlc";
        if !std::path::Path::new(test_compressor_path).exists() {
            println!("Skipping test_loaded_compressor: {} not found", test_compressor_path);
            return;
        }

        let data = b"Test data for loaded compressor";

        // Load trained compressor
        let compressor = Arc::new(
            Compressor::load_from_file(test_compressor_path)
                .expect("Failed to load trained compressor")
        );

        // Compress
        let mut cctx = CCtx::new().unwrap();
        cctx.attach_compressor(compressor).unwrap();

        let compressed = cctx.compress(data).unwrap();

        println!("Loaded compressor - Original: {} bytes, Compressed: {} bytes",
                 data.len(), compressed.len());

        // Decompress
        let mut dctx = DCtx::new().unwrap();
        let decompressed = dctx.decompress(&compressed).unwrap();

        assert_eq!(data.as_slice(), decompressed.as_slice());
    }

    #[test]
    fn test_real_otap_data_with_generic_compressor() {
        // This test demonstrates that the generic compressor works with any data
        // including real OTAP payloads from OpenTelemetry

        let payload_path = "testdata/otap_payload.bin";

        if !std::path::Path::new(payload_path).exists() {
            println!("Skipping test_real_otap_data_with_generic_compressor: {} not found", payload_path);
            return;
        }

        // Read real OTAP payload (868KB of OpenTelemetry metrics data)
        let payload_data = std::fs::read(payload_path).expect("Failed to read payload");
        println!("Loaded OTAP payload: {} bytes", payload_data.len());

        // Use generic compressor (automatically selected in Compressor::new())
        let compressor = Arc::new(Compressor::new().expect("Failed to create compressor"));
        println!("Created generic compressor");

        // Compress
        let mut cctx = CCtx::new().expect("Failed to create CCtx");
        cctx.attach_compressor(compressor.clone()).expect("Failed to attach compressor");

        let compressed = cctx.compress(&payload_data).expect("Compression failed");

        let compression_ratio = payload_data.len() as f64 / compressed.len() as f64;
        println!("OTAP Compression - Original: {} bytes, Compressed: {} bytes, Ratio: {:.2}x",
                 payload_data.len(), compressed.len(), compression_ratio);

        // Verify compression actually happened
        assert!(compressed.len() < payload_data.len(),
                "Compressed size should be smaller than original");

        // Decompress (note: decompression doesn't need the trained compressor)
        let mut dctx = DCtx::new().expect("Failed to create DCtx");
        let decompressed = dctx.decompress(&compressed).expect("Decompression failed");

        println!("Decompressed: {} bytes", decompressed.len());

        // Verify roundtrip
        assert_eq!(payload_data.len(), decompressed.len(),
                   "Decompressed size doesn't match original");
        assert_eq!(payload_data, decompressed,
                   "Roundtrip failed: decompressed data doesn't match original");

        println!("✓ Roundtrip verification successful!");
    }

    #[test]
    fn test_trained_compressor_with_matching_data() {
        // This test uses a training payload (same format used during training)
        // to verify that trained compressors work correctly

        let compressor_path = "testdata/otlp_metrics_compressor.zlc";
        let payload_path = "testdata/otlp_metrics_training_payload.bin";

        if !std::path::Path::new(compressor_path).exists() {
            println!("Skipping test_trained_compressor_with_matching_data: {} not found", compressor_path);
            return;
        }

        if !std::path::Path::new(payload_path).exists() {
            println!("Skipping test_trained_compressor_with_matching_data: {} not found", payload_path);
            return;
        }

        // Read training payload (this is from the same dataset used to train the compressor)
        let payload_data = std::fs::read(payload_path).expect("Failed to read payload");
        println!("Loaded training payload: {} bytes", payload_data.len());

        // Load trained OTLP metrics compressor
        let compressor = Arc::new(
            Compressor::load_from_file(compressor_path)
                .expect("Failed to load trained OTLP metrics compressor")
        );
        println!("Loaded trained compressor from {}", compressor_path);

        // Compress using trained compressor
        let mut cctx = CCtx::new().expect("Failed to create CCtx");
        cctx.attach_compressor(compressor.clone()).expect("Failed to attach compressor");

        let compressed = cctx.compress(&payload_data).expect("Compression failed");

        let compression_ratio = payload_data.len() as f64 / compressed.len() as f64;
        println!("Trained Compressor - Original: {} bytes, Compressed: {} bytes, Ratio: {:.2}x",
                 payload_data.len(), compressed.len(), compression_ratio);

        // Verify compression actually happened
        assert!(compressed.len() < payload_data.len(),
                "Compressed size should be smaller than original");

        // Decompress (note: decompression doesn't need the trained compressor)
        let mut dctx = DCtx::new().expect("Failed to create DCtx");
        let decompressed = dctx.decompress(&compressed).expect("Decompression failed");

        println!("Decompressed: {} bytes", decompressed.len());

        // Verify roundtrip
        assert_eq!(payload_data.len(), decompressed.len(),
                   "Decompressed size doesn't match original");
        assert_eq!(payload_data, decompressed,
                   "Roundtrip failed: decompressed data doesn't match original");

        println!("✓ Trained compressor roundtrip verification successful!");
    }

    #[test]
    fn test_trained_compressor_with_metadata() {
        // Test the compress_with_metadata API with a trained compressor

        let compressor_path = "testdata/otlp_metrics_compressor.zlc";
        let payload_path = "testdata/otlp_metrics_payload.bin";

        if !std::path::Path::new(compressor_path).exists() {
            println!("Skipping test_trained_compressor_with_metadata: {} not found", compressor_path);
            return;
        }

        if !std::path::Path::new(payload_path).exists() {
            println!("Skipping test_trained_compressor_with_metadata: {} not found", payload_path);
            return;
        }

        let payload_data = std::fs::read(payload_path).expect("Failed to read payload");
        println!("Loaded payload: {} bytes", payload_data.len());

        // Load trained compressor
        let compressor = Arc::new(
            Compressor::load_from_file(compressor_path)
                .expect("Failed to load trained compressor")
        );

        // Create compression context and attach compressor
        let mut cctx = CCtx::new().expect("Failed to create CCtx");
        cctx.attach_compressor(compressor).expect("Failed to attach compressor");

        // Compress using the new metadata API
        let compressed = cctx.compress_with_metadata(&payload_data, 0)
            .expect("Compression with metadata failed");

        let compression_ratio = payload_data.len() as f64 / compressed.len() as f64;
        println!("Compressed with metadata: {} -> {} bytes ({:.2}x)",
                 payload_data.len(), compressed.len(), compression_ratio);

        // Decompress
        let mut dctx = DCtx::new().expect("Failed to create DCtx");
        let decompressed = dctx.decompress(&compressed).expect("Decompression failed");

        println!("Decompressed: {} bytes", decompressed.len());

        // Verify roundtrip
        assert_eq!(payload_data.len(), decompressed.len(),
                   "Decompressed size doesn't match original");
        assert_eq!(payload_data, decompressed,
                   "Roundtrip failed: decompressed data doesn't match original");

        println!("✓ Metadata API roundtrip successful!");
    }
}
