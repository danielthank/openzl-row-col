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

pub mod proto;
pub use proto::{ProtoDeserializer, ProtoSerializer};

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
pub(crate) fn is_error(report: ffi::ZL_Report) -> bool {
    // ZL_isError checks if _code != ZL_ErrorCode_no_error
    // SAFETY: Accessing union field _code is safe as it's always initialized
    unsafe { report._code != ffi::ZL_ErrorCode_ZL_ErrorCode_no_error }
}

/// Extract the value from a successful ZL_Report
#[inline]
pub(crate) fn get_value(report: ffi::ZL_Report) -> usize {
    // SAFETY: _value field is valid when is_error() returns false
    unsafe { report._value._value }
}

/// Extract the error code from a failed ZL_Report
#[inline]
pub(crate) fn get_error_code(report: ffi::ZL_Report) -> u32 {
    // SAFETY: Accessing union field _code is safe as it's always initialized
    unsafe { report._code }
}

/// Calculate the maximum size needed for compression output
///
/// This is a very conservative upper bound from OpenZL: `(size * 2) + 512 + 8`
#[inline]
pub(crate) fn compress_bound(size: usize) -> usize {
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
        cctx.attach_compressor(compressor.clone())
            .expect("Failed to attach compressor");

        // Compress
        let compressed = cctx.compress(data).expect("Compression failed");

        println!("Original size: {} bytes", data.len());
        println!("Compressed size: {} bytes", compressed.len());
        println!(
            "Compression ratio: {:.2}x",
            data.len() as f64 / compressed.len() as f64
        );

        // Decompress
        let mut dctx = DCtx::new().expect("Failed to create DCtx");
        let decompressed = dctx.decompress(&compressed).expect("Decompression failed");

        // Verify roundtrip
        assert_eq!(
            data.as_slice(),
            decompressed.as_slice(),
            "Roundtrip failed: data doesn't match"
        );
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
        assert!(
            decompressed.is_empty(),
            "Empty compressed should decompress to empty"
        );
    }

    #[test]
    fn test_large_repetitive_data() {
        // Create highly compressible data
        let data = vec![b'A'; 10000];

        let compressor = Arc::new(Compressor::new().unwrap());
        let mut cctx = CCtx::new().unwrap();
        cctx.attach_compressor(compressor).unwrap();

        let compressed = cctx.compress(&data).unwrap();

        println!(
            "Repetitive data - Original: {} bytes, Compressed: {} bytes, Ratio: {:.2}x",
            data.len(),
            compressed.len(),
            data.len() as f64 / compressed.len() as f64
        );

        // Should compress well
        assert!(
            compressed.len() < data.len(),
            "Repetitive data should compress"
        );

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

        println!(
            "Binary data - Original: {} bytes, Compressed: {} bytes",
            data.len(),
            compressed.len()
        );

        let mut dctx = DCtx::new().unwrap();
        let decompressed = dctx.decompress(&compressed).unwrap();

        assert_eq!(data, decompressed);
    }
}
