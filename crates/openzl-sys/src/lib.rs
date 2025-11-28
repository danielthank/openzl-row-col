//! Low-level FFI bindings to Meta's OpenZL compression library
//!
//! This crate provides unsafe bindings to the OpenZL C API.
//! For a safe interface, use the `openzl` crate instead.
//!
//! # Safety
//!
//! All functions in this crate are `unsafe` as they directly call into C code.
//! Incorrect usage can lead to undefined behavior, memory leaks, or crashes.

#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(dead_code)]

// Include the generated bindings
include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bindings_exist() {
        // Basic smoke test to ensure bindings were generated
        // We'll just check that key types exist
        let _error_code = ZL_ErrorCode_ZL_ErrorCode_no_error;
    }
}
