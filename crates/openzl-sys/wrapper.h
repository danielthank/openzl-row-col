/*
 * Wrapper header for OpenZL C API bindings
 * This includes the essential OpenZL headers needed for compression/decompression
 */

// Core error handling and types
#include <openzl/zl_errors.h>
#include <openzl/zl_errors_types.h>
#include <openzl/zl_opaque_types.h>
#include <openzl/zl_version.h>

// Data structures
#include <openzl/zl_data.h>

// Compression and decompression APIs
#include <openzl/zl_compress.h>
#include <openzl/zl_decompress.h>
#include <openzl/zl_compressor.h>

// Input metadata (for clustering tags)
#include <openzl/zl_input.h>

// Compressor serialization (for loading .zl files)
#include <openzl/zl_compressor_serialization.h>

// Proto-aware compression (requires protobuf)
#include <tools/protobuf/proto_compress.h>
