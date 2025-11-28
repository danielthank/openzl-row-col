use std::collections::HashSet;
use std::env;
use std::path::{Path, PathBuf};

/// Recursively find all directories containing .a files
fn find_lib_dirs(root: &Path) -> HashSet<PathBuf> {
    let mut dirs = HashSet::new();
    if let Ok(entries) = std::fs::read_dir(root) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                dirs.extend(find_lib_dirs(&path));
            } else if path.extension().map(|e| e == "a").unwrap_or(false) {
                if let Some(parent) = path.parent() {
                    dirs.insert(parent.to_path_buf());
                }
            }
        }
    }
    dirs
}

fn main() {
    let openzl_root = env::var("OPENZL_ROOT").unwrap_or_else(|_| {
        let manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
        format!("{}/../../openzl/build-install", manifest_dir)
    });

    let openzl_source = env::var("OPENZL_SOURCE").unwrap_or_else(|_| {
        let manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
        format!("{}/../../openzl", manifest_dir)
    });

    println!("cargo:rerun-if-changed=wrapper.h");
    println!("cargo:rerun-if-env-changed=OPENZL_ROOT");

    // Add all directories containing static libraries
    for dir in find_lib_dirs(Path::new(&openzl_root)) {
        println!("cargo:rustc-link-search=native={}", dir.display());
    }

    // Link libraries (order matters for static linking)
    let libs = [
        // OpenZL core
        "openzl",
        "zstd",
        // Proto compression
        "protobuf_serializer",
        "openzl_cpp",
        // Protobuf and dependencies
        "protobuf",
        "utf8_validity",
        "utf8_range",
        // Abseil (protobuf dependency)
        "absl_str_format_internal",
        "absl_strings",
        "absl_strings_internal",
        "absl_string_view",
        "absl_hash",
        "absl_city",
        "absl_low_level_hash",
        "absl_raw_hash_set",
        "absl_hashtablez_sampler",
        "absl_time",
        "absl_time_zone",
        "absl_civil_time",
        "absl_int128",
        "absl_synchronization",
        "absl_graphcycles_internal",
        "absl_kernel_timeout_internal",
        "absl_stacktrace",
        "absl_symbolize",
        "absl_debugging_internal",
        "absl_demangle_internal",
        "absl_demangle_rust",
        "absl_decode_rust_punycode",
        "absl_utf8_for_code_point",
        "absl_leak_check",
        "absl_status",
        "absl_statusor",
        "absl_cord",
        "absl_cord_internal",
        "absl_cordz_functions",
        "absl_cordz_handle",
        "absl_cordz_info",
        "absl_cordz_sample_token",
        "absl_crc32c",
        "absl_crc_cord_state",
        "absl_crc_cpu_detect",
        "absl_crc_internal",
        "absl_log_internal_check_op",
        "absl_log_internal_conditions",
        "absl_log_internal_format",
        "absl_log_internal_globals",
        "absl_log_internal_log_sink_set",
        "absl_log_internal_message",
        "absl_log_internal_nullguard",
        "absl_log_internal_proto",
        "absl_log_internal_fnmatch",
        "absl_log_globals",
        "absl_log_severity",
        "absl_log_sink",
        "absl_vlog_config_internal",
        "absl_flags_commandlineflag",
        "absl_flags_commandlineflag_internal",
        "absl_flags_config",
        "absl_flags_internal",
        "absl_flags_marshalling",
        "absl_flags_parse",
        "absl_flags_private_handle_accessor",
        "absl_flags_program_name",
        "absl_flags_reflection",
        "absl_flags_usage",
        "absl_flags_usage_internal",
        "absl_exponential_biased",
        "absl_periodic_sampler",
        "absl_random_distributions",
        "absl_random_seed_sequences",
        "absl_random_internal_entropy_pool",
        "absl_random_internal_platform",
        "absl_random_internal_randen",
        "absl_random_internal_randen_hwaes",
        "absl_random_internal_randen_hwaes_impl",
        "absl_random_internal_randen_slow",
        "absl_random_internal_seed_material",
        "absl_random_seed_gen_exception",
        "absl_base",
        "absl_spinlock_wait",
        "absl_malloc_internal",
        "absl_raw_logging_internal",
        "absl_throw_delegate",
        "absl_strerror",
        "absl_examine_stack",
        "absl_tracing_internal",
    ];

    for lib in libs {
        println!("cargo:rustc-link-lib=static={}", lib);
    }
    println!("cargo:rustc-link-lib=dylib=stdc++");

    // Generate bindings
    let bindings = bindgen::Builder::default()
        .header("wrapper.h")
        .clang_arg(format!("-I{}/include", openzl_root))
        .clang_arg(format!("-I{}", openzl_source))
        .clang_arg("-xc++")
        .clang_arg("-std=c++17")
        .allowlist_type("ZL_.*")
        .allowlist_function("ZL_.*")
        .allowlist_var("ZL_.*")
        .derive_debug(true)
        .derive_default(true)
        .wrap_unsafe_ops(true)
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
        .generate()
        .expect("Unable to generate bindings");

    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Couldn't write bindings!");
}
