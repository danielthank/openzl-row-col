use std::env;
use std::path::PathBuf;

fn main() {
    // Path to OpenZL installation
    let openzl_root = env::var("OPENZL_ROOT")
        .unwrap_or_else(|_| {
            let manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
            format!("{}/../../openzl/build-install", manifest_dir)
        });

    let openzl_include = format!("{}/include", openzl_root);

    // Check for lib in build-install directory, lib64, or lib directory
    let openzl_lib = if std::path::Path::new(&format!("{}/libopenzl.a", openzl_root)).exists() {
        // Library is directly in build-install
        openzl_root.clone()
    } else if std::path::Path::new(&format!("{}/lib64", openzl_root)).exists() {
        format!("{}/lib64", openzl_root)
    } else {
        format!("{}/lib", openzl_root)
    };

    println!("cargo:rerun-if-changed=wrapper.h");
    println!("cargo:rerun-if-env-changed=OPENZL_ROOT");

    // Link to OpenZL library (static)
    println!("cargo:rustc-link-search=native={}", openzl_lib);
    println!("cargo:rustc-link-lib=static=openzl");

    // Zstd is bundled with OpenZL - check for it in zstd_build/lib
    let zstd_lib = format!("{}/zstd_build/lib", openzl_root);
    if std::path::Path::new(&zstd_lib).exists() {
        println!("cargo:rustc-link-search=native={}", zstd_lib);
    }
    println!("cargo:rustc-link-lib=static=zstd");

    // Link C++ standard library (OpenZL uses C++)
    println!("cargo:rustc-link-lib=dylib=stdc++");

    // Generate bindings
    let bindings = bindgen::Builder::default()
        .header("wrapper.h")
        .clang_arg(format!("-I{}", openzl_include))
        .clang_arg("-xc++")  // Parse as C++ to handle extern "C" blocks
        .clang_arg("-std=c++17")  // Use C++17 standard
        // Core types and errors
        .allowlist_type("ZL_.*")
        .allowlist_function("ZL_.*")
        .allowlist_var("ZL_.*")
        // Generate simplified bindings
        .derive_debug(true)
        .derive_default(true)
        // Wrap extern blocks in unsafe for Rust 2024
        .wrap_unsafe_ops(true)
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
        .generate()
        .expect("Unable to generate bindings");

    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Couldn't write bindings!");
}
