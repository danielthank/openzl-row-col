# OpenZL Evaluation Repository

Consolidated Rust workspace for evaluating OpenZL compression performance on OpenTelemetry data.

## Project Status

**✅ All Phases Complete!**

All implementation phases (0-7) have been successfully completed:
- ✅ Cargo workspace with 5 crates (openzl-sys, openzl, otel-data, rebatch, benchmarks)
- ✅ Git submodules (openzl@evaluation, otel-arrow@main)
- ✅ OTAP parser removed (not needed)
- ✅ otel-data crate with multipart reader/writer for OTLP
- ✅ rebatch tool (Rust port with CLI)
- ✅ benchmarks (Rust port with direct library usage, no external processes)
- ✅ Python training script updated
- ✅ Testdata files copied

See [STATUS.md](STATUS.md) for detailed implementation status.

## Quick Start

### Prerequisites

```bash
# Rust 1.87.0+ with edition 2024 support
rustup update

# Python 3 for training script
python3 --version

# CMake and C++ compiler for OpenZL
cmake --version
g++ --version
```

### Build

```bash
# Clone and setup submodules
git submodule update --init --recursive
cd otel-arrow && git submodule update --init --recursive && cd ..

# Build OpenZL (already done if you see openzl/build-install/)
# cd openzl/build-install && cmake -DCMAKE_INSTALL_PREFIX=$(pwd) -DOPENZL_BUILD_PROTOBUF_TOOLS=ON .. && make -j16 && cd ../..

# Build Rust crates (openzl-sys and openzl are ready)
cargo build --release -p openzl-sys
cargo build --release -p openzl
```

### Workflow

1. **Rebatch testdata**:
   ```bash
   cargo run --release --bin rebatch -- \
       --input testdata/astronomy-otelmetrics.zst \
       --mode metrics \
       --batch-size 10,100,1000
   ```

2. **Train compressors**:
   ```bash
   cd scripts && python3 train_compressors.py && cd ..
   ```

3. **Run benchmarks**:
   ```bash
   cargo run --release --bin benchmark
   cat data/batch_size_results.json
   ```

## Architecture

### Crates

- **openzl-sys**: FFI bindings to OpenZL C library (✅ complete)
- **openzl**: Safe Rust wrapper (Compressor, CCtx, DCtx) (✅ complete)
- **otel-data**: OTLP data structures and multipart reader/writer (✅ complete)
- **rebatch**: CLI tool for rebatching testdata (✅ complete)
- **benchmarks**: Performance benchmarking suite (✅ complete)

### Key Design Decisions

1. **No OTAP Parser**: Removed `otap_registerOtapCompressionGraph()` - not needed
2. **Direct Library Usage**: Benchmarks use `openzl::CCtx::compress()` instead of external `protobuf_cli` for accurate CPU time measurements
3. **Parallel Processing**: Benchmarks use rayon for parallel execution across batch sizes and compressors
4. **Git Submodules**:
   - `openzl` → danielthank/openzl@evaluation
   - `otel-arrow` → open-telemetry/otel-arrow@main
5. **Simplified Dependencies**: otel-data uses `opentelemetry-proto` instead of `otap-df-pdata` to avoid ~675 workspace dependency conflicts

## Development

### Building Individual Crates

```bash
cargo build -p openzl-sys    # FFI bindings
cargo build -p openzl        # Safe wrapper
cargo build -p otel-data     # OTLP data structures
cargo build -p rebatch       # Rebatch tool
cargo build -p benchmarks    # Benchmark suite

# Or build everything at once
cargo build --workspace --release
```

### Testing

```bash
cargo test -p openzl-sys
cargo test -p openzl
cargo test -p otel-data
```

## Files

```
15712/
├── Cargo.toml                      # Workspace root
├── STATUS.md                       # Detailed implementation status
├── README.md                       # This file
├── openzl/                         # Submodule (evaluation branch)
├── otel-arrow/                     # Submodule (main branch)
├── crates/
│   ├── openzl-sys/                # ✅ FFI bindings
│   ├── openzl/                    # ✅ Safe wrapper
│   ├── otel-data/                 # 🔄 OTLP/OTAP data
│   ├── rebatch/                   # ⏸️ CLI tool
│   └── benchmarks/                # ⏸️ Benchmarks
├── scripts/
│   └── train_compressors.py       # ✅ Training script
├── testdata/
│   ├── *.zst                      # ✅ Source data
│   └── generated/                 # Rebatched data (empty)
└── data/                          # Trained compressors (empty)
    ├── otap/
    ├── otlp_metrics/
    └── otlp_traces/
```

## Contributing

See [STATUS.md](STATUS.md) for remaining work and implementation details.

## License

Apache-2.0
