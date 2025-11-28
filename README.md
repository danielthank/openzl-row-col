# OpenZL Evaluation Repository

Consolidated Rust workspace for evaluating OpenZL compression performance on OpenTelemetry data.

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

# Build OpenZL
cd openzl
mkdir -p build-install && cd build-install
cmake -DCMAKE_BUILD_TYPE=Release -DOPENZL_BUILD_MODE=opt -DOPENZL_BUILD_PROTOBUF_TOOLS=ON -DCMAKE_INSTALL_PREFIX=./ ..
make -j$(nproc) && make install
cd ../..

# Build Rust workspace
cargo build --release
```

### Workflow

1. **Rebatch testdata**:
   ```bash
   cd go && go run ./cmd/rebatch
   ```

2. **Train compressors**:
   ```bash
   cd scripts && python3 train_compressors.py && cd ..
   ```

3. **Run benchmarks**:
   ```bash
   cargo run --release --bin benchmark -- --zstd-level 4 --iterations 3
   ```
   Options:
   - `--zstd-level <1-22>`: Zstd compression level (default: 4)
   - `--iterations <N>`: Iterations per dataset (default: 1)
   - `--data-dir <PATH>`: Data directory (default: data/generated/)
   - `--compressor-dir <PATH>`: Compressor directory (default: data)

4. **Visualize results**:
   ```bash
   cd scripts && uv run visualize_batch_size.py [INPUT] [--output-dir DIR]
   ```
   - `INPUT`: Path to JSON results (default: ../data/benchmark_results.json)
   - `--output-dir`: Output directory for plots (default: ../data/plots)

## Architecture

### Crates

- **openzl-sys**: FFI bindings to OpenZL C library
- **openzl**: Safe Rust wrapper (Compressor, CCtx, DCtx)
- **benchmarks**: Performance benchmarking suite

### Go

- **go/cmd/rebatch**: CLI tool for rebatching testdata

### Scripts

- **scripts/train_compressors.py**: Train OpenZL compressors
- **scripts/visualize_batch_size.py**: Generate benchmark plots

## Files

```
15712/
├── Cargo.toml                      # Workspace root
├── README.md                       # This file
├── openzl/                         # Submodule (evaluation branch)
├── otel-arrow/                     # Submodule (main branch)
├── crates/
│   ├── openzl-sys/                 # FFI bindings
│   ├── openzl/                     # Safe wrapper
│   └── benchmarks/                 # Benchmarks
├── go/
│   └── cmd/rebatch/                # Rebatch tool (Go)
├── scripts/
│   ├── train_compressors.py        # Training script
│   └── visualize_batch_size.py     # Visualization script
├── testdata/
│   └── *.zst                       # Source data
└── data/
    ├── generated/                  # Rebatched data
    ├── otap/                       # Trained compressors
    ├── otlp_metrics/
    └── otlp_traces/
```

## License

Apache-2.0
