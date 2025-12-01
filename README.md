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

1. **Rebatch OTel testdata**:
   ```bash
   cd go && go run ./cmd/rebatch \
       --input ../testdata/astronomy-otelmetrics.zst \
       --mode metrics \
       --batch-size 10,100,1000 \
       --format otlp,otap,otapnodict,otapdictperfile
   ```
   Options:
   - `--input <FILE>`: Input .zst file
   - `--mode <metrics|traces|dump>`: Data type or dump mode
   - `--batch-size <SIZES>`: Comma-separated batch sizes
   - `--format <FORMATS>`: otlp, otap, otapnodict, otapdictperfile
   - `--dump-file <FILE>`: File to dump (for dump mode)

2. **Generate TPC-H data**:
   ```bash
   cd go && go run ./cmd/tpch-gen \
       --data-dir /path/to/tpch/tbl/files \
       --batch-size 1000,10000 \
       --format proto,arrow,arrownodict,arrowdictperfile \
       --tables lineitem,orders
   ```
   Options:
   - `--data-dir <PATH>`: Directory containing TPC-H .tbl files (required)
   - `--batch-size <SIZES>`: Comma-separated batch sizes (default: 1000)
   - `--format <FORMATS>`: proto, arrow, arrownodict, arrowdictperfile (default: proto,arrow)
   - `--tables <TABLES>`: Comma-separated tables (default: lineitem,orders)
   - `--output <PATH>`: Output directory (default: ../data/generated)

3. **Train compressors**:
   ```bash
   cd scripts && python3 train_compressors.py --schema all && cd ..
   ```
   Options:
   - `--schema <SCHEMA>`: Schema(s) to train: all, otel, tpch, or specific name (otap, otlp_metrics, otlp_traces, tpch_proto)

4. **Run benchmarks**:
   ```bash
   cargo run --release --bin benchmark -- --zstd-level 7 --iterations 3
   ```
   Options:
   - `--zstd-level <1-22>`: Zstd compression level (default: 7)
   - `--iterations <N>`: Iterations per dataset (default: 3)
   - `--data-dir <PATH>`: Data directory (default: data/generated/)
   - `--compressor-dir <PATH>`: Compressor directory (default: data)
   - `--dataset <all|otel|tpch>`: Filter datasets to benchmark (default: all)

   Output: `data/benchmark_results_zstd{level}_iter{N}.json`

5. **Visualize results**:
   ```bash
   cd scripts && uv run visualize_batch_size.py [INPUT] [--output-dir DIR]
   ```
   - `INPUT`: Path to JSON results (default: ../data/benchmark_results.json)
   - `--output-dir`: Output directory for plots (default: {INPUT}-plots)

## Architecture

### Crates

- **openzl-sys**: FFI bindings to OpenZL C library
- **openzl**: Safe Rust wrapper (Compressor, CCtx, DCtx)
- **benchmarks**: Performance benchmarking suite

### Go

- **go/cmd/rebatch**: CLI tool for rebatching OTel testdata
- **go/cmd/tpch-gen**: CLI tool for generating TPC-H benchmark data
- **go/pkg/arrowutil**: Arrow IPC producer with incremental dictionary support

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
│   ├── cmd/
│   │   ├── rebatch/                # OTel rebatch tool
│   │   └── tpch-gen/               # TPC-H data generator
│   └── pkg/
│       ├── arrowutil/              # Arrow IPC producer
│       └── tpch/                   # TPC-H protobuf definitions
├── scripts/
│   ├── train_compressors.py        # Training script
│   └── visualize_batch_size.py     # Visualization script
├── testdata/
│   └── *.zst                       # Source OTel data
└── data/
    ├── generated/                  # Generated benchmark data
    ├── otap/                       # Trained compressors
    ├── otlp_metrics/
    └── otlp_traces/
```

## License

Apache-2.0
