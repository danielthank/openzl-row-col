# OpenZL Evaluation Repository

Consolidated Rust workspace for evaluating OpenZL compression performance on OpenTelemetry data.

## Quick Start

### Prerequisites

```bash
# Rust 1.87.0+ with edition 2024 support
rustup update

# Go 1.21+ for data generation tools
go version

# uv for Python scripts
uv --version

# CMake and C++ compiler for OpenZL
cmake --version
g++ --version
```

### Build

```bash
# Clone and setup submodules
git submodule update --init --recursive

# Build OpenZL
cd openzl
mkdir -p build-install && cd build-install
cmake -DCMAKE_BUILD_TYPE=Release -DOPENZL_BUILD_MODE=opt -DOPENZL_BUILD_PROTOBUF_TOOLS=ON -DCMAKE_INSTALL_PREFIX=./ ..
make -j$(nproc) && make install
cd ../..

# Build Rust workspace
cargo build --release
```

### Full Workflow

The recommended workflow uses train/test split for proper evaluation:

```bash
# 1. Generate train/test data (splits raw data, generates all batch sizes)
cd scripts && bash generate.sh && cd ..

# 2. Train compressors on train data
cd scripts && uv run train_compressors.py && cd ..

# 3. Run benchmarks on test data
cargo run --release --bin benchmark -- --zstd-level 9 --iterations 3

# 4. Generate plots
cd scripts && uv run paper_plots.py && cd ..
```

## Tools

### Data Generation

#### generate.sh - Full Data Pipeline

Automated script that:
1. Splits raw OTel data into train (10% random sample) and test (90%) sets
2. Generates batched data for training (batch_size=1000, otlp format only)
3. Generates batched data for testing (all batch sizes, all formats)
4. Splits and generates TPC-H data similarly

```bash
cd scripts && bash generate.sh
```

#### split - Raw Data Splitter

Splits raw OTel .zst files by randomly sampling data points:

```bash
cd go && go run ./cmd/split \
    --input ../testdata/astronomy-otelmetrics.zst \
    --mode metrics \
    --train-ratio 0.01 \
    --seed 42 \
    --output-train ../data/split/train/astronomy-otelmetrics.zst \
    --output-test ../data/split/test/astronomy-otelmetrics.zst
```

Options:
- `--input <FILE>`: Input .zst file
- `--mode <metrics|traces>`: Data type
- `--train-ratio <RATIO>`: Fraction for training (default: 0.01 = 1%)
- `--seed <INT>`: Random seed for reproducibility (default: 42)
- `--output-train <FILE>`: Output train file
- `--output-test <FILE>`: Output test file

#### rebatch - OTel Batch Generator

Rebatches OTel data into various formats and batch sizes:

```bash
cd go && go run ./cmd/rebatch \
    --input ../data/split/test/astronomy-otelmetrics.zst \
    --mode metrics \
    --batch-size 10,100,1000,10000 \
    --format otlp,otap,otapnodict,otapdictperfile,otapnosort,otapnodedup \
    --output ../data/generated/test
```

Options:
- `--input <FILE>`: Input .zst file
- `--mode <metrics|traces|dump>`: Data type or dump mode
- `--batch-size <SIZES>`: Comma-separated batch sizes
- `--format <FORMATS>`: otlp, otlpmetricsdict, otlptracesdict, otap, otapnodict, otapdictperfile, otapnosort, otapnodedup
- `--output <PATH>`: Output directory (default: ../data/generated)
- `--dump-file <FILE>`: File to dump (for dump mode)

#### tpch-gen - TPC-H Data Generator

Generates TPC-H benchmark data in protobuf and Arrow formats:

```bash
cd go && go run ./cmd/tpch-gen \
    --data-dir ../data/tpch \
    --batch-size 100,1000,10000 \
    --format proto,arrow,arrownodict,arrowdictperfile \
    --tables lineitem \
    --output ../data/generated/test
```

Options:
- `--data-dir <PATH>`: Directory containing TPC-H .tbl files (required)
- `--batch-size <SIZES>`: Comma-separated batch sizes (default: 1000)
- `--format <FORMATS>`: proto, arrow, arrownodict, arrowdictperfile (default: proto,arrow)
- `--tables <TABLES>`: Comma-separated tables (default: lineitem,orders)
- `--output <PATH>`: Output directory (default: ../data/generated)

### Training

#### train_compressors.py

Trains OpenZL compressors using all payload files from training data:

```bash
cd scripts && uv run train_compressors.py --schema all
```

Options:
- `--schema <SCHEMA>`: Schema(s) to train: all, otel, tpch, or specific name (otlp_metrics, otlp_traces, tpch_proto)

Note: Only protobuf-based formats need OpenZL training. OTAP uses Arrow IPC format which is zstd-only.

### Benchmarking

```bash
cargo run --release --bin benchmark -- --zstd-level 9 --iterations 3
```

Options:
- `--zstd-level <1-22>`: Zstd compression level (default: 9)
- `--iterations <N>`: Iterations per dataset (default: 3)
- `--threads <N>`: Number of parallel threads (default: 8)
- `--data-dir <PATH>`: Data directory (default: data/generated/test/)
- `--compressor-dir <PATH>`: Compressor directory (default: data)
- `--dataset <all|otel|tpch>`: Filter datasets to benchmark (default: all)

Benchmark modes:
- **zstd + OpenZL**: otlp, otlpmetricsdict, otlptracesdict, tpch_proto
- **zstd-only**: otap, otapnodict, otapdictperfile, otapnosort, otapnodedup, arrow, arrownodict, arrowdictperfile

Output: `data/benchmark_{dataset}_zstd{level}_iter{N}.json`

### Visualization

#### paper_plots.py - Publication Plots

Generates plots for the research paper:

```bash
cd scripts && uv run paper_plots.py
```

Output: `data/paper_plots/`

#### visualize_batch_size.py - Detailed Analysis

Generates detailed benchmark visualization:

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

- **go/cmd/split**: CLI tool for splitting raw OTel data into train/test sets
- **go/cmd/rebatch**: CLI tool for rebatching OTel testdata
- **go/cmd/tpch-gen**: CLI tool for generating TPC-H benchmark data
- **go/pkg/arrowutil**: Arrow IPC producer with incremental dictionary support

### Scripts

- **scripts/generate.sh**: Full data generation pipeline with train/test split
- **scripts/train_compressors.py**: Train OpenZL compressors
- **scripts/paper_plots.py**: Generate publication plots
- **scripts/visualize_batch_size.py**: Generate detailed benchmark plots

## Directory Structure

```
15712/
├── Cargo.toml                      # Workspace root
├── README.md                       # This file
├── openzl/                         # Submodule (evaluation branch)
├── crates/
│   ├── openzl-sys/                 # FFI bindings
│   ├── openzl/                     # Safe wrapper
│   └── benchmarks/                 # Benchmarks
├── go/
│   ├── cmd/
│   │   ├── split/                  # Raw data splitter
│   │   ├── rebatch/                # OTel rebatch tool
│   │   └── tpch-gen/               # TPC-H data generator
│   └── pkg/
│       ├── arrowutil/              # Arrow IPC producer
│       ├── otlpmetricsdict/        # Dictionary-encoded OTLP metrics
│       ├── otlptracesdict/         # Dictionary-encoded OTLP traces
│       └── tpch/                   # TPC-H protobuf definitions
├── scripts/
│   ├── generate.sh                 # Data generation pipeline
│   ├── train_compressors.py        # Training script
│   ├── paper_plots.py              # Publication plots
│   └── visualize_batch_size.py     # Visualization script
├── testdata/
│   └── *.zst                       # Source OTel data
└── data/
    ├── tpch/                       # Source TPC-H data
    │   └── lineitem.tbl
    ├── split/                      # Split raw data
    │   ├── train/                  # 10% for training
    │   └── test/                   # 90% for testing
    ├── generated/                  # Generated benchmark data
    │   ├── train/                  # Training batches (batch_size=1000, otlp only)
    │   └── test/                   # Test batches (all sizes, all formats)
    ├── otlp_metrics/               # Trained compressor + training payloads
    │   ├── payload_*.bin
    │   └── trained.zlc
    ├── otlp_traces/
    │   ├── payload_*.bin
    │   └── trained.zlc
    └── tpch_proto/
        ├── payload_*.bin
        └── trained.zlc
```

## License

Apache-2.0
