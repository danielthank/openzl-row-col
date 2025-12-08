#!/bin/bash
set -e

cd "$(dirname "$0")/../go"

BATCH_SIZES="10,50,100,500,1000,5000,10000,50000"
FORMATS="otlp,otap,otapnodict,otapdictperfile,otapnosort,otapnodedup"
TRAIN_RATIO="0.1"

# Training only needs batch=1000, otlp format (for OpenZL training)
TRAIN_BATCH_SIZES="1000"
TRAIN_FORMATS="otlp"

# Create split directories
mkdir -p ../data/split/train
mkdir -p ../data/split/test
mkdir -p ../data/generated/train
mkdir -p ../data/generated/test

echo "=== Step 1: Splitting raw data into train/test ==="

for input in ../testdata/*.zst; do
    if [[ "$input" == *"metrics"* ]]; then
        mode="metrics"
    else
        mode="traces"
    fi

    basename=$(basename "$input")

    echo "Splitting $basename (mode=$mode, train_ratio=$TRAIN_RATIO)"
    go run ./cmd/split \
        --input "$input" \
        --mode "$mode" \
        --train-ratio "$TRAIN_RATIO" \
        --output-train "../data/split/train/$basename" \
        --output-test "../data/split/test/$basename"
done

echo ""
echo "=== Step 2: Generating train batches ==="

for input in ../data/split/train/*.zst; do
    if [[ "$input" == *"metrics"* ]]; then
        mode="metrics"
    else
        mode="traces"
    fi

    echo "Processing $input (mode=$mode)"
    go run ./cmd/rebatch \
        --input "$input" \
        --mode "$mode" \
        --batch-size "$TRAIN_BATCH_SIZES" \
        --format "$TRAIN_FORMATS" \
        --output "../data/generated/train"
done

echo ""
echo "=== Step 3: Generating test batches ==="

for input in ../data/split/test/*.zst; do
    if [[ "$input" == *"metrics"* ]]; then
        mode="metrics"
    else
        mode="traces"
    fi

    echo "Processing $input (mode=$mode)"
    go run ./cmd/rebatch \
        --input "$input" \
        --mode "$mode" \
        --batch-size "$BATCH_SIZES" \
        --format "$FORMATS" \
        --output "../data/generated/test"
done

echo ""
echo "=== Step 4: Splitting TPC-H data ==="

if [ -f "../data/tpch/lineitem.tbl" ]; then
    mkdir -p ../data/split/train/tpch
    mkdir -p ../data/split/test/tpch

    total_lines=$(wc -l < ../data/tpch/lineitem.tbl)
    train_lines=$((total_lines / 10))  # 10% for training

    echo "Splitting lineitem.tbl: $train_lines train lines, $((total_lines - train_lines)) test lines"
    head -n "$train_lines" ../data/tpch/lineitem.tbl > ../data/split/train/tpch/lineitem.tbl
    tail -n "+$((train_lines + 1))" ../data/tpch/lineitem.tbl > ../data/split/test/tpch/lineitem.tbl

    echo "Generating TPC-H train batches..."
    go run ./cmd/tpch-gen \
        --data-dir "../data/split/train/tpch" \
        --tables lineitem \
        --batch-size "$TRAIN_BATCH_SIZES" \
        --format "proto" \
        --output "../data/generated/train"

    echo "Generating TPC-H test batches..."
    go run ./cmd/tpch-gen \
        --data-dir "../data/split/test/tpch" \
        --tables lineitem \
        --batch-size "$BATCH_SIZES" \
        --format "proto,arrow,arrownodict,arrowdictperfile" \
        --output "../data/generated/test"
fi

echo ""
echo "=== Done ==="
echo "Train data: ../data/generated/train/"
echo "Test data:  ../data/generated/test/"
