#!/bin/bash
cd "$(dirname "$0")/../go"

BATCH_SIZES="10,50,100,500,1000,5000,10000,50000"
FORMATS="otlp,otap,otapnodict,otapdictperfile"

for input in ../testdata/*.zst; do
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
        --format "$FORMATS"
done
