package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/danielthank/15712/go/pkg/arrowutil"
	"github.com/danielthank/15712/go/pkg/otlpmetricsdict"
	"github.com/danielthank/15712/go/pkg/otlptracesdict"
	"github.com/klauspost/compress/zstd"
	arrowpb "github.com/open-telemetry/otel-arrow/go/api/experimental/arrow/v1"
	"github.com/open-telemetry/otel-arrow/go/pkg/config"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/arrow_record"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"google.golang.org/protobuf/proto"
)

type OTAPMode int

const (
	OTAPModeNative      OTAPMode = iota // Reuse producer (incremental dict)
	OTAPModeNoDict                      // config.WithNoDictionary()
	OTAPModeDictPerFile                 // New producer per batch
	OTAPModeNoSort                      // No sorting optimization
	OTAPModeNoDedup                     // No resource/scope deduplication
)

// Metadata contains information about the generated dataset
type Metadata struct {
	TotalDataPoints int `json:"total_data_points"`
	NumPayloads     int `json:"num_payloads"`
}

func writeMetadata(outputDir string, totalDataPoints, numPayloads int) error {
	metadata := Metadata{
		TotalDataPoints: totalDataPoints,
		NumPayloads:     numPayloads,
	}
	data, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(outputDir, "metadata.json"), data, 0644)
}

func main() {
	inputFile := flag.String("input", "", "Input .zst file")
	mode := flag.String("mode", "", "Mode: 'metrics', 'traces', or 'dump'")
	batchSizeStr := flag.String("batch-size", "", "Comma-separated list of batch sizes")
	formatStr := flag.String("format", "", "Comma-separated list of formats: otlp, otlpmetricsdict, otlptracesdict, otap, otapnodict, otapdictperfile, otapnosort")
	outputDir := flag.String("output", "", "Output directory (default: ../data/generated)")
	dumpFile := flag.String("dump-file", "", "File to dump (for dump mode)")
	flag.Parse()

	// Set default output directory
	if *outputDir == "" {
		*outputDir = filepath.Join("..", "data", "generated")
	}

	// Handle dump mode
	if *mode == "dump" {
		if *dumpFile == "" {
			fmt.Fprintf(os.Stderr, "Usage: %s --mode dump --dump-file <file.bin>\n", os.Args[0])
			os.Exit(1)
		}
		dumpOTAPFile(*dumpFile)
		return
	}

	// Handle analyze mode
	if *mode == "analyze" {
		if *inputFile == "" {
			fmt.Fprintf(os.Stderr, "Usage: %s --mode analyze --input <file.zst>\n", os.Args[0])
			os.Exit(1)
		}
		analyzeTraces(*inputFile)
		return
	}

	if *inputFile == "" || *mode == "" || *batchSizeStr == "" || *formatStr == "" {
		fmt.Fprintf(os.Stderr, "Usage: %s --input <file.zst> --mode <metrics|traces> --batch-size <sizes> --format <formats>\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "       %s --mode dump --dump-file <file.bin>\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Formats: otlp, otlpmetricsdict, otlptracesdict, otap, otapnodict, otapdictperfile\n")
		fmt.Fprintf(os.Stderr, "Example: %s --input testdata/astronomy-oteltraces.zst --mode traces --batch-size 50000 --format otlp,otlptracesdict,otap\n", os.Args[0])
		os.Exit(1)
	}

	if *mode != "metrics" && *mode != "traces" {
		fmt.Fprintf(os.Stderr, "Error: mode must be 'metrics' or 'traces'\n")
		os.Exit(1)
	}

	// Parse batch sizes
	batchSizes := []int{}
	for _, sizeStr := range strings.Split(*batchSizeStr, ",") {
		var size int
		_, err := fmt.Sscanf(strings.TrimSpace(sizeStr), "%d", &size)
		if err != nil || size <= 0 {
			fmt.Fprintf(os.Stderr, "Error: invalid batch size '%s'\n", sizeStr)
			os.Exit(1)
		}
		batchSizes = append(batchSizes, size)
	}

	// Parse formats
	validFormats := map[string]bool{
		"otlp":            true,
		"otlpmetricsdict": true,
		"otlptracesdict":  true,
		"otap":            true,
		"otapnodict":      true,
		"otapdictperfile": true,
		"otapnosort":      true,
		"otapnodedup":     true,
	}
	formats := []string{}
	for _, format := range strings.Split(*formatStr, ",") {
		format = strings.TrimSpace(format)
		if !validFormats[format] {
			fmt.Fprintf(os.Stderr, "Error: invalid format '%s', must be one of: otlp, otlpmetricsdict, otlptracesdict, otap, otapnodict, otapdictperfile, otapnosort, otapnodedup\n", format)
			os.Exit(1)
		}
		formats = append(formats, format)
	}

	// Read input file
	if *mode == "metrics" {
		processMetrics(*inputFile, batchSizes, formats, *outputDir)
	} else {
		processTraces(*inputFile, batchSizes, formats, *outputDir)
	}
}

func processMetrics(inputFile string, batchSizes []int, formats []string, baseOutputDir string) {
	// Read all metrics from input file
	allMetrics := []pmetric.Metrics{}

	f, err := os.Open(inputFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening file: %v\n", err)
		os.Exit(1)
	}
	defer f.Close()

	zreader, err := zstd.NewReader(f)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating zstd reader: %v\n", err)
		os.Exit(1)
	}
	defer zreader.Close()

	unmarshaler := pmetric.ProtoUnmarshaler{}

	for {
		var sizeBytes [4]byte
		n, err := zreader.Read(sizeBytes[:])
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Fprintf(os.Stderr, "Error reading size: %v\n", err)
			os.Exit(1)
		}
		if n != 4 {
			fmt.Fprintf(os.Stderr, "Invalid input: expected 4 bytes\n")
			os.Exit(1)
		}

		bytesSize := binary.BigEndian.Uint32(sizeBytes[:])
		payload := make([]byte, bytesSize)

		n, err = io.ReadFull(zreader, payload)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error reading payload: %v\n", err)
			os.Exit(1)
		}

		metrics, err := unmarshaler.UnmarshalMetrics(payload)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error unmarshaling metrics: %v\n", err)
			os.Exit(1)
		}

		allMetrics = append(allMetrics, metrics)
	}

	// Count total data points
	totalDataPoints := 0
	for _, m := range allMetrics {
		totalDataPoints += m.DataPointCount()
	}

	fmt.Printf("Read %d payloads with %d total data points\n", len(allMetrics), totalDataPoints)

	// Get base name for output directories
	inputBase := filepath.Base(inputFile)
	baseName := inputBase[:len(inputBase)-len(filepath.Ext(inputBase))]

	// Process each batch size and format combination
	for _, batchSize := range batchSizes {
		for _, format := range formats {
			fmt.Printf("\nProcessing batch-size=%d format=%s\n", batchSize, format)

			outputDir := filepath.Join(baseOutputDir, fmt.Sprintf("%s-%s-%d", baseName, format, batchSize))
			if err := os.MkdirAll(outputDir, 0755); err != nil {
				fmt.Fprintf(os.Stderr, "Error creating directory: %v\n", err)
				continue
			}

			// Rebatch metrics
			batches := rebatchMetrics(allMetrics, batchSize)

			// Write batches
			switch format {
			case "otlp":
				writeBatchesOTLPMetrics(batches, outputDir)
			case "otlpmetricsdict":
				writeBatchesOTLPDictMetrics(batches, outputDir)
			case "otap":
				writeBatchesOTAPMetrics(batches, outputDir, OTAPModeNative)
			case "otapnodict":
				writeBatchesOTAPMetrics(batches, outputDir, OTAPModeNoDict)
			case "otapdictperfile":
				writeBatchesOTAPMetrics(batches, outputDir, OTAPModeDictPerFile)
			case "otapnosort":
				writeBatchesOTAPMetrics(batches, outputDir, OTAPModeNoSort)
			case "otapnodedup":
				writeBatchesOTAPMetricsNoDedup(batches, outputDir)
			}

			// Write metadata
			if err := writeMetadata(outputDir, totalDataPoints, len(batches)); err != nil {
				fmt.Fprintf(os.Stderr, "Error writing metadata: %v\n", err)
			}
		}
	}
}

func processTraces(inputFile string, batchSizes []int, formats []string, baseOutputDir string) {
	// Read all traces from input file
	allTraces := []ptrace.Traces{}

	f, err := os.Open(inputFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening file: %v\n", err)
		os.Exit(1)
	}
	defer f.Close()

	zreader, err := zstd.NewReader(f)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating zstd reader: %v\n", err)
		os.Exit(1)
	}
	defer zreader.Close()

	unmarshaler := ptrace.ProtoUnmarshaler{}

	for {
		var sizeBytes [4]byte
		n, err := zreader.Read(sizeBytes[:])
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Fprintf(os.Stderr, "Error reading size: %v\n", err)
			os.Exit(1)
		}
		if n != 4 {
			fmt.Fprintf(os.Stderr, "Invalid input: expected 4 bytes\n")
			os.Exit(1)
		}

		bytesSize := binary.BigEndian.Uint32(sizeBytes[:])
		payload := make([]byte, bytesSize)

		n, err = io.ReadFull(zreader, payload)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error reading payload: %v\n", err)
			os.Exit(1)
		}

		traces, err := unmarshaler.UnmarshalTraces(payload)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error unmarshaling traces: %v\n", err)
			os.Exit(1)
		}

		allTraces = append(allTraces, traces)
	}

	// Count total spans
	totalSpans := 0
	for _, t := range allTraces {
		totalSpans += t.SpanCount()
	}

	fmt.Printf("Read %d payloads with %d total spans\n", len(allTraces), totalSpans)

	// Get base name for output directories
	inputBase := filepath.Base(inputFile)
	baseName := inputBase[:len(inputBase)-len(filepath.Ext(inputBase))]

	// Process each batch size and format combination
	for _, batchSize := range batchSizes {
		for _, format := range formats {
			fmt.Printf("\nProcessing batch-size=%d format=%s\n", batchSize, format)

			outputDir := filepath.Join(baseOutputDir, fmt.Sprintf("%s-%s-%d", baseName, format, batchSize))
			if err := os.MkdirAll(outputDir, 0755); err != nil {
				fmt.Fprintf(os.Stderr, "Error creating directory: %v\n", err)
				continue
			}

			// Rebatch traces
			batches := rebatchTraces(allTraces, batchSize)

			// Write batches
			switch format {
			case "otlp":
				writeBatchesOTLPTraces(batches, outputDir)
			case "otlptracesdict":
				writeBatchesOTLPDictTraces(batches, outputDir)
			case "otap":
				writeBatchesOTAPTraces(batches, outputDir, OTAPModeNative)
			case "otapnodict":
				writeBatchesOTAPTraces(batches, outputDir, OTAPModeNoDict)
			case "otapdictperfile":
				writeBatchesOTAPTraces(batches, outputDir, OTAPModeDictPerFile)
			case "otapnosort":
				writeBatchesOTAPTraces(batches, outputDir, OTAPModeNoSort)
			case "otapnodedup":
				writeBatchesOTAPTracesNoDedup(batches, outputDir)
			}

			// Write metadata
			if err := writeMetadata(outputDir, totalSpans, len(batches)); err != nil {
				fmt.Fprintf(os.Stderr, "Error writing metadata: %v\n", err)
			}
		}
	}
}

func rebatchMetrics(allMetrics []pmetric.Metrics, batchSize int) []pmetric.Metrics {
	batches := []pmetric.Metrics{}
	currentBatch := pmetric.NewMetrics()
	currentCount := 0

	for _, metrics := range allMetrics {
		for i := 0; i < metrics.ResourceMetrics().Len(); i++ {
			rm := metrics.ResourceMetrics().At(i)

			for j := 0; j < rm.ScopeMetrics().Len(); j++ {
				sm := rm.ScopeMetrics().At(j)

				for k := 0; k < sm.Metrics().Len(); k++ {
					m := sm.Metrics().At(k)
					dataPointCount := getMetricDataPointCount(m)

					// If adding this metric would exceed batch size, start new batch
					if currentCount > 0 && currentCount+dataPointCount > batchSize {
						batches = append(batches, currentBatch)
						currentBatch = pmetric.NewMetrics()
						currentCount = 0
					}

					// Add metric to current batch
					destRM := findOrCreateResourceMetrics(currentBatch, rm.Resource())
					destSM := findOrCreateScopeMetrics(destRM, sm.Scope())
					m.CopyTo(destSM.Metrics().AppendEmpty())
					currentCount += dataPointCount
				}
			}
		}
	}

	// Add final batch if not empty
	if currentCount > 0 {
		batches = append(batches, currentBatch)
	}

	return batches
}

func rebatchTraces(allTraces []ptrace.Traces, batchSize int) []ptrace.Traces {
	batches := []ptrace.Traces{}
	currentBatch := ptrace.NewTraces()
	currentCount := 0

	for _, traces := range allTraces {
		for i := 0; i < traces.ResourceSpans().Len(); i++ {
			rs := traces.ResourceSpans().At(i)

			for j := 0; j < rs.ScopeSpans().Len(); j++ {
				ss := rs.ScopeSpans().At(j)

				for k := 0; k < ss.Spans().Len(); k++ {
					span := ss.Spans().At(k)

					// If adding this span would exceed batch size, start new batch
					if currentCount > 0 && currentCount+1 > batchSize {
						batches = append(batches, currentBatch)
						currentBatch = ptrace.NewTraces()
						currentCount = 0
					}

					// Add span to current batch
					destRS := findOrCreateResourceSpans(currentBatch, rs.Resource())
					destSS := findOrCreateScopeSpans(destRS, ss.Scope())
					span.CopyTo(destSS.Spans().AppendEmpty())
					currentCount++
				}
			}
		}
	}

	// Add final batch if not empty
	if currentCount > 0 {
		batches = append(batches, currentBatch)
	}

	return batches
}

func findOrCreateResourceMetrics(metrics pmetric.Metrics, resource pcommon.Resource) pmetric.ResourceMetrics {
	// For simplicity, create a new ResourceMetrics for each unique resource
	// In practice, could optimize by reusing if attributes match
	rm := metrics.ResourceMetrics().AppendEmpty()
	resource.CopyTo(rm.Resource())
	return rm
}

func findOrCreateScopeMetrics(rm pmetric.ResourceMetrics, scope pcommon.InstrumentationScope) pmetric.ScopeMetrics {
	sm := rm.ScopeMetrics().AppendEmpty()
	scope.CopyTo(sm.Scope())
	return sm
}

func findOrCreateResourceSpans(traces ptrace.Traces, resource pcommon.Resource) ptrace.ResourceSpans {
	rs := traces.ResourceSpans().AppendEmpty()
	resource.CopyTo(rs.Resource())
	return rs
}

func findOrCreateScopeSpans(rs ptrace.ResourceSpans, scope pcommon.InstrumentationScope) ptrace.ScopeSpans {
	ss := rs.ScopeSpans().AppendEmpty()
	scope.CopyTo(ss.Scope())
	return ss
}

func getMetricDataPointCount(m pmetric.Metric) int {
	switch m.Type() {
	case pmetric.MetricTypeGauge:
		return m.Gauge().DataPoints().Len()
	case pmetric.MetricTypeSum:
		return m.Sum().DataPoints().Len()
	case pmetric.MetricTypeHistogram:
		return m.Histogram().DataPoints().Len()
	case pmetric.MetricTypeExponentialHistogram:
		return m.ExponentialHistogram().DataPoints().Len()
	case pmetric.MetricTypeSummary:
		return m.Summary().DataPoints().Len()
	default:
		return 0
	}
}

func writeBatchesOTLPMetrics(batches []pmetric.Metrics, outputDir string) {
	marshaler := pmetric.ProtoMarshaler{}

	for i, batch := range batches {
		data, err := marshaler.MarshalMetrics(batch)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error marshaling batch %d: %v\n", i, err)
			continue
		}

		outputFile := filepath.Join(outputDir, fmt.Sprintf("payload_%04d.bin", i))
		if err := os.WriteFile(outputFile, data, 0644); err != nil {
			fmt.Fprintf(os.Stderr, "Error writing file: %v\n", err)
			continue
		}
	}

	fmt.Printf("  Wrote %d batches to %s\n", len(batches), outputDir)
}

func writeBatchesOTLPDictMetrics(batches []pmetric.Metrics, outputDir string) {
	for i, batch := range batches {
		// Convert to dictionary-encoded format
		dictBatch := otlpmetricsdict.Convert(batch)

		data, err := proto.Marshal(dictBatch)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error marshaling batch %d: %v\n", i, err)
			continue
		}

		outputFile := filepath.Join(outputDir, fmt.Sprintf("payload_%04d.bin", i))
		if err := os.WriteFile(outputFile, data, 0644); err != nil {
			fmt.Fprintf(os.Stderr, "Error writing file: %v\n", err)
			continue
		}
	}

	fmt.Printf("  Wrote %d batches to %s\n", len(batches), outputDir)
}

func writeBatchesOTLPDictTraces(batches []ptrace.Traces, outputDir string) {
	for i, batch := range batches {
		// Convert to dictionary-encoded format
		dictBatch := otlptracesdict.Convert(batch)

		data, err := proto.Marshal(dictBatch)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error marshaling batch %d: %v\n", i, err)
			continue
		}

		outputFile := filepath.Join(outputDir, fmt.Sprintf("payload_%04d.bin", i))
		if err := os.WriteFile(outputFile, data, 0644); err != nil {
			fmt.Fprintf(os.Stderr, "Error writing file: %v\n", err)
			continue
		}
	}

	fmt.Printf("  Wrote %d batches to %s\n", len(batches), outputDir)
}

func writeBatchesOTAPMetrics(batches []pmetric.Metrics, outputDir string, mode OTAPMode) {
	// For nosort mode, use the special NoSortMetricsProducer
	if mode == OTAPModeNoSort {
		writeBatchesOTAPMetricsNoSort(batches, outputDir)
		return
	}

	var producer *arrow_record.Producer

	// For native mode, create one producer and reuse (incremental dictionaries)
	if mode == OTAPModeNative {
		producer = arrow_record.NewProducerWithOptions(config.WithNoZstd())
		defer producer.Close()
	}

	for i, batch := range batches {
		// For dictperfile, create fresh producer each iteration (dictionary but no deltas)
		if mode == OTAPModeDictPerFile {
			producer = arrow_record.NewProducerWithOptions(config.WithNoZstd())
		}
		// For nodict, create fresh producer with no dictionary
		if mode == OTAPModeNoDict {
			producer = arrow_record.NewProducerWithOptions(config.WithNoZstd(), config.WithNoDictionary())
		}

		arrowRecords, err := producer.BatchArrowRecordsFromMetrics(batch)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error converting batch %d to Arrow: %v\n", i, err)
			fmt.Fprintf(os.Stderr, "  (This may be due to high cardinality - try using OTLP format instead)\n")
			if mode != OTAPModeNative {
				producer.Close()
			}
			return
		}

		data, err := proto.Marshal(arrowRecords)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error marshaling Arrow batch %d: %v\n", i, err)
			if mode != OTAPModeNative {
				producer.Close()
			}
			continue
		}

		// Close per-batch producer
		if mode != OTAPModeNative {
			producer.Close()
		}

		outputFile := filepath.Join(outputDir, fmt.Sprintf("payload_%04d.bin", i))
		if err := os.WriteFile(outputFile, data, 0644); err != nil {
			fmt.Fprintf(os.Stderr, "Error writing file: %v\n", err)
			continue
		}
	}

	fmt.Printf("  Wrote %d batches to %s\n", len(batches), outputDir)
}

func writeBatchesOTAPMetricsNoSort(batches []pmetric.Metrics, outputDir string) {
	// Create fresh producer for each batch (no incremental dictionaries for nosort)
	for i, batch := range batches {
		producer := arrowutil.NewNoSortMetricsProducer(config.WithNoZstd())

		arrowRecords, err := producer.BatchArrowRecordsFromMetrics(batch)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error converting batch %d to Arrow: %v\n", i, err)
			fmt.Fprintf(os.Stderr, "  (This may be due to high cardinality - try using OTLP format instead)\n")
			producer.Close()
			return
		}

		data, err := proto.Marshal(arrowRecords)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error marshaling Arrow batch %d: %v\n", i, err)
			producer.Close()
			continue
		}

		producer.Close()

		outputFile := filepath.Join(outputDir, fmt.Sprintf("payload_%04d.bin", i))
		if err := os.WriteFile(outputFile, data, 0644); err != nil {
			fmt.Fprintf(os.Stderr, "Error writing file: %v\n", err)
			continue
		}
	}

	fmt.Printf("  Wrote %d batches to %s\n", len(batches), outputDir)
}

func writeBatchesOTAPMetricsNoDedup(batches []pmetric.Metrics, outputDir string) {
	// Create fresh producer for each batch (no resource/scope deduplication)
	for i, batch := range batches {
		producer := arrowutil.NewNoDedupMetricsProducer(config.WithNoZstd())

		arrowRecords, err := producer.BatchArrowRecordsFromMetrics(batch)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error converting batch %d to Arrow: %v\n", i, err)
			fmt.Fprintf(os.Stderr, "  (This may be due to high cardinality - try using OTLP format instead)\n")
			producer.Close()
			return
		}

		data, err := proto.Marshal(arrowRecords)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error marshaling Arrow batch %d: %v\n", i, err)
			producer.Close()
			continue
		}

		producer.Close()

		outputFile := filepath.Join(outputDir, fmt.Sprintf("payload_%04d.bin", i))
		if err := os.WriteFile(outputFile, data, 0644); err != nil {
			fmt.Fprintf(os.Stderr, "Error writing file: %v\n", err)
			continue
		}
	}

	fmt.Printf("  Wrote %d batches to %s\n", len(batches), outputDir)
}

func writeBatchesOTLPTraces(batches []ptrace.Traces, outputDir string) {
	marshaler := ptrace.ProtoMarshaler{}

	for i, batch := range batches {
		data, err := marshaler.MarshalTraces(batch)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error marshaling batch %d: %v\n", i, err)
			continue
		}

		outputFile := filepath.Join(outputDir, fmt.Sprintf("payload_%04d.bin", i))
		if err := os.WriteFile(outputFile, data, 0644); err != nil {
			fmt.Fprintf(os.Stderr, "Error writing file: %v\n", err)
			continue
		}
	}

	fmt.Printf("  Wrote %d batches to %s\n", len(batches), outputDir)
}

func writeBatchesOTAPTraces(batches []ptrace.Traces, outputDir string, mode OTAPMode) {
	var producer *arrow_record.Producer

	// For native mode, create one producer and reuse (incremental dictionaries)
	if mode == OTAPModeNative {
		producer = arrow_record.NewProducerWithOptions(config.WithNoZstd())
		defer producer.Close()
	}

	for i, batch := range batches {
		// For dictperfile, create fresh producer each iteration (dictionary but no deltas)
		if mode == OTAPModeDictPerFile {
			producer = arrow_record.NewProducerWithOptions(config.WithNoZstd())
		}
		// For nodict, create fresh producer with no dictionary
		if mode == OTAPModeNoDict {
			producer = arrow_record.NewProducerWithOptions(config.WithNoZstd(), config.WithNoDictionary())
		}
		// For nosort, create fresh producer with no sorting
		if mode == OTAPModeNoSort {
			producer = arrow_record.NewProducerWithOptions(
				config.WithNoZstd(),
				config.WithOrderSpanBy(config.OrderSpanByNothing),
				config.WithOrderAttrs16By(config.OrderAttrs16ByNothing),
				config.WithOrderAttrs32By(config.OrderAttrs32ByNothing),
			)
		}

		arrowRecords, err := producer.BatchArrowRecordsFromTraces(batch)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error converting batch %d to Arrow: %v\n", i, err)
			fmt.Fprintf(os.Stderr, "  (This may be due to high cardinality - try using OTLP format instead)\n")
			if mode != OTAPModeNative {
				producer.Close()
			}
			return
		}

		data, err := proto.Marshal(arrowRecords)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error marshaling Arrow batch %d: %v\n", i, err)
			if mode != OTAPModeNative {
				producer.Close()
			}
			continue
		}

		// Close per-batch producer
		if mode != OTAPModeNative {
			producer.Close()
		}

		outputFile := filepath.Join(outputDir, fmt.Sprintf("payload_%04d.bin", i))
		if err := os.WriteFile(outputFile, data, 0644); err != nil {
			fmt.Fprintf(os.Stderr, "Error writing file: %v\n", err)
			continue
		}
	}

	fmt.Printf("  Wrote %d batches to %s\n", len(batches), outputDir)
}

func writeBatchesOTAPTracesNoDedup(batches []ptrace.Traces, outputDir string) {
	// Create fresh producer for each batch (no resource/scope deduplication)
	for i, batch := range batches {
		producer := arrowutil.NewNoDedupTracesProducer(config.WithNoZstd())

		arrowRecords, err := producer.BatchArrowRecordsFromTraces(batch)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error converting batch %d to Arrow: %v\n", i, err)
			fmt.Fprintf(os.Stderr, "  (This may be due to high cardinality - try using OTLP format instead)\n")
			producer.Close()
			return
		}

		data, err := proto.Marshal(arrowRecords)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error marshaling Arrow batch %d: %v\n", i, err)
			producer.Close()
			continue
		}

		producer.Close()

		outputFile := filepath.Join(outputDir, fmt.Sprintf("payload_%04d.bin", i))
		if err := os.WriteFile(outputFile, data, 0644); err != nil {
			fmt.Fprintf(os.Stderr, "Error writing file: %v\n", err)
			continue
		}
	}

	fmt.Printf("  Wrote %d batches to %s\n", len(batches), outputDir)
}

func dumpOTAPFile(filePath string) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading file: %v\n", err)
		os.Exit(1)
	}

	var batch arrowpb.BatchArrowRecords
	if err := proto.Unmarshal(data, &batch); err != nil {
		fmt.Fprintf(os.Stderr, "Error unmarshaling BatchArrowRecords: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("BatchArrowRecords:\n")
	fmt.Printf("  batch_id: %d\n", batch.BatchId)
	fmt.Printf("  headers: %d bytes\n", len(batch.Headers))
	fmt.Printf("  arrow_payloads: %d\n", len(batch.ArrowPayloads))

	for i, payload := range batch.ArrowPayloads {
		fmt.Printf("\n  [%d] ArrowPayload:\n", i)
		fmt.Printf("       schema_id: %s\n", payload.SchemaId)
		fmt.Printf("       type: %s (%d)\n", payload.Type.String(), payload.Type)
		fmt.Printf("       record: %d bytes\n", len(payload.Record))

		// Decode Arrow IPC stream with dictionary deltas support
		reader, err := ipc.NewReader(
			bytes.NewReader(payload.Record),
			ipc.WithDictionaryDeltas(true),
		)
		if err != nil {
			fmt.Printf("       [error reading Arrow IPC: %v]\n", err)
			continue
		}

		schema := reader.Schema()
		fmt.Printf("       schema: %d fields\n", schema.NumFields())
		for j := 0; j < schema.NumFields(); j++ {
			field := schema.Field(j)
			fmt.Printf("         - %s: %s\n", field.Name, field.Type)
		}

		// Read records
		recordNum := 0
		for reader.Next() {
			rec := reader.Record()
			fmt.Printf("       record[%d]: %d rows, %d cols\n", recordNum, rec.NumRows(), rec.NumCols())

			// Print first few values of each column
			for c := 0; c < int(rec.NumCols()); c++ {
				col := rec.Column(c)
				colName := schema.Field(c).Name
				fmt.Printf("         %s: %s\n", colName, col)
			}
			recordNum++
		}

		if err := reader.Err(); err != nil {
			fmt.Printf("       [error iterating records: %v]\n", err)
		}
		reader.Release()
	}
}

// analyzeTraces reads a trace file and outputs diversity statistics
func analyzeTraces(inputFile string) {
	f, err := os.Open(inputFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening file: %v\n", err)
		os.Exit(1)
	}
	defer f.Close()

	zreader, err := zstd.NewReader(f)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating zstd reader: %v\n", err)
		os.Exit(1)
	}
	defer zreader.Close()

	unmarshaler := ptrace.ProtoUnmarshaler{}

	// Track unique values
	uniqueResources := make(map[string]int)  // resource hash -> count of spans
	uniqueScopes := make(map[string]int)     // scope name -> count of spans
	uniqueSpanNames := make(map[string]int)  // span name -> count
	totalSpans := 0
	totalPayloads := 0

	for {
		var sizeBytes [4]byte
		n, err := zreader.Read(sizeBytes[:])
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Fprintf(os.Stderr, "Error reading size: %v\n", err)
			os.Exit(1)
		}
		if n != 4 {
			fmt.Fprintf(os.Stderr, "Invalid input: expected 4 bytes\n")
			os.Exit(1)
		}

		bytesSize := binary.BigEndian.Uint32(sizeBytes[:])
		payload := make([]byte, bytesSize)

		n, err = io.ReadFull(zreader, payload)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error reading payload: %v\n", err)
			os.Exit(1)
		}

		traces, err := unmarshaler.UnmarshalTraces(payload)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error unmarshaling traces: %v\n", err)
			os.Exit(1)
		}

		totalPayloads++

		// Analyze traces
		for i := 0; i < traces.ResourceSpans().Len(); i++ {
			rs := traces.ResourceSpans().At(i)
			resourceHash := hashResource(rs.Resource())

			for j := 0; j < rs.ScopeSpans().Len(); j++ {
				ss := rs.ScopeSpans().At(j)
				scopeName := ss.Scope().Name()

				for k := 0; k < ss.Spans().Len(); k++ {
					span := ss.Spans().At(k)
					spanName := span.Name()

					uniqueResources[resourceHash]++
					uniqueScopes[scopeName]++
					uniqueSpanNames[spanName]++
					totalSpans++
				}
			}
		}
	}

	// Calculate statistics
	fmt.Printf("\n=== Diversity Analysis: %s ===\n\n", filepath.Base(inputFile))
	fmt.Printf("Total payloads:      %d\n", totalPayloads)
	fmt.Printf("Total spans:         %d\n", totalSpans)
	fmt.Printf("\n")
	fmt.Printf("Unique resources:    %d\n", len(uniqueResources))
	fmt.Printf("Unique scopes:       %d\n", len(uniqueScopes))
	fmt.Printf("Unique span names:   %d\n", len(uniqueSpanNames))
	fmt.Printf("\n")
	fmt.Printf("Spans per resource:  %.2f\n", float64(totalSpans)/float64(len(uniqueResources)))
	fmt.Printf("Spans per scope:     %.2f\n", float64(totalSpans)/float64(len(uniqueScopes)))
	fmt.Printf("Spans per name:      %.2f\n", float64(totalSpans)/float64(len(uniqueSpanNames)))

	// Show top resources by span count
	fmt.Printf("\n--- Top 5 Resources by Span Count ---\n")
	type kv struct {
		key   string
		value int
	}
	var sortedResources []kv
	for k, v := range uniqueResources {
		sortedResources = append(sortedResources, kv{k, v})
	}
	// Sort by value descending
	for i := 0; i < len(sortedResources); i++ {
		for j := i + 1; j < len(sortedResources); j++ {
			if sortedResources[j].value > sortedResources[i].value {
				sortedResources[i], sortedResources[j] = sortedResources[j], sortedResources[i]
			}
		}
	}
	for i := 0; i < 5 && i < len(sortedResources); i++ {
		pct := float64(sortedResources[i].value) / float64(totalSpans) * 100
		fmt.Printf("  %d. %s: %d spans (%.1f%%)\n", i+1, sortedResources[i].key, sortedResources[i].value, pct)
	}

	// Show top scopes by span count
	fmt.Printf("\n--- Top 5 Scopes by Span Count ---\n")
	var sortedScopes []kv
	for k, v := range uniqueScopes {
		sortedScopes = append(sortedScopes, kv{k, v})
	}
	for i := 0; i < len(sortedScopes); i++ {
		for j := i + 1; j < len(sortedScopes); j++ {
			if sortedScopes[j].value > sortedScopes[i].value {
				sortedScopes[i], sortedScopes[j] = sortedScopes[j], sortedScopes[i]
			}
		}
	}
	for i := 0; i < 5 && i < len(sortedScopes); i++ {
		pct := float64(sortedScopes[i].value) / float64(totalSpans) * 100
		fmt.Printf("  %d. %s: %d spans (%.1f%%)\n", i+1, sortedScopes[i].key, sortedScopes[i].value, pct)
	}
}

// hashResource creates a simple hash of resource attributes for deduplication counting
func hashResource(resource pcommon.Resource) string {
	attrs := resource.Attributes()
	var parts []string
	attrs.Range(func(k string, v pcommon.Value) bool {
		parts = append(parts, fmt.Sprintf("%s=%s", k, v.AsString()))
		return true
	})
	// Sort for consistent hashing
	for i := 0; i < len(parts); i++ {
		for j := i + 1; j < len(parts); j++ {
			if parts[j] < parts[i] {
				parts[i], parts[j] = parts[j], parts[i]
			}
		}
	}
	return strings.Join(parts, ",")
}
