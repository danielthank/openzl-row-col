package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"

	"github.com/klauspost/compress/zstd"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func main() {
	inputFile := flag.String("input", "", "Input .zst file")
	trainRatio := flag.Float64("train-ratio", 0.01, "Ratio of data for training (0.01 = 1%)")
	outputTrain := flag.String("output-train", "", "Output train file (.zst)")
	outputTest := flag.String("output-test", "", "Output test file (.zst)")
	mode := flag.String("mode", "", "Mode: 'metrics' or 'traces'")
	seed := flag.Int64("seed", 42, "Random seed for reproducibility")
	flag.Parse()

	// Set random seed for reproducibility
	rand.Seed(*seed)

	if *inputFile == "" || *outputTrain == "" || *outputTest == "" || *mode == "" {
		fmt.Fprintf(os.Stderr, "Usage: %s --input <file.zst> --output-train <train.zst> --output-test <test.zst> --mode <metrics|traces>\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Example: %s --input astronomy-metrics.zst --output-train train/astronomy-metrics.zst --output-test test/astronomy-metrics.zst --mode metrics\n", os.Args[0])
		os.Exit(1)
	}

	if *mode != "metrics" && *mode != "traces" {
		fmt.Fprintf(os.Stderr, "Error: mode must be 'metrics' or 'traces'\n")
		os.Exit(1)
	}

	if *trainRatio <= 0 || *trainRatio >= 1 {
		fmt.Fprintf(os.Stderr, "Error: train-ratio must be between 0 and 1 (exclusive)\n")
		os.Exit(1)
	}

	// Ensure output directories exist
	if err := os.MkdirAll(filepath.Dir(*outputTrain), 0755); err != nil {
		fmt.Fprintf(os.Stderr, "Error creating train directory: %v\n", err)
		os.Exit(1)
	}
	if err := os.MkdirAll(filepath.Dir(*outputTest), 0755); err != nil {
		fmt.Fprintf(os.Stderr, "Error creating test directory: %v\n", err)
		os.Exit(1)
	}

	if *mode == "metrics" {
		splitMetrics(*inputFile, *outputTrain, *outputTest, *trainRatio)
	} else {
		splitTraces(*inputFile, *outputTrain, *outputTest, *trainRatio)
	}
}

func splitMetrics(inputFile, outputTrain, outputTest string, trainRatio float64) {
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

	// Calculate split point
	trainDataPoints := int(float64(totalDataPoints) * trainRatio)

	fmt.Printf("Read %d payloads with %d total data points\n", len(allMetrics), totalDataPoints)
	fmt.Printf("Splitting: %d train (%.1f%%), %d test (%.1f%%)\n",
		trainDataPoints, trainRatio*100,
		totalDataPoints-trainDataPoints, (1-trainRatio)*100)

	// Split metrics by random sampling (each metric has trainRatio chance of being in train)
	trainMetrics := pmetric.NewMetrics()
	testMetrics := pmetric.NewMetrics()

	for _, metrics := range allMetrics {
		rms := metrics.ResourceMetrics()
		for i := 0; i < rms.Len(); i++ {
			rm := rms.At(i)
			sms := rm.ScopeMetrics()
			for j := 0; j < sms.Len(); j++ {
				sm := sms.At(j)
				ms := sm.Metrics()
				for k := 0; k < ms.Len(); k++ {
					m := ms.At(k)

					// Random sampling: each metric has trainRatio chance of being in train
					var targetMetrics pmetric.Metrics
					if rand.Float64() < trainRatio {
						targetMetrics = trainMetrics
					} else {
						targetMetrics = testMetrics
					}

					// Find or create matching ResourceMetrics
					targetRM := findOrCreateResourceMetrics(targetMetrics, rm.Resource())
					// Find or create matching ScopeMetrics
					targetSM := findOrCreateScopeMetrics(targetRM, sm.Scope())
					// Copy the metric
					m.CopyTo(targetSM.Metrics().AppendEmpty())
				}
			}
		}
	}

	// Write train metrics
	writeMetricsToFileSingle(trainMetrics, outputTrain)

	// Write test metrics
	writeMetricsToFileSingle(testMetrics, outputTest)

	fmt.Printf("\nTrain: %d data points -> %s\n", trainMetrics.DataPointCount(), outputTrain)
	fmt.Printf("Test:  %d data points -> %s\n", testMetrics.DataPointCount(), outputTest)
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

func findOrCreateResourceMetrics(metrics pmetric.Metrics, resource pcommon.Resource) pmetric.ResourceMetrics {
	// For simplicity, always create new (metrics will be grouped by the structure)
	rm := metrics.ResourceMetrics().AppendEmpty()
	resource.CopyTo(rm.Resource())
	return rm
}

func findOrCreateScopeMetrics(rm pmetric.ResourceMetrics, scope pcommon.InstrumentationScope) pmetric.ScopeMetrics {
	// For simplicity, always create new
	sm := rm.ScopeMetrics().AppendEmpty()
	scope.CopyTo(sm.Scope())
	return sm
}

func splitTraces(inputFile, outputTrain, outputTest string, trainRatio float64) {
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

	// Calculate split point
	trainSpans := int(float64(totalSpans) * trainRatio)

	fmt.Printf("Read %d payloads with %d total spans\n", len(allTraces), totalSpans)
	fmt.Printf("Splitting: %d train (%.1f%%), %d test (%.1f%%)\n",
		trainSpans, trainRatio*100,
		totalSpans-trainSpans, (1-trainRatio)*100)

	// Split traces by random sampling (each span has trainRatio chance of being in train)
	trainTraces := ptrace.NewTraces()
	testTraces := ptrace.NewTraces()

	for _, traces := range allTraces {
		rss := traces.ResourceSpans()
		for i := 0; i < rss.Len(); i++ {
			rs := rss.At(i)
			sss := rs.ScopeSpans()
			for j := 0; j < sss.Len(); j++ {
				ss := sss.At(j)
				spans := ss.Spans()
				for k := 0; k < spans.Len(); k++ {
					span := spans.At(k)

					// Random sampling: each span has trainRatio chance of being in train
					var targetTraces ptrace.Traces
					if rand.Float64() < trainRatio {
						targetTraces = trainTraces
					} else {
						targetTraces = testTraces
					}

					// Find or create matching ResourceSpans
					targetRS := findOrCreateResourceSpans(targetTraces, rs.Resource())
					// Find or create matching ScopeSpans
					targetSS := findOrCreateScopeSpans(targetRS, ss.Scope())
					// Copy the span
					span.CopyTo(targetSS.Spans().AppendEmpty())
				}
			}
		}
	}

	// Write train traces
	writeTracesToFileSingle(trainTraces, outputTrain)

	// Write test traces
	writeTracesToFileSingle(testTraces, outputTest)

	fmt.Printf("\nTrain: %d spans -> %s\n", trainTraces.SpanCount(), outputTrain)
	fmt.Printf("Test:  %d spans -> %s\n", testTraces.SpanCount(), outputTest)
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

func writeMetricsToFileSingle(metrics pmetric.Metrics, outputFile string) {
	f, err := os.Create(outputFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating output file: %v\n", err)
		os.Exit(1)
	}
	defer f.Close()

	zwriter, err := zstd.NewWriter(f)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating zstd writer: %v\n", err)
		os.Exit(1)
	}
	defer zwriter.Close()

	marshaler := pmetric.ProtoMarshaler{}

	if metrics.DataPointCount() == 0 {
		return
	}

	data, err := marshaler.MarshalMetrics(metrics)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error marshaling metrics: %v\n", err)
		os.Exit(1)
	}

	// Write size prefix
	var sizeBytes [4]byte
	binary.BigEndian.PutUint32(sizeBytes[:], uint32(len(data)))
	if _, err := zwriter.Write(sizeBytes[:]); err != nil {
		fmt.Fprintf(os.Stderr, "Error writing size: %v\n", err)
		os.Exit(1)
	}

	// Write payload
	if _, err := zwriter.Write(data); err != nil {
		fmt.Fprintf(os.Stderr, "Error writing payload: %v\n", err)
		os.Exit(1)
	}
}

func writeTracesToFileSingle(traces ptrace.Traces, outputFile string) {
	f, err := os.Create(outputFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating output file: %v\n", err)
		os.Exit(1)
	}
	defer f.Close()

	zwriter, err := zstd.NewWriter(f)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating zstd writer: %v\n", err)
		os.Exit(1)
	}
	defer zwriter.Close()

	marshaler := ptrace.ProtoMarshaler{}

	if traces.SpanCount() == 0 {
		return
	}

	data, err := marshaler.MarshalTraces(traces)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error marshaling traces: %v\n", err)
		os.Exit(1)
	}

	// Write size prefix
	var sizeBytes [4]byte
	binary.BigEndian.PutUint32(sizeBytes[:], uint32(len(data)))
	if _, err := zwriter.Write(sizeBytes[:]); err != nil {
		fmt.Fprintf(os.Stderr, "Error writing size: %v\n", err)
		os.Exit(1)
	}

	// Write payload
	if _, err := zwriter.Write(data); err != nil {
		fmt.Fprintf(os.Stderr, "Error writing payload: %v\n", err)
		os.Exit(1)
	}
}
