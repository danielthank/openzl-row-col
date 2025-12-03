package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/klauspost/compress/zstd"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// FieldStats tracks cardinality for a single field
type FieldStats struct {
	Name         string
	UniqueValues map[string]struct{}
	MaxPerBatch  int
}

// BatchStats tracks all field cardinalities for one batch
type BatchStats struct {
	ResourceAttrKeys   map[string]struct{}
	ResourceAttrValues map[string]map[string]struct{} // key -> values
	ScopeNames         map[string]struct{}
	ScopeVersions      map[string]struct{}
	ScopeAttrKeys      map[string]struct{}
	ScopeAttrValues    map[string]map[string]struct{}
	MetricNames        map[string]struct{}
	MetricDescriptions map[string]struct{}
	MetricUnits        map[string]struct{}
	DPAttrKeys         map[string]struct{}
	DPAttrValues       map[string]map[string]struct{} // key -> values
	SchemaURLs         map[string]struct{}
}

func newBatchStats() *BatchStats {
	return &BatchStats{
		ResourceAttrKeys:   make(map[string]struct{}),
		ResourceAttrValues: make(map[string]map[string]struct{}),
		ScopeNames:         make(map[string]struct{}),
		ScopeVersions:      make(map[string]struct{}),
		ScopeAttrKeys:      make(map[string]struct{}),
		ScopeAttrValues:    make(map[string]map[string]struct{}),
		MetricNames:        make(map[string]struct{}),
		MetricDescriptions: make(map[string]struct{}),
		MetricUnits:        make(map[string]struct{}),
		DPAttrKeys:         make(map[string]struct{}),
		DPAttrValues:       make(map[string]map[string]struct{}),
		SchemaURLs:         make(map[string]struct{}),
	}
}

// GlobalStats tracks max cardinality across all batches
type GlobalStats struct {
	ResourceAttrKeys   *FieldStats
	ResourceAttrValues map[string]*FieldStats // key -> field stats
	ScopeNames         *FieldStats
	ScopeVersions      *FieldStats
	ScopeAttrKeys      *FieldStats
	ScopeAttrValues    map[string]*FieldStats
	MetricNames        *FieldStats
	MetricDescriptions *FieldStats
	MetricUnits        *FieldStats
	DPAttrKeys         *FieldStats
	DPAttrValues       map[string]*FieldStats
	SchemaURLs         *FieldStats
}

func newGlobalStats() *GlobalStats {
	return &GlobalStats{
		ResourceAttrKeys:   &FieldStats{Name: "resource.attribute.keys", UniqueValues: make(map[string]struct{})},
		ResourceAttrValues: make(map[string]*FieldStats),
		ScopeNames:         &FieldStats{Name: "scope.name", UniqueValues: make(map[string]struct{})},
		ScopeVersions:      &FieldStats{Name: "scope.version", UniqueValues: make(map[string]struct{})},
		ScopeAttrKeys:      &FieldStats{Name: "scope.attribute.keys", UniqueValues: make(map[string]struct{})},
		ScopeAttrValues:    make(map[string]*FieldStats),
		MetricNames:        &FieldStats{Name: "metric.name", UniqueValues: make(map[string]struct{})},
		MetricDescriptions: &FieldStats{Name: "metric.description", UniqueValues: make(map[string]struct{})},
		MetricUnits:        &FieldStats{Name: "metric.unit", UniqueValues: make(map[string]struct{})},
		DPAttrKeys:         &FieldStats{Name: "datapoint.attribute.keys", UniqueValues: make(map[string]struct{})},
		DPAttrValues:       make(map[string]*FieldStats),
		SchemaURLs:         &FieldStats{Name: "schema_url", UniqueValues: make(map[string]struct{})},
	}
}

func main() {
	inputFile := flag.String("input", "", "Input .zst file containing OTLP metrics")
	batchSize := flag.Int("batch-size", 10000, "Batch size (data points per batch)")
	flag.Parse()

	if *inputFile == "" {
		fmt.Fprintf(os.Stderr, "Usage: %s --input <file.zst> [--batch-size N]\n", os.Args[0])
		os.Exit(1)
	}

	// Read all metrics from input file
	allMetrics, err := readMetrics(*inputFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading metrics: %v\n", err)
		os.Exit(1)
	}

	totalDataPoints := 0
	for _, m := range allMetrics {
		totalDataPoints += m.DataPointCount()
	}
	fmt.Printf("Read %d payloads with %d total data points\n", len(allMetrics), totalDataPoints)

	// Rebatch and analyze
	batches := rebatchMetrics(allMetrics, *batchSize)
	fmt.Printf("Created %d batches of size %d\n\n", len(batches), *batchSize)

	globalStats := newGlobalStats()

	for _, batch := range batches {
		batchStats := analyzeBatch(batch)
		updateGlobalStats(globalStats, batchStats)
	}

	printResults(globalStats)
}

func readMetrics(inputFile string) ([]pmetric.Metrics, error) {
	f, err := os.Open(inputFile)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	zreader, err := zstd.NewReader(f)
	if err != nil {
		return nil, err
	}
	defer zreader.Close()

	unmarshaler := pmetric.ProtoUnmarshaler{}
	var allMetrics []pmetric.Metrics

	for {
		var sizeBytes [4]byte
		n, err := zreader.Read(sizeBytes[:])
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		if n != 4 {
			return nil, fmt.Errorf("invalid input: expected 4 bytes")
		}

		bytesSize := binary.BigEndian.Uint32(sizeBytes[:])
		payload := make([]byte, bytesSize)

		_, err = io.ReadFull(zreader, payload)
		if err != nil {
			return nil, err
		}

		metrics, err := unmarshaler.UnmarshalMetrics(payload)
		if err != nil {
			return nil, err
		}

		allMetrics = append(allMetrics, metrics)
	}

	return allMetrics, nil
}

func rebatchMetrics(allMetrics []pmetric.Metrics, batchSize int) []pmetric.Metrics {
	var batches []pmetric.Metrics
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

					if currentCount > 0 && currentCount+dataPointCount > batchSize {
						batches = append(batches, currentBatch)
						currentBatch = pmetric.NewMetrics()
						currentCount = 0
					}

					destRM := findOrCreateResourceMetrics(currentBatch, rm.Resource(), rm.SchemaUrl())
					destSM := findOrCreateScopeMetrics(destRM, sm.Scope(), sm.SchemaUrl())
					m.CopyTo(destSM.Metrics().AppendEmpty())
					currentCount += dataPointCount
				}
			}
		}
	}

	if currentCount > 0 {
		batches = append(batches, currentBatch)
	}

	return batches
}

func findOrCreateResourceMetrics(metrics pmetric.Metrics, resource pcommon.Resource, schemaURL string) pmetric.ResourceMetrics {
	rm := metrics.ResourceMetrics().AppendEmpty()
	resource.CopyTo(rm.Resource())
	rm.SetSchemaUrl(schemaURL)
	return rm
}

func findOrCreateScopeMetrics(rm pmetric.ResourceMetrics, scope pcommon.InstrumentationScope, schemaURL string) pmetric.ScopeMetrics {
	sm := rm.ScopeMetrics().AppendEmpty()
	scope.CopyTo(sm.Scope())
	sm.SetSchemaUrl(schemaURL)
	return sm
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

func analyzeBatch(metrics pmetric.Metrics) *BatchStats {
	stats := newBatchStats()

	for i := 0; i < metrics.ResourceMetrics().Len(); i++ {
		rm := metrics.ResourceMetrics().At(i)

		// Schema URL
		if rm.SchemaUrl() != "" {
			stats.SchemaURLs[rm.SchemaUrl()] = struct{}{}
		}

		// Resource attributes
		rm.Resource().Attributes().Range(func(k string, v pcommon.Value) bool {
			stats.ResourceAttrKeys[k] = struct{}{}
			if _, ok := stats.ResourceAttrValues[k]; !ok {
				stats.ResourceAttrValues[k] = make(map[string]struct{})
			}
			stats.ResourceAttrValues[k][v.AsString()] = struct{}{}
			return true
		})

		for j := 0; j < rm.ScopeMetrics().Len(); j++ {
			sm := rm.ScopeMetrics().At(j)

			// Scope schema URL
			if sm.SchemaUrl() != "" {
				stats.SchemaURLs[sm.SchemaUrl()] = struct{}{}
			}

			// Scope name and version
			scope := sm.Scope()
			if scope.Name() != "" {
				stats.ScopeNames[scope.Name()] = struct{}{}
			}
			if scope.Version() != "" {
				stats.ScopeVersions[scope.Version()] = struct{}{}
			}

			// Scope attributes
			scope.Attributes().Range(func(k string, v pcommon.Value) bool {
				stats.ScopeAttrKeys[k] = struct{}{}
				if _, ok := stats.ScopeAttrValues[k]; !ok {
					stats.ScopeAttrValues[k] = make(map[string]struct{})
				}
				stats.ScopeAttrValues[k][v.AsString()] = struct{}{}
				return true
			})

			for k := 0; k < sm.Metrics().Len(); k++ {
				m := sm.Metrics().At(k)

				// Metric metadata
				stats.MetricNames[m.Name()] = struct{}{}
				if m.Description() != "" {
					stats.MetricDescriptions[m.Description()] = struct{}{}
				}
				if m.Unit() != "" {
					stats.MetricUnits[m.Unit()] = struct{}{}
				}

				// Data point attributes
				analyzeDataPointAttributes(m, stats)
			}
		}
	}

	return stats
}

func analyzeDataPointAttributes(m pmetric.Metric, stats *BatchStats) {
	var analyzeAttrs func(attrs pcommon.Map)
	analyzeAttrs = func(attrs pcommon.Map) {
		attrs.Range(func(k string, v pcommon.Value) bool {
			stats.DPAttrKeys[k] = struct{}{}
			if _, ok := stats.DPAttrValues[k]; !ok {
				stats.DPAttrValues[k] = make(map[string]struct{})
			}
			stats.DPAttrValues[k][v.AsString()] = struct{}{}
			return true
		})
	}

	switch m.Type() {
	case pmetric.MetricTypeGauge:
		dps := m.Gauge().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			analyzeAttrs(dps.At(i).Attributes())
		}
	case pmetric.MetricTypeSum:
		dps := m.Sum().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			analyzeAttrs(dps.At(i).Attributes())
		}
	case pmetric.MetricTypeHistogram:
		dps := m.Histogram().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			analyzeAttrs(dps.At(i).Attributes())
		}
	case pmetric.MetricTypeExponentialHistogram:
		dps := m.ExponentialHistogram().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			analyzeAttrs(dps.At(i).Attributes())
		}
	case pmetric.MetricTypeSummary:
		dps := m.Summary().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			analyzeAttrs(dps.At(i).Attributes())
		}
	}
}

func updateGlobalStats(global *GlobalStats, batch *BatchStats) {
	// Update simple fields
	updateFieldStats(global.ResourceAttrKeys, batch.ResourceAttrKeys)
	updateFieldStats(global.ScopeNames, batch.ScopeNames)
	updateFieldStats(global.ScopeVersions, batch.ScopeVersions)
	updateFieldStats(global.ScopeAttrKeys, batch.ScopeAttrKeys)
	updateFieldStats(global.MetricNames, batch.MetricNames)
	updateFieldStats(global.MetricDescriptions, batch.MetricDescriptions)
	updateFieldStats(global.MetricUnits, batch.MetricUnits)
	updateFieldStats(global.DPAttrKeys, batch.DPAttrKeys)
	updateFieldStats(global.SchemaURLs, batch.SchemaURLs)

	// Update per-key attribute values
	for key, values := range batch.ResourceAttrValues {
		if _, ok := global.ResourceAttrValues[key]; !ok {
			global.ResourceAttrValues[key] = &FieldStats{
				Name:         fmt.Sprintf("resource.attr[%s]", key),
				UniqueValues: make(map[string]struct{}),
			}
		}
		updateFieldStats(global.ResourceAttrValues[key], values)
	}

	for key, values := range batch.ScopeAttrValues {
		if _, ok := global.ScopeAttrValues[key]; !ok {
			global.ScopeAttrValues[key] = &FieldStats{
				Name:         fmt.Sprintf("scope.attr[%s]", key),
				UniqueValues: make(map[string]struct{}),
			}
		}
		updateFieldStats(global.ScopeAttrValues[key], values)
	}

	for key, values := range batch.DPAttrValues {
		if _, ok := global.DPAttrValues[key]; !ok {
			global.DPAttrValues[key] = &FieldStats{
				Name:         fmt.Sprintf("datapoint.attr[%s]", key),
				UniqueValues: make(map[string]struct{}),
			}
		}
		updateFieldStats(global.DPAttrValues[key], values)
	}
}

func updateFieldStats(field *FieldStats, batchValues map[string]struct{}) {
	batchCount := len(batchValues)
	if batchCount > field.MaxPerBatch {
		field.MaxPerBatch = batchCount
	}
	for v := range batchValues {
		field.UniqueValues[v] = struct{}{}
	}
}

func printResults(global *GlobalStats) {
	fmt.Println("=== Cardinality Analysis ===")
	fmt.Println()
	fmt.Printf("%-45s %15s %15s   %s\n", "Field", "Max/Batch", "Total Unique", "Sample Values")
	fmt.Println(strings.Repeat("-", 120))

	// Collect all stats for sorting
	var allStats []*FieldStats

	allStats = append(allStats, global.ResourceAttrKeys)
	for _, fs := range global.ResourceAttrValues {
		allStats = append(allStats, fs)
	}
	allStats = append(allStats, global.ScopeNames)
	allStats = append(allStats, global.ScopeVersions)
	allStats = append(allStats, global.ScopeAttrKeys)
	for _, fs := range global.ScopeAttrValues {
		allStats = append(allStats, fs)
	}
	allStats = append(allStats, global.MetricNames)
	allStats = append(allStats, global.MetricDescriptions)
	allStats = append(allStats, global.MetricUnits)
	allStats = append(allStats, global.DPAttrKeys)
	for _, fs := range global.DPAttrValues {
		allStats = append(allStats, fs)
	}
	allStats = append(allStats, global.SchemaURLs)

	// Sort by field name
	sort.Slice(allStats, func(i, j int) bool {
		return allStats[i].Name < allStats[j].Name
	})

	for _, fs := range allStats {
		if len(fs.UniqueValues) == 0 {
			continue
		}
		samples := getSamples(fs.UniqueValues, 3)
		fmt.Printf("%-45s %15d %15d   %s\n", fs.Name, fs.MaxPerBatch, len(fs.UniqueValues), samples)
	}
}

func getSamples(values map[string]struct{}, n int) string {
	var samples []string
	for v := range values {
		if len(samples) >= n {
			break
		}
		// Truncate long values
		if len(v) > 30 {
			v = v[:27] + "..."
		}
		samples = append(samples, v)
	}
	return "[" + strings.Join(samples, ", ") + "]"
}
