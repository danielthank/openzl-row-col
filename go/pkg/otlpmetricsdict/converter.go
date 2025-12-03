package otlpmetricsdict

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// StringTable builds and manages the string dictionary
type StringTable struct {
	strings []string
	index   map[string]int32
}

func NewStringTable() *StringTable {
	return &StringTable{
		strings: make([]string, 0),
		index:   make(map[string]int32),
	}
}

func (st *StringTable) Add(s string) int32 {
	if idx, ok := st.index[s]; ok {
		return idx
	}
	idx := int32(len(st.strings))
	st.strings = append(st.strings, s)
	st.index[s] = idx
	return idx
}

func (st *StringTable) Strings() []string {
	return st.strings
}

// Convert transforms pmetric.Metrics to MetricsDictBatch
func Convert(metrics pmetric.Metrics) *MetricsDictBatch {
	st := NewStringTable()
	batch := &MetricsDictBatch{}

	// Single pass: convert metrics, building string table as we go
	batch.ResourceMetrics = convertResourceMetrics(metrics, st)
	batch.StringTable = st.Strings()

	return batch
}

func convertResourceMetrics(metrics pmetric.Metrics, st *StringTable) []*ResourceMetrics {
	result := make([]*ResourceMetrics, 0, metrics.ResourceMetrics().Len())

	for i := 0; i < metrics.ResourceMetrics().Len(); i++ {
		rm := metrics.ResourceMetrics().At(i)
		result = append(result, &ResourceMetrics{
			Resource:     convertResource(rm.Resource(), st),
			ScopeMetrics: convertScopeMetrics(rm.ScopeMetrics(), st),
			SchemaUrl:    rm.SchemaUrl(),
		})
	}

	return result
}

func convertResource(res pcommon.Resource, st *StringTable) *Resource {
	return &Resource{
		Attributes:             convertAttributes(res.Attributes(), st),
		DroppedAttributesCount: res.DroppedAttributesCount(),
	}
}

func convertScopeMetrics(sms pmetric.ScopeMetricsSlice, st *StringTable) []*ScopeMetrics {
	result := make([]*ScopeMetrics, 0, sms.Len())

	for i := 0; i < sms.Len(); i++ {
		sm := sms.At(i)
		result = append(result, &ScopeMetrics{
			Scope:     convertScope(sm.Scope(), st),
			Metrics:   convertMetrics(sm.Metrics(), st),
			SchemaUrl: sm.SchemaUrl(),
		})
	}

	return result
}

func convertScope(scope pcommon.InstrumentationScope, st *StringTable) *InstrumentationScope {
	return &InstrumentationScope{
		Name:                   scope.Name(),
		Version:                scope.Version(),
		Attributes:             convertAttributes(scope.Attributes(), st),
		DroppedAttributesCount: scope.DroppedAttributesCount(),
	}
}

func convertMetrics(ms pmetric.MetricSlice, st *StringTable) []*Metric {
	result := make([]*Metric, 0, ms.Len())

	for i := 0; i < ms.Len(); i++ {
		m := ms.At(i)
		metric := &Metric{
			Name:        m.Name(),
			Description: m.Description(),
			Unit:        m.Unit(),
			Metadata:    convertAttributes(m.Metadata(), st),
		}

		switch m.Type() {
		case pmetric.MetricTypeGauge:
			metric.Data = &Metric_Gauge{
				Gauge: convertGauge(m.Gauge(), st),
			}
		case pmetric.MetricTypeSum:
			metric.Data = &Metric_Sum{
				Sum: convertSum(m.Sum(), st),
			}
		case pmetric.MetricTypeHistogram:
			metric.Data = &Metric_Histogram{
				Histogram: convertHistogram(m.Histogram(), st),
			}
		case pmetric.MetricTypeExponentialHistogram:
			metric.Data = &Metric_ExponentialHistogram{
				ExponentialHistogram: convertExponentialHistogram(m.ExponentialHistogram(), st),
			}
		case pmetric.MetricTypeSummary:
			metric.Data = &Metric_Summary{
				Summary: convertSummary(m.Summary(), st),
			}
		}

		result = append(result, metric)
	}

	return result
}

func convertGauge(g pmetric.Gauge, st *StringTable) *Gauge {
	return &Gauge{
		DataPoints: convertNumberDataPoints(g.DataPoints(), st),
	}
}

func convertSum(s pmetric.Sum, st *StringTable) *Sum {
	return &Sum{
		DataPoints:             convertNumberDataPoints(s.DataPoints(), st),
		AggregationTemporality: convertAggregationTemporality(s.AggregationTemporality()),
		IsMonotonic:            s.IsMonotonic(),
	}
}

func convertHistogram(h pmetric.Histogram, st *StringTable) *Histogram {
	return &Histogram{
		DataPoints:             convertHistogramDataPoints(h.DataPoints(), st),
		AggregationTemporality: convertAggregationTemporality(h.AggregationTemporality()),
	}
}

func convertExponentialHistogram(eh pmetric.ExponentialHistogram, st *StringTable) *ExponentialHistogram {
	return &ExponentialHistogram{
		DataPoints:             convertExponentialHistogramDataPoints(eh.DataPoints(), st),
		AggregationTemporality: convertAggregationTemporality(eh.AggregationTemporality()),
	}
}

func convertSummary(s pmetric.Summary, st *StringTable) *Summary {
	return &Summary{
		DataPoints: convertSummaryDataPoints(s.DataPoints(), st),
	}
}

func convertAggregationTemporality(at pmetric.AggregationTemporality) AggregationTemporality {
	switch at {
	case pmetric.AggregationTemporalityDelta:
		return AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA
	case pmetric.AggregationTemporalityCumulative:
		return AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE
	default:
		return AggregationTemporality_AGGREGATION_TEMPORALITY_UNSPECIFIED
	}
}

func convertNumberDataPoints(dps pmetric.NumberDataPointSlice, st *StringTable) []*NumberDataPoint {
	result := make([]*NumberDataPoint, 0, dps.Len())

	for i := 0; i < dps.Len(); i++ {
		dp := dps.At(i)
		ndp := &NumberDataPoint{
			Attributes:        convertAttributes(dp.Attributes(), st),
			StartTimeUnixNano: uint64(dp.StartTimestamp()),
			TimeUnixNano:      uint64(dp.Timestamp()),
			Exemplars:         convertExemplars(dp.Exemplars(), st),
			Flags:             uint32(dp.Flags()),
		}

		switch dp.ValueType() {
		case pmetric.NumberDataPointValueTypeDouble:
			ndp.Value = &NumberDataPoint_AsDouble{AsDouble: dp.DoubleValue()}
		case pmetric.NumberDataPointValueTypeInt:
			ndp.Value = &NumberDataPoint_AsInt{AsInt: dp.IntValue()}
		}

		result = append(result, ndp)
	}

	return result
}

func convertHistogramDataPoints(dps pmetric.HistogramDataPointSlice, st *StringTable) []*HistogramDataPoint {
	result := make([]*HistogramDataPoint, 0, dps.Len())

	for i := 0; i < dps.Len(); i++ {
		dp := dps.At(i)
		hdp := &HistogramDataPoint{
			Attributes:        convertAttributes(dp.Attributes(), st),
			StartTimeUnixNano: uint64(dp.StartTimestamp()),
			TimeUnixNano:      uint64(dp.Timestamp()),
			Count:             dp.Count(),
			BucketCounts:      dp.BucketCounts().AsRaw(),
			ExplicitBounds:    dp.ExplicitBounds().AsRaw(),
			Exemplars:         convertExemplars(dp.Exemplars(), st),
			Flags:             uint32(dp.Flags()),
		}

		if dp.HasSum() {
			sum := dp.Sum()
			hdp.Sum = &sum
		}
		if dp.HasMin() {
			min := dp.Min()
			hdp.Min = &min
		}
		if dp.HasMax() {
			max := dp.Max()
			hdp.Max = &max
		}

		result = append(result, hdp)
	}

	return result
}

func convertExponentialHistogramDataPoints(dps pmetric.ExponentialHistogramDataPointSlice, st *StringTable) []*ExponentialHistogramDataPoint {
	result := make([]*ExponentialHistogramDataPoint, 0, dps.Len())

	for i := 0; i < dps.Len(); i++ {
		dp := dps.At(i)
		ehdp := &ExponentialHistogramDataPoint{
			Attributes:        convertAttributes(dp.Attributes(), st),
			StartTimeUnixNano: uint64(dp.StartTimestamp()),
			TimeUnixNano:      uint64(dp.Timestamp()),
			Count:             dp.Count(),
			Scale:             dp.Scale(),
			ZeroCount:         dp.ZeroCount(),
			Positive: &ExponentialHistogramDataPoint_Buckets{
				Offset:       dp.Positive().Offset(),
				BucketCounts: dp.Positive().BucketCounts().AsRaw(),
			},
			Negative: &ExponentialHistogramDataPoint_Buckets{
				Offset:       dp.Negative().Offset(),
				BucketCounts: dp.Negative().BucketCounts().AsRaw(),
			},
			Flags:         uint32(dp.Flags()),
			Exemplars:     convertExemplars(dp.Exemplars(), st),
			ZeroThreshold: dp.ZeroThreshold(),
		}

		if dp.HasSum() {
			sum := dp.Sum()
			ehdp.Sum = &sum
		}
		if dp.HasMin() {
			min := dp.Min()
			ehdp.Min = &min
		}
		if dp.HasMax() {
			max := dp.Max()
			ehdp.Max = &max
		}

		result = append(result, ehdp)
	}

	return result
}

func convertSummaryDataPoints(dps pmetric.SummaryDataPointSlice, st *StringTable) []*SummaryDataPoint {
	result := make([]*SummaryDataPoint, 0, dps.Len())

	for i := 0; i < dps.Len(); i++ {
		dp := dps.At(i)
		sdp := &SummaryDataPoint{
			Attributes:        convertAttributes(dp.Attributes(), st),
			StartTimeUnixNano: uint64(dp.StartTimestamp()),
			TimeUnixNano:      uint64(dp.Timestamp()),
			Count:             dp.Count(),
			Sum:               dp.Sum(),
			QuantileValues:    convertQuantileValues(dp.QuantileValues()),
			Flags:             uint32(dp.Flags()),
		}

		result = append(result, sdp)
	}

	return result
}

func convertQuantileValues(qvs pmetric.SummaryDataPointValueAtQuantileSlice) []*SummaryDataPoint_ValueAtQuantile {
	result := make([]*SummaryDataPoint_ValueAtQuantile, 0, qvs.Len())

	for i := 0; i < qvs.Len(); i++ {
		qv := qvs.At(i)
		result = append(result, &SummaryDataPoint_ValueAtQuantile{
			Quantile: qv.Quantile(),
			Value:    qv.Value(),
		})
	}

	return result
}

func convertExemplars(exemplars pmetric.ExemplarSlice, st *StringTable) []*Exemplar {
	result := make([]*Exemplar, 0, exemplars.Len())

	for i := 0; i < exemplars.Len(); i++ {
		e := exemplars.At(i)
		spanID := e.SpanID()
		traceID := e.TraceID()
		ex := &Exemplar{
			FilteredAttributes: convertAttributes(e.FilteredAttributes(), st),
			TimeUnixNano:       uint64(e.Timestamp()),
			SpanId:             spanID[:],
			TraceId:            traceID[:],
		}

		switch e.ValueType() {
		case pmetric.ExemplarValueTypeDouble:
			ex.Value = &Exemplar_AsDouble{AsDouble: e.DoubleValue()}
		case pmetric.ExemplarValueTypeInt:
			ex.Value = &Exemplar_AsInt{AsInt: e.IntValue()}
		}

		result = append(result, ex)
	}

	return result
}

func convertAttributes(attrs pcommon.Map, st *StringTable) []*KeyValue {
	result := make([]*KeyValue, 0, attrs.Len())

	attrs.Range(func(k string, v pcommon.Value) bool {
		result = append(result, &KeyValue{
			KeyRef: st.Add(k),
			Value:  convertAnyValue(v, st),
		})
		return true
	})

	return result
}

func convertAnyValue(v pcommon.Value, st *StringTable) *AnyValue {
	switch v.Type() {
	case pcommon.ValueTypeStr:
		return &AnyValue{Value: &AnyValue_StringValue{StringValue: v.Str()}}
	case pcommon.ValueTypeBool:
		return &AnyValue{Value: &AnyValue_BoolValue{BoolValue: v.Bool()}}
	case pcommon.ValueTypeInt:
		return &AnyValue{Value: &AnyValue_IntValue{IntValue: v.Int()}}
	case pcommon.ValueTypeDouble:
		return &AnyValue{Value: &AnyValue_DoubleValue{DoubleValue: v.Double()}}
	case pcommon.ValueTypeBytes:
		return &AnyValue{Value: &AnyValue_BytesValue{BytesValue: v.Bytes().AsRaw()}}
	case pcommon.ValueTypeSlice:
		return &AnyValue{Value: &AnyValue_ArrayValue{ArrayValue: convertArrayValue(v.Slice(), st)}}
	case pcommon.ValueTypeMap:
		return &AnyValue{Value: &AnyValue_KvlistValue{KvlistValue: convertKeyValueList(v.Map(), st)}}
	default:
		return &AnyValue{}
	}
}

func convertArrayValue(slice pcommon.Slice, st *StringTable) *ArrayValue {
	values := make([]*AnyValue, 0, slice.Len())
	for i := 0; i < slice.Len(); i++ {
		values = append(values, convertAnyValue(slice.At(i), st))
	}
	return &ArrayValue{Values: values}
}

func convertKeyValueList(m pcommon.Map, st *StringTable) *KeyValueList {
	return &KeyValueList{Values: convertAttributes(m, st)}
}
