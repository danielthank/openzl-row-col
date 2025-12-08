// Package arrowutil provides utilities for working with Apache Arrow IPC format.
package arrowutil

// NoDedupMetricsBuilder is a metrics builder that disables resource/scope deduplication.
// This is used for benchmarking to measure the impact of columnar normalization.

import (
	"math"

	"github.com/apache/arrow-go/v18/arrow"
	"go.opentelemetry.io/collector/pdata/pmetric"

	carrow "github.com/open-telemetry/otel-arrow/go/pkg/otel/common/arrow"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/common/schema/builder"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/constants"
	metricsarrow "github.com/open-telemetry/otel-arrow/go/pkg/otel/metrics/arrow"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/observer"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/stats"
	"github.com/open-telemetry/otel-arrow/go/pkg/werror"
)

// NoDedupMetricsBuilder is a metrics builder that disables resource/scope deduplication.
// Unlike the standard MetricsBuilder, this one creates a new resource/scope ID for every
// metric instead of deduplicating them.
type NoDedupMetricsBuilder struct {
	released bool

	builder *builder.RecordBuilderExt     // Record builder
	rb      *carrow.ResourceBuilder       // `resource` builder
	scb     *carrow.ScopeBuilder          // `scope` builder
	sschb   *builder.StringBuilder        // scope `schema_url` builder
	ib      *builder.Uint16DeltaBuilder   //  id builder
	mtb     *builder.Uint8Builder         // metric type builder
	nb      *builder.StringBuilder        // metric name builder
	db      *builder.StringBuilder        // metric description builder
	ub      *builder.StringBuilder        // metric unit builder
	atb     *builder.Int32Builder         // aggregation temporality builder
	imb     *builder.BooleanBuilder       // is monotonic builder

	optimizer *metricsarrow.MetricsOptimizer

	relatedData *metricsarrow.RelatedData
}

// NewNoDedupMetricsBuilder creates a new NoDedupMetricsBuilder.
func NewNoDedupMetricsBuilder(
	rBuilder *builder.RecordBuilderExt,
	cfg *metricsarrow.Config,
	stats *stats.ProducerStats,
	observer observer.ProducerObserver,
) (*NoDedupMetricsBuilder, error) {
	relatedData, err := metricsarrow.NewRelatedData(cfg, stats, observer)
	if err != nil {
		panic(err)
	}

	optimizer := metricsarrow.NewMetricsOptimizer(cfg.Metric.Sorter)

	b := &NoDedupMetricsBuilder{
		released:    false,
		builder:     rBuilder,
		optimizer:   optimizer,
		relatedData: relatedData,
	}

	if err := b.init(); err != nil {
		return nil, werror.Wrap(err)
	}

	return b, nil
}

func (b *NoDedupMetricsBuilder) init() error {
	b.ib = b.builder.Uint16DeltaBuilder(constants.ID)
	b.ib.SetMaxDelta(1)

	b.rb = carrow.ResourceBuilderFrom(b.builder.StructBuilder(constants.Resource))
	b.scb = carrow.ScopeBuilderFrom(b.builder.StructBuilder(constants.Scope))
	b.sschb = b.builder.StringBuilder(constants.SchemaUrl)

	b.mtb = b.builder.Uint8Builder(constants.MetricType)
	b.nb = b.builder.StringBuilder(constants.Name)
	b.db = b.builder.StringBuilder(constants.Description)
	b.ub = b.builder.StringBuilder(constants.Unit)
	b.atb = b.builder.Int32Builder(constants.AggregationTemporality)
	b.imb = b.builder.BooleanBuilder(constants.IsMonotonic)

	return nil
}

func (b *NoDedupMetricsBuilder) RelatedData() *metricsarrow.RelatedData {
	return b.relatedData
}

// Build builds an Arrow Record from the builder.
func (b *NoDedupMetricsBuilder) Build() (record arrow.Record, err error) {
	if b.released {
		return nil, werror.Wrap(carrow.ErrBuilderAlreadyReleased)
	}

	record, err = b.builder.NewRecord()
	if err != nil {
		initErr := b.init()
		if initErr != nil {
			err = werror.Wrap(initErr)
		}
	}

	return
}

// Append appends a new set of resource metrics to the builder.
// Unlike the standard MetricsBuilder, this does NOT deduplicate resources/scopes.
// Every metric gets its own resource and scope ID.
func (b *NoDedupMetricsBuilder) Append(metrics pmetric.Metrics) error {
	if b.released {
		return werror.Wrap(carrow.ErrBuilderAlreadyReleased)
	}

	optimizedMetrics := b.optimizer.Optimize(metrics)

	metricID := uint16(0)
	resID := int64(-1)
	scopeID := int64(-1)
	// NOTE: Removed resMetricsID and scopeMetricsID tracking variables
	var err error

	b.builder.Reserve(len(optimizedMetrics.Metrics))

	for _, metric := range optimizedMetrics.Metrics {
		ID := metricID

		b.ib.Append(ID)
		metricID++

		// === Process resource and schema URL ===
		// NOTE: NO DEDUP - always increment resID and append attributes
		resAttrs := metric.Resource.Attributes()
		resID++
		err = b.relatedData.AttrsBuilders().Resource().Accumulator().
			AppendWithID(uint16(resID), resAttrs)
		if err != nil {
			return werror.Wrap(err)
		}

		// Check resID validity
		if resID > math.MaxUint16 {
			return werror.WrapWithContext(carrow.ErrInvalidResourceID, map[string]interface{}{
				"resource_id": resID,
			})
		}
		// Append the resource schema URL if exists
		if err = b.rb.Append(resID, metric.Resource, metric.ResourceSchemaUrl); err != nil {
			return werror.Wrap(err)
		}

		// === Process scope and schema URL ===
		// NOTE: NO DEDUP - always increment scopeID and append attributes
		scopeAttrs := metric.Scope.Attributes()
		scopeID++
		err = b.relatedData.AttrsBuilders().Scope().Accumulator().
			AppendWithID(uint16(scopeID), scopeAttrs)
		if err != nil {
			return werror.Wrap(err)
		}

		// Check scopeID validity
		if scopeID > math.MaxUint16 {
			return werror.WrapWithContext(carrow.ErrInvalidScopeID, map[string]interface{}{
				"scope_id": scopeID,
			})
		}
		// Append the scope name, version, and schema URL (if exists)
		if err = b.scb.Append(scopeID, metric.Scope); err != nil {
			return werror.Wrap(err)
		}
		b.sschb.AppendNonEmpty(metric.ScopeSchemaUrl)

		// === Process metric ===
		b.mtb.Append(uint8(metric.Metric.Type()))
		b.nb.AppendNonEmpty(metric.Metric.Name())
		b.db.Append(metric.Metric.Description())
		b.ub.AppendNonEmpty(metric.Metric.Unit())

		switch metric.Metric.Type() {
		case pmetric.MetricTypeGauge:
			b.atb.AppendNull()
			b.imb.AppendNull()
			dps := metric.Metric.Gauge().DataPoints()
			for i := 0; i < dps.Len(); i++ {
				dp := dps.At(i)
				b.relatedData.NumberDPBuilder().Accumulator().Append(ID, &dp)
			}
		case pmetric.MetricTypeSum:
			sum := metric.Metric.Sum()
			b.atb.Append(int32(sum.AggregationTemporality()))
			b.imb.Append(sum.IsMonotonic())
			dps := sum.DataPoints()
			for i := 0; i < dps.Len(); i++ {
				dp := dps.At(i)
				b.relatedData.NumberDPBuilder().Accumulator().Append(ID, &dp)
			}
		case pmetric.MetricTypeSummary:
			b.atb.AppendNull()
			b.imb.AppendNull()
			dps := metric.Metric.Summary().DataPoints()
			b.relatedData.SummaryDPBuilder().Accumulator().Append(ID, dps)
		case pmetric.MetricTypeHistogram:
			histogram := metric.Metric.Histogram()
			b.atb.Append(int32(histogram.AggregationTemporality()))
			b.imb.AppendNull()
			dps := histogram.DataPoints()
			b.relatedData.HistogramDPBuilder().Accumulator().Append(ID, dps)
		case pmetric.MetricTypeExponentialHistogram:
			exponentialHistogram := metric.Metric.ExponentialHistogram()
			b.atb.Append(int32(exponentialHistogram.AggregationTemporality()))
			b.imb.AppendNull()
			dps := exponentialHistogram.DataPoints()
			b.relatedData.EHistogramDPBuilder().Accumulator().Append(ID, dps)
		case pmetric.MetricTypeEmpty:
			b.atb.AppendNull()
			b.imb.AppendNull()
		default:
			// Unknown metric types are ignored
		}
	}
	return nil
}

// Release releases the memory allocated by the builder.
func (b *NoDedupMetricsBuilder) Release() {
	if !b.released {
		b.relatedData.Release()
		b.released = true
	}
}
