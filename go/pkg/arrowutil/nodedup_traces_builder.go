// Package arrowutil provides utilities for working with Apache Arrow IPC format.
package arrowutil

// NoDedupTracesBuilder is a traces builder that disables resource/scope deduplication.
// This is used for benchmarking to measure the impact of columnar normalization.

import (
	"math"

	"github.com/apache/arrow-go/v18/arrow"
	"go.opentelemetry.io/collector/pdata/ptrace"

	acommon "github.com/open-telemetry/otel-arrow/go/pkg/otel/common/arrow"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/common/schema/builder"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/constants"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/observer"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/stats"
	tracesarrow "github.com/open-telemetry/otel-arrow/go/pkg/otel/traces/arrow"
	"github.com/open-telemetry/otel-arrow/go/pkg/werror"
)

// NoDedupTracesBuilder is a traces builder that disables resource/scope deduplication.
// Unlike the standard TracesBuilder, this one creates a new resource/scope ID for every
// span instead of deduplicating them.
type NoDedupTracesBuilder struct {
	released bool

	builder *builder.RecordBuilderExt // Record builder

	rb    *acommon.ResourceBuilder        // `resource` builder
	scb   *acommon.ScopeBuilder           // `scope` builder
	sschb *builder.StringBuilder          // scope `schema_url` builder
	ib    *builder.Uint16DeltaBuilder     //  id builder
	stunb *builder.TimestampBuilder       // start time unix nano builder
	dtunb *builder.DurationBuilder        // duration time unix nano builder
	tib   *builder.FixedSizeBinaryBuilder // trace id builder
	sib   *builder.FixedSizeBinaryBuilder // span id builder
	tsb   *builder.StringBuilder          // trace state builder
	psib  *builder.FixedSizeBinaryBuilder // parent span id builder
	fb    *builder.Uint32Builder          // flags builder
	nb    *builder.StringBuilder          // name builder
	kb    *builder.Int32Builder           // kind builder
	dacb  *builder.Uint32Builder          // dropped attributes count builder
	decb  *builder.Uint32Builder          // dropped events count builder
	dlcb  *builder.Uint32Builder          // dropped links count builder
	sb    *tracesarrow.StatusBuilder      // status builder

	optimizer *tracesarrow.TracesOptimizer

	relatedData *tracesarrow.RelatedData
}

// NewNoDedupTracesBuilder creates a new NoDedupTracesBuilder.
func NewNoDedupTracesBuilder(
	rBuilder *builder.RecordBuilderExt,
	cfg *tracesarrow.Config,
	stats *stats.ProducerStats,
	observer observer.ProducerObserver,
) (*NoDedupTracesBuilder, error) {
	relatedData, err := tracesarrow.NewRelatedData(cfg, stats, observer)
	if err != nil {
		panic(err)
	}

	optimizer := tracesarrow.NewTracesOptimizer(cfg.Span.Sorter)

	b := &NoDedupTracesBuilder{
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

func (b *NoDedupTracesBuilder) init() error {
	ib := b.builder.Uint16DeltaBuilder(constants.ID)
	ib.SetMaxDelta(1)

	b.ib = ib
	b.rb = acommon.ResourceBuilderFrom(b.builder.StructBuilder(constants.Resource))
	b.scb = acommon.ScopeBuilderFrom(b.builder.StructBuilder(constants.Scope))
	b.sschb = b.builder.StringBuilder(constants.SchemaUrl)

	b.stunb = b.builder.TimestampBuilder(constants.StartTimeUnixNano)
	b.dtunb = b.builder.DurationBuilder(constants.DurationTimeUnixNano)
	b.tib = b.builder.FixedSizeBinaryBuilder(constants.TraceId)
	b.sib = b.builder.FixedSizeBinaryBuilder(constants.SpanId)
	b.tsb = b.builder.StringBuilder(constants.TraceState)
	b.psib = b.builder.FixedSizeBinaryBuilder(constants.ParentSpanId)
	b.nb = b.builder.StringBuilder(constants.Name)
	b.fb = b.builder.Uint32Builder(constants.Flags)
	b.kb = b.builder.Int32Builder(constants.KIND)
	b.dacb = b.builder.Uint32Builder(constants.DroppedAttributesCount)
	b.decb = b.builder.Uint32Builder(constants.DroppedEventsCount)
	b.dlcb = b.builder.Uint32Builder(constants.DroppedLinksCount)
	b.sb = tracesarrow.StatusBuilderFrom(b.builder.StructBuilder(constants.Status))

	return nil
}

func (b *NoDedupTracesBuilder) RelatedData() *tracesarrow.RelatedData {
	return b.relatedData
}

// Build builds an Arrow Record from the builder.
func (b *NoDedupTracesBuilder) Build() (record arrow.Record, err error) {
	if b.released {
		return nil, werror.Wrap(acommon.ErrBuilderAlreadyReleased)
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

// Append appends a new set of resource spans to the builder.
// Unlike the standard TracesBuilder, this does NOT deduplicate resources/scopes.
// Every span gets its own resource and scope ID.
func (b *NoDedupTracesBuilder) Append(traces ptrace.Traces) error {
	if b.released {
		return werror.Wrap(acommon.ErrBuilderAlreadyReleased)
	}

	optimTraces := b.optimizer.Optimize(traces)

	spanID := uint16(0)
	resID := int64(-1)
	scopeID := int64(-1)
	// NOTE: Removed resSpanID and scopeSpanID tracking variables
	var err error

	attrsAccu := b.relatedData.AttrsBuilders().Span().Accumulator()
	eventsAccu := b.relatedData.EventBuilder().Accumulator()
	linksAccu := b.relatedData.LinkBuilder().Accumulator()

	b.builder.Reserve(len(optimTraces.Spans))

	for _, span := range optimTraces.Spans {
		spanAttrs := span.Span.Attributes()
		spanEvents := span.Span.Events()
		spanLinks := span.Span.Links()

		ID := spanID
		if spanAttrs.Len() == 0 && spanEvents.Len() == 0 && spanLinks.Len() == 0 {
			// No related data found
			b.ib.AppendNull()
		} else {
			b.ib.Append(ID)
			spanID++
		}

		// === Process resource and schema URL ===
		// NOTE: NO DEDUP - always increment resID and append attributes
		resAttrs := span.Resource.Attributes()
		resID++
		err = b.relatedData.AttrsBuilders().Resource().Accumulator().
			AppendWithID(uint16(resID), resAttrs)
		if err != nil {
			return werror.Wrap(err)
		}

		// Check resID validity
		if resID > math.MaxUint16 {
			return werror.WrapWithContext(acommon.ErrInvalidResourceID, map[string]interface{}{
				"resource_id": resID,
			})
		}
		// Append the resource schema URL if exists
		if err = b.rb.Append(resID, span.Resource, span.ResourceSchemaUrl); err != nil {
			return werror.Wrap(err)
		}

		// === Process scope and schema URL ===
		// NOTE: NO DEDUP - always increment scopeID and append attributes
		scopeAttrs := span.Scope.Attributes()
		scopeID++
		err = b.relatedData.AttrsBuilders().Scope().Accumulator().
			AppendWithID(uint16(scopeID), scopeAttrs)
		if err != nil {
			return werror.Wrap(err)
		}

		// Check scopeID validity
		if scopeID > math.MaxUint16 {
			return werror.WrapWithContext(acommon.ErrInvalidScopeID, map[string]interface{}{
				"scope_id": scopeID,
			})
		}
		// Append the scope name, version, and schema URL (if exists)
		if err = b.scb.Append(scopeID, span.Scope); err != nil {
			return werror.Wrap(err)
		}
		b.sschb.AppendNonEmpty(span.ScopeSchemaUrl)

		// === Process span ===
		b.stunb.Append(arrow.Timestamp(span.Span.StartTimestamp()))
		duration := span.Span.EndTimestamp().AsTime().Sub(span.Span.StartTimestamp().AsTime()).Nanoseconds()
		b.dtunb.Append(arrow.Duration(duration))
		tib := span.Span.TraceID()
		b.tib.Append(tib[:])
		sib := span.Span.SpanID()
		b.sib.Append(sib[:])
		b.tsb.AppendNonEmpty(span.Span.TraceState().AsRaw())
		psib := span.Span.ParentSpanID()
		if psib.IsEmpty() {
			b.psib.AppendNull()
		} else {
			b.psib.Append(psib[:])
		}
		b.fb.Append(uint32(span.Span.Flags()))
		b.nb.AppendNonEmpty(span.Span.Name())
		b.kb.AppendNonZero(int32(span.Span.Kind()))

		// Span Attributes
		if spanAttrs.Len() > 0 {
			err = attrsAccu.AppendWithID(ID, spanAttrs)
			if err != nil {
				return werror.Wrap(err)
			}
		}
		b.dacb.AppendNonZero(span.Span.DroppedAttributesCount())

		// Events
		if spanEvents.Len() > 0 {
			err = eventsAccu.Append(ID, spanEvents)
			if err != nil {
				return werror.Wrap(err)
			}
		}
		b.decb.AppendNonZero(span.Span.DroppedEventsCount())

		// Links
		if spanLinks.Len() > 0 {
			err = linksAccu.Append(ID, spanLinks)
			if err != nil {
				return werror.Wrap(err)
			}
		}
		b.dlcb.AppendNonZero(span.Span.DroppedLinksCount())

		if err = b.sb.Append(span.Span.Status()); err != nil {
			return werror.Wrap(err)
		}
	}
	return nil
}

// Release releases the memory allocated by the builder.
func (b *NoDedupTracesBuilder) Release() {
	if !b.released {
		b.relatedData.Release()
		b.released = true
	}
}
