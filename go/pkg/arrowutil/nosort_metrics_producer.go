// Package arrowutil provides utilities for working with Apache Arrow IPC format.
package arrowutil

import (
	"bytes"
	"errors"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"go.opentelemetry.io/collector/pdata/pmetric"

	colarspb "github.com/open-telemetry/otel-arrow/go/api/experimental/arrow/v1"
	cfg "github.com/open-telemetry/otel-arrow/go/pkg/config"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/common/schema"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/common/schema/builder"
	config "github.com/open-telemetry/otel-arrow/go/pkg/otel/common/schema/config"
	metricsarrow "github.com/open-telemetry/otel-arrow/go/pkg/otel/metrics/arrow"
	pstats "github.com/open-telemetry/otel-arrow/go/pkg/otel/stats"
	"github.com/open-telemetry/otel-arrow/go/pkg/record_message"
)

// NoSortMetricsProducer is a metrics-only producer that disables all sorting optimizations.
// This is used for benchmarking to measure the impact of sorting on compression.
type NoSortMetricsProducer struct {
	pool            memory.Allocator
	zstd            bool
	streamProducers map[string]*metricsStreamProducer
	nextSchemaId    int64
	batchId         int64

	metricsBuilder       *metricsarrow.MetricsBuilder
	metricsRecordBuilder *builder.RecordBuilderExt

	stats *pstats.ProducerStats
}

type metricsStreamProducer struct {
	output         bytes.Buffer
	ipcWriter      *ipc.Writer
	schemaID       string
	lastProduction time.Time
	schema         *arrow.Schema
	payloadType    record_message.PayloadType
}

// NewNoSortMetricsProducer creates a new metrics producer with all sorting disabled.
func NewNoSortMetricsProducer(options ...cfg.Option) *NoSortMetricsProducer {
	// Default configuration
	conf := cfg.DefaultConfig()
	for _, opt := range options {
		opt(conf)
	}

	stats := pstats.NewProducerStats()

	// Create record builder for metrics
	metricsRecordBuilder := builder.NewRecordBuilderExt(
		conf.Pool,
		metricsarrow.MetricsSchema,
		config.NewDictionary(conf.LimitIndexSize, conf.DictResetThreshold),
		stats,
		conf.Observer,
	)
	metricsRecordBuilder.SetLabel("metrics")

	// Use NewNoSortConfig instead of NewConfig - this is the key difference!
	metricsBuilder, err := metricsarrow.NewMetricsBuilder(
		metricsRecordBuilder,
		metricsarrow.NewNoSortConfig(conf), // No sorting!
		stats,
		conf.Observer,
	)
	if err != nil {
		panic(err)
	}

	return &NoSortMetricsProducer{
		pool:                 conf.Pool,
		zstd:                 conf.Zstd,
		streamProducers:      make(map[string]*metricsStreamProducer),
		batchId:              0,
		metricsBuilder:       metricsBuilder,
		metricsRecordBuilder: metricsRecordBuilder,
		stats:                stats,
	}
}

// BatchArrowRecordsFromMetrics produces a BatchArrowRecords message from metrics.
func (p *NoSortMetricsProducer) BatchArrowRecordsFromMetrics(metrics pmetric.Metrics) (*colarspb.BatchArrowRecords, error) {
	// Build main record
	record, err := p.buildMetricsRecord(metrics)
	if err != nil {
		return nil, err
	}

	// Build related records
	rms, err := p.metricsBuilder.RelatedData().BuildRecordMessages()
	if err != nil {
		return nil, err
	}

	schemaID := p.metricsRecordBuilder.SchemaID()

	// Main record first
	rms = append([]*record_message.RecordMessage{record_message.NewMetricsMessage(schemaID, record)}, rms...)

	bar, err := p.produce(rms)
	if err != nil {
		return nil, err
	}
	p.stats.MetricsBatchesProduced++
	return bar, nil
}

func (p *NoSortMetricsProducer) buildMetricsRecord(metrics pmetric.Metrics) (arrow.Record, error) {
	schemaNotUpToDateCount := 0

	for {
		p.metricsBuilder.RelatedData().Reset()

		if err := p.metricsBuilder.Append(metrics); err != nil {
			return nil, err
		}

		record, err := p.metricsBuilder.Build()
		if err != nil {
			if record != nil {
				record.Release()
			}

			if errors.Is(err, schema.ErrSchemaNotUpToDate) {
				schemaNotUpToDateCount++
				if schemaNotUpToDateCount > 5 {
					panic("Too many consecutive schema updates")
				}
				continue
			}
			return nil, err
		}
		return record, nil
	}
}

func (p *NoSortMetricsProducer) produce(rms []*record_message.RecordMessage) (*colarspb.BatchArrowRecords, error) {
	oapl := make([]*colarspb.ArrowPayload, len(rms))

	for i, rm := range rms {
		err := func() error {
			defer func() {
				rm.Record().Release()
			}()

			sp := p.streamProducers[rm.SchemaID()]
			if sp == nil {
				// Cleanup previous stream producer with same PayloadType
				for ssID, existingSp := range p.streamProducers {
					if existingSp.payloadType == rm.PayloadType() {
						if err := existingSp.ipcWriter.Close(); err != nil {
							return err
						}
						delete(p.streamProducers, ssID)
					}
				}

				var buf bytes.Buffer
				sp = &metricsStreamProducer{
					output:      buf,
					schemaID:    string(rune('0' + p.nextSchemaId)),
					payloadType: rm.PayloadType(),
				}
				p.streamProducers[rm.SchemaID()] = sp
				p.nextSchemaId++
			}

			sp.lastProduction = time.Now()
			sp.schema = rm.Record().Schema()

			if sp.ipcWriter == nil {
				options := []ipc.Option{
					ipc.WithAllocator(p.pool),
					ipc.WithSchema(rm.Record().Schema()),
					ipc.WithDictionaryDeltas(true),
				}
				if p.zstd {
					options = append(options, ipc.WithZstd())
				}
				sp.ipcWriter = ipc.NewWriter(&sp.output, options...)
			}

			err := sp.ipcWriter.Write(rm.Record())
			if err != nil {
				return err
			}
			outputBuf := sp.output.Bytes()
			buf := make([]byte, len(outputBuf))
			copy(buf, outputBuf)

			sp.output.Reset()

			oapl[i] = &colarspb.ArrowPayload{
				SchemaId: sp.schemaID,
				Type:     rm.PayloadType(),
				Record:   buf,
			}
			return nil
		}()
		if err != nil {
			return nil, err
		}
	}

	batchId := p.batchId
	p.batchId++

	return &colarspb.BatchArrowRecords{
		BatchId:       batchId,
		ArrowPayloads: oapl,
	}, nil
}

// Close closes the producer and releases resources.
func (p *NoSortMetricsProducer) Close() error {
	p.metricsBuilder.Release()
	p.metricsRecordBuilder.Release()

	for _, sp := range p.streamProducers {
		if err := sp.ipcWriter.Close(); err != nil {
			return err
		}
	}
	return nil
}
