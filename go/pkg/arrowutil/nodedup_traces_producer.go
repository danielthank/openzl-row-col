// Package arrowutil provides utilities for working with Apache Arrow IPC format.
package arrowutil

import (
	"bytes"
	"errors"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"go.opentelemetry.io/collector/pdata/ptrace"

	colarspb "github.com/open-telemetry/otel-arrow/go/api/experimental/arrow/v1"
	cfg "github.com/open-telemetry/otel-arrow/go/pkg/config"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/common/schema"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/common/schema/builder"
	config "github.com/open-telemetry/otel-arrow/go/pkg/otel/common/schema/config"
	pstats "github.com/open-telemetry/otel-arrow/go/pkg/otel/stats"
	tracesarrow "github.com/open-telemetry/otel-arrow/go/pkg/otel/traces/arrow"
	"github.com/open-telemetry/otel-arrow/go/pkg/record_message"
)

// NoDedupTracesProducer is a traces-only producer that disables resource/scope deduplication.
// This is used for benchmarking to measure the impact of columnar normalization on compression.
type NoDedupTracesProducer struct {
	pool            memory.Allocator
	zstd            bool
	streamProducers map[string]*nodedupTracesStreamProducer
	nextSchemaId    int64
	batchId         int64

	tracesBuilder       *NoDedupTracesBuilder
	tracesRecordBuilder *builder.RecordBuilderExt

	stats *pstats.ProducerStats
}

type nodedupTracesStreamProducer struct {
	output         bytes.Buffer
	ipcWriter      *ipc.Writer
	schemaID       string
	lastProduction time.Time
	schema         *arrow.Schema
	payloadType    record_message.PayloadType
}

// NewNoDedupTracesProducer creates a new traces producer with resource/scope deduplication disabled.
func NewNoDedupTracesProducer(options ...cfg.Option) *NoDedupTracesProducer {
	// Default configuration
	conf := cfg.DefaultConfig()
	for _, opt := range options {
		opt(conf)
	}

	stats := pstats.NewProducerStats()

	// Create record builder for traces
	tracesRecordBuilder := builder.NewRecordBuilderExt(
		conf.Pool,
		tracesarrow.TracesSchema,
		config.NewDictionary(conf.LimitIndexSize, conf.DictResetThreshold),
		stats,
		conf.Observer,
	)
	tracesRecordBuilder.SetLabel("traces")

	// Use NoDedupTracesBuilder - disables resource/scope deduplication
	tracesBuilder, err := NewNoDedupTracesBuilder(
		tracesRecordBuilder,
		tracesarrow.NewConfig(conf), // Keep sorting enabled
		stats,
		conf.Observer,
	)
	if err != nil {
		panic(err)
	}

	return &NoDedupTracesProducer{
		pool:                conf.Pool,
		zstd:                conf.Zstd,
		streamProducers:     make(map[string]*nodedupTracesStreamProducer),
		batchId:             0,
		tracesBuilder:       tracesBuilder,
		tracesRecordBuilder: tracesRecordBuilder,
		stats:               stats,
	}
}

// BatchArrowRecordsFromTraces produces a BatchArrowRecords message from traces.
func (p *NoDedupTracesProducer) BatchArrowRecordsFromTraces(traces ptrace.Traces) (*colarspb.BatchArrowRecords, error) {
	// Build main record
	record, err := p.buildTracesRecord(traces)
	if err != nil {
		return nil, err
	}

	// Build related records
	rms, err := p.tracesBuilder.RelatedData().BuildRecordMessages()
	if err != nil {
		return nil, err
	}

	schemaID := p.tracesRecordBuilder.SchemaID()

	// Main record first
	rms = append([]*record_message.RecordMessage{record_message.NewTraceMessage(schemaID, record)}, rms...)

	bar, err := p.produce(rms)
	if err != nil {
		return nil, err
	}
	p.stats.TracesBatchesProduced++
	return bar, nil
}

func (p *NoDedupTracesProducer) buildTracesRecord(traces ptrace.Traces) (arrow.Record, error) {
	schemaNotUpToDateCount := 0

	for {
		p.tracesBuilder.RelatedData().Reset()

		if err := p.tracesBuilder.Append(traces); err != nil {
			return nil, err
		}

		record, err := p.tracesBuilder.Build()
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

func (p *NoDedupTracesProducer) produce(rms []*record_message.RecordMessage) (*colarspb.BatchArrowRecords, error) {
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
				sp = &nodedupTracesStreamProducer{
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
func (p *NoDedupTracesProducer) Close() error {
	p.tracesBuilder.Release()
	p.tracesRecordBuilder.Release()

	for _, sp := range p.streamProducers {
		if err := sp.ipcWriter.Close(); err != nil {
			return err
		}
	}
	return nil
}
