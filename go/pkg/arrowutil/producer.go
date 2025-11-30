// Package arrowutil provides utilities for working with Apache Arrow IPC format.
package arrowutil

import (
	"bytes"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

// Producer produces Arrow IPC data from records with incremental dictionary deltas.
// Similar to arrow_record.Producer in the OTAP library.
//
// The producer maintains a single IPC writer with WithDictionaryDeltas(true) that
// tracks dictionary state across batches. Each call to Produce() returns the IPC
// bytes for that batch, including any dictionary deltas.
//
// Usage patterns:
//   - Incremental dictionaries: Create one Producer, call Produce() for each batch
//   - Dictionary per file: Create a new Producer for each batch
//   - No dictionary: Use a schema without dictionary types
type Producer struct {
	buf    *bytes.Buffer
	writer *ipc.Writer
}

// NewProducer creates a new Arrow producer with the given schema.
// The producer uses WithDictionaryDeltas(true) to enable incremental dictionaries.
func NewProducer(schema *arrow.Schema) *Producer {
	buf := &bytes.Buffer{}
	writer := ipc.NewWriter(buf, ipc.WithSchema(schema), ipc.WithDictionaryDeltas(true))

	return &Producer{
		buf:    buf,
		writer: writer,
	}
}

// Produce converts an Arrow record to IPC bytes.
// Maintains dictionary state across calls - first batch includes full dictionary,
// subsequent batches include only deltas for new dictionary values.
func (p *Producer) Produce(record arrow.Record) ([]byte, error) {
	if err := p.writer.Write(record); err != nil {
		return nil, err
	}
	// Copy the buffer contents and reset
	data := make([]byte, p.buf.Len())
	copy(data, p.buf.Bytes())
	p.buf.Reset()
	return data, nil
}

// Close closes the producer and releases resources.
func (p *Producer) Close() error {
	return p.writer.Close()
}
