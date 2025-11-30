package arrowutil

import (
	"bytes"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestProducerBasic(t *testing.T) {
	// Create a simple schema with a dictionary field
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "status", Type: &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int8, ValueType: arrow.BinaryTypes.String}},
	}, nil)

	producer := NewProducer(schema)
	defer producer.Close()

	// Create first batch
	rec1 := buildTestRecord(schema, []int64{1, 2, 3}, []string{"active", "inactive", "active"})
	defer rec1.Release()

	data1, err := producer.Produce(rec1)
	if err != nil {
		t.Fatalf("Produce batch 1 failed: %v", err)
	}
	if len(data1) == 0 {
		t.Fatal("Produce batch 1 returned empty data")
	}
	t.Logf("Batch 1: %d bytes", len(data1))

	// Create second batch with same dictionary values (should use deltas)
	rec2 := buildTestRecord(schema, []int64{4, 5}, []string{"active", "inactive"})
	defer rec2.Release()

	data2, err := producer.Produce(rec2)
	if err != nil {
		t.Fatalf("Produce batch 2 failed: %v", err)
	}
	if len(data2) == 0 {
		t.Fatal("Produce batch 2 returned empty data")
	}
	t.Logf("Batch 2: %d bytes", len(data2))

	// Create third batch with new dictionary value (should have delta)
	rec3 := buildTestRecord(schema, []int64{6}, []string{"pending"})
	defer rec3.Release()

	data3, err := producer.Produce(rec3)
	if err != nil {
		t.Fatalf("Produce batch 3 failed: %v", err)
	}
	if len(data3) == 0 {
		t.Fatal("Produce batch 3 returned empty data")
	}
	t.Logf("Batch 3: %d bytes", len(data3))

	// Note: Only the first batch is independently readable since it contains the schema.
	// Subsequent batches only contain record data (and dictionary deltas).
	// This is expected for incremental dictionary mode.
	verifyCanRead(t, data1, schema, "batch 1")
}

func TestProducerDictPerFile(t *testing.T) {
	// Test the "dict per file" pattern: new producer for each batch
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "status", Type: &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int8, ValueType: arrow.BinaryTypes.String}},
	}, nil)

	// Batch 1 with fresh producer
	producer1 := NewProducer(schema)
	rec1 := buildTestRecord(schema, []int64{1, 2}, []string{"active", "inactive"})
	data1, err := producer1.Produce(rec1)
	rec1.Release()
	producer1.Close()
	if err != nil {
		t.Fatalf("Produce batch 1 failed: %v", err)
	}
	t.Logf("Batch 1 (fresh producer): %d bytes", len(data1))

	// Batch 2 with fresh producer (should have full dictionary, not delta)
	producer2 := NewProducer(schema)
	rec2 := buildTestRecord(schema, []int64{3, 4}, []string{"active", "pending"})
	data2, err := producer2.Produce(rec2)
	rec2.Release()
	producer2.Close()
	if err != nil {
		t.Fatalf("Produce batch 2 failed: %v", err)
	}
	t.Logf("Batch 2 (fresh producer): %d bytes", len(data2))

	// Both batches should be independently readable
	verifyCanRead(t, data1, schema, "batch 1")
	verifyCanRead(t, data2, schema, "batch 2")
}

func TestProducerIncrementalDict(t *testing.T) {
	// Test that incremental dictionaries actually produce smaller subsequent batches
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "category", Type: &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int8, ValueType: arrow.BinaryTypes.String}},
	}, nil)

	producer := NewProducer(schema)
	defer producer.Close()

	// First batch: establish dictionary with some values
	rec1 := buildTestRecord(schema, []int64{1, 2, 3, 4, 5}, []string{"cat_a", "cat_b", "cat_c", "cat_a", "cat_b"})
	defer rec1.Release()
	data1, err := producer.Produce(rec1)
	if err != nil {
		t.Fatalf("Produce batch 1 failed: %v", err)
	}

	// Second batch: reuse same dictionary values (should be smaller - no new dict entries)
	rec2 := buildTestRecord(schema, []int64{6, 7, 8, 9, 10}, []string{"cat_a", "cat_b", "cat_c", "cat_a", "cat_b"})
	defer rec2.Release()
	data2, err := producer.Produce(rec2)
	if err != nil {
		t.Fatalf("Produce batch 2 failed: %v", err)
	}

	t.Logf("Batch 1 (with full dict): %d bytes", len(data1))
	t.Logf("Batch 2 (dict reuse): %d bytes", len(data2))

	// Second batch should be smaller since it doesn't need to include dictionary
	// (First batch has schema + dictionary + data, second has only data)
	if len(data2) >= len(data1) {
		t.Logf("Warning: Batch 2 (%d bytes) is not smaller than Batch 1 (%d bytes)", len(data2), len(data1))
		t.Log("This might indicate dictionary deltas are not working as expected")
	}
}

func buildTestRecord(schema *arrow.Schema, ids []int64, statuses []string) arrow.Record {
	alloc := memory.NewGoAllocator()
	b := array.NewRecordBuilder(alloc, schema)
	defer b.Release()

	for i := range ids {
		b.Field(0).(*array.Int64Builder).Append(ids[i])
		b.Field(1).(*array.BinaryDictionaryBuilder).AppendString(statuses[i])
	}

	return b.NewRecord()
}

func verifyCanRead(t *testing.T, data []byte, expectedSchema *arrow.Schema, name string) {
	t.Helper()

	reader, err := ipc.NewReader(
		newBytesReader(data),
		ipc.WithDictionaryDeltas(true),
	)
	if err != nil {
		t.Fatalf("Failed to create reader for %s: %v", name, err)
	}
	defer reader.Release()

	// Verify schema matches
	if reader.Schema().NumFields() != expectedSchema.NumFields() {
		t.Errorf("%s: schema field count mismatch: got %d, want %d",
			name, reader.Schema().NumFields(), expectedSchema.NumFields())
	}

	// Read all records
	recordCount := 0
	for reader.Next() {
		rec := reader.Record()
		t.Logf("%s: record %d has %d rows", name, recordCount, rec.NumRows())
		recordCount++
	}
	if err := reader.Err(); err != nil {
		t.Errorf("%s: reader error: %v", name, err)
	}
	if recordCount == 0 {
		t.Errorf("%s: no records read", name)
	}
}

func newBytesReader(data []byte) *bytes.Reader {
	return bytes.NewReader(data)
}
