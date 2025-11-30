package main

import (
	"bufio"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/danielthank/15712/go/pkg/arrowutil"
	"github.com/danielthank/15712/go/pkg/tpch"
	"google.golang.org/protobuf/proto"
)

func main() {
	tablesStr := flag.String("tables", "lineitem,orders", "Comma-separated list of tables")
	batchSizeStr := flag.String("batch-size", "1000", "Comma-separated list of batch sizes")
	formatStr := flag.String("format", "proto,arrow", "Comma-separated list of formats: proto, arrow, arrownodict, arrowdictperfile")
	outputDir := flag.String("output", "../data/generated", "Output directory")
	dataDir := flag.String("data-dir", "", "Directory containing TPC-H .tbl files (required)")
	flag.Parse()

	// Validate required flags
	if *dataDir == "" {
		fmt.Fprintf(os.Stderr, "Error: --data-dir is required\n")
		flag.Usage()
		os.Exit(1)
	}

	// Parse tables
	tables := []string{}
	for _, t := range strings.Split(*tablesStr, ",") {
		t = strings.TrimSpace(strings.ToLower(t))
		if t == "lineitem" || t == "orders" {
			tables = append(tables, t)
		} else {
			fmt.Fprintf(os.Stderr, "Warning: unsupported table '%s', skipping\n", t)
		}
	}
	if len(tables) == 0 {
		fmt.Fprintf(os.Stderr, "Error: no valid tables specified\n")
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
		"proto":            true,
		"arrow":            true,
		"arrownodict":      true,
		"arrowdictperfile": true,
	}
	formats := []string{}
	for _, format := range strings.Split(*formatStr, ",") {
		format = strings.TrimSpace(format)
		if !validFormats[format] {
			fmt.Fprintf(os.Stderr, "Error: invalid format '%s'\n", format)
			os.Exit(1)
		}
		formats = append(formats, format)
	}

	tblDir := *dataDir
	fmt.Printf("Using TPC-H data from: %s\n", tblDir)

	// Process each table
	for _, table := range tables {
		fmt.Printf("\nProcessing table: %s\n", table)

		tblFile := filepath.Join(tblDir, table+".tbl")
		if _, err := os.Stat(tblFile); os.IsNotExist(err) {
			fmt.Fprintf(os.Stderr, "Error: %s not found\n", tblFile)
			continue
		}

		// Process each batch size and format
		for _, batchSize := range batchSizes {
			for _, format := range formats {
				fmt.Printf("  batch-size=%d format=%s\n", batchSize, format)

				outDir := filepath.Join(*outputDir, fmt.Sprintf("tpch-%s-%s-%d", table, format, batchSize))
				if err := os.MkdirAll(outDir, 0755); err != nil {
					fmt.Fprintf(os.Stderr, "Error creating directory: %v\n", err)
					continue
				}

				switch table {
				case "lineitem":
					processLineItem(tblFile, outDir, batchSize, format)
				case "orders":
					processOrders(tblFile, outDir, batchSize, format)
				}
			}
		}
	}
}

func processLineItem(tblFile, outDir string, batchSize int, format string) {
	// For Arrow formats with incremental dictionaries (ModeNative), we need to
	// read all rows first and use a single producer that maintains state.
	// For other formats, we can process batch by batch.
	if format == "arrow" || format == "arrownodict" || format == "arrowdictperfile" {
		processLineItemArrow(tblFile, outDir, batchSize, format)
		return
	}

	// Proto format: process batch by batch (streaming)
	f, err := os.Open(tblFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening file: %v\n", err)
		return
	}
	defer f.Close()

	reader := csv.NewReader(bufio.NewReader(f))
	reader.Comma = '|'
	reader.FieldsPerRecord = -1 // Variable fields (trailing |)

	var batch []*tpch.LineItem
	batchNum := 0
	rowCount := 0

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error reading CSV: %v\n", err)
			continue
		}

		// Parse LineItem row (16 fields + trailing empty)
		if len(record) < 16 {
			continue
		}

		item := &tpch.LineItem{
			LOrderkey:      parseInt64(record[0]),
			LPartkey:       parseInt64(record[1]),
			LSuppkey:       parseInt64(record[2]),
			LLinenumber:    int32(parseInt64(record[3])),
			LQuantity:      parseFloat64(record[4]),
			LExtendedprice: parseFloat64(record[5]),
			LDiscount:      parseFloat64(record[6]),
			LTax:           parseFloat64(record[7]),
			LReturnflag:    record[8],
			LLinestatus:    record[9],
			LShipdate:      record[10],
			LCommitdate:    record[11],
			LReceiptdate:   record[12],
			LShipinstruct:  record[13],
			LShipmode:      record[14],
			LComment:       record[15],
		}

		batch = append(batch, item)
		rowCount++

		if len(batch) >= batchSize {
			writeLineItemBatch(batch, outDir, batchNum, format)
			batch = nil
			batchNum++
		}
	}

	// Write remaining batch
	if len(batch) > 0 {
		writeLineItemBatch(batch, outDir, batchNum, format)
		batchNum++
	}

	fmt.Printf("    Wrote %d batches (%d rows)\n", batchNum, rowCount)
}

// processLineItemArrow processes LineItem using the ArrowProducer.
// - arrow: One producer, reused across batches (incremental dictionary deltas)
// - arrowdictperfile: New producer per batch (fresh dictionary each batch)
// - arrownodict: New producer per batch with non-dictionary schema
func processLineItemArrow(tblFile, outDir string, batchSize int, format string) {
	// Read all rows first (needed for "arrow" format to maintain dictionary state)
	allItems, err := readAllLineItems(tblFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading file: %v\n", err)
		return
	}

	if len(allItems) == 0 {
		fmt.Fprintf(os.Stderr, "No rows found in %s\n", tblFile)
		return
	}

	useDictionary := format != "arrownodict"
	schema := getLineItemSchema(useDictionary)

	// For "arrow" format, create one producer and reuse (incremental dictionaries)
	var producer *arrowutil.Producer
	if format == "arrow" {
		producer = arrowutil.NewProducer(schema)
		defer producer.Close()
	}

	batchNum := 0
	for start := 0; start < len(allItems); start += batchSize {
		end := start + batchSize
		if end > len(allItems) {
			end = len(allItems)
		}
		batchItems := allItems[start:end]

		// For dictperfile/nodict, create fresh producer each batch
		if format != "arrow" {
			producer = arrowutil.NewProducer(schema)
		}

		// Build Arrow record
		rec := buildLineItemRecord(batchItems, schema, useDictionary)

		// Produce IPC bytes
		data, err := producer.Produce(rec)
		rec.Release()
		if err != nil {
			if format != "arrow" {
				producer.Close()
			}
			fmt.Fprintf(os.Stderr, "Error producing Arrow batch: %v\n", err)
			continue
		}

		// Close per-batch producer
		if format != "arrow" {
			producer.Close()
		}

		// Write to file
		outputFile := filepath.Join(outDir, fmt.Sprintf("payload_%04d.bin", batchNum))
		if err := os.WriteFile(outputFile, data, 0644); err != nil {
			fmt.Fprintf(os.Stderr, "Error writing file: %v\n", err)
		}
		batchNum++
	}

	modeStr := "incremental dictionaries"
	if !useDictionary {
		modeStr = "no dictionary"
	} else if format == "arrowdictperfile" {
		modeStr = "dictionary per file"
	}
	fmt.Printf("    Wrote %d batches (%d rows) with %s\n", batchNum, len(allItems), modeStr)
}

// readAllLineItems reads all LineItem rows from a TPC-H .tbl file.
func readAllLineItems(tblFile string) ([]*tpch.LineItem, error) {
	f, err := os.Open(tblFile)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	reader := csv.NewReader(bufio.NewReader(f))
	reader.Comma = '|'
	reader.FieldsPerRecord = -1

	var items []*tpch.LineItem
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue
		}
		if len(record) < 16 {
			continue
		}

		items = append(items, &tpch.LineItem{
			LOrderkey:      parseInt64(record[0]),
			LPartkey:       parseInt64(record[1]),
			LSuppkey:       parseInt64(record[2]),
			LLinenumber:    int32(parseInt64(record[3])),
			LQuantity:      parseFloat64(record[4]),
			LExtendedprice: parseFloat64(record[5]),
			LDiscount:      parseFloat64(record[6]),
			LTax:           parseFloat64(record[7]),
			LReturnflag:    record[8],
			LLinestatus:    record[9],
			LShipdate:      record[10],
			LCommitdate:    record[11],
			LReceiptdate:   record[12],
			LShipinstruct:  record[13],
			LShipmode:      record[14],
			LComment:       record[15],
		})
	}
	return items, nil
}

func processOrders(tblFile, outDir string, batchSize int, format string) {
	// For Arrow formats, use the ArrowProducer
	if format == "arrow" || format == "arrownodict" || format == "arrowdictperfile" {
		processOrdersArrow(tblFile, outDir, batchSize, format)
		return
	}

	// Proto format: process batch by batch (streaming)
	f, err := os.Open(tblFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening file: %v\n", err)
		return
	}
	defer f.Close()

	reader := csv.NewReader(bufio.NewReader(f))
	reader.Comma = '|'
	reader.FieldsPerRecord = -1

	var batch []*tpch.Orders
	batchNum := 0
	rowCount := 0

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error reading CSV: %v\n", err)
			continue
		}

		// Parse Orders row (9 fields + trailing empty)
		if len(record) < 9 {
			continue
		}

		order := &tpch.Orders{
			OOrderkey:      parseInt64(record[0]),
			OCustkey:       parseInt64(record[1]),
			OOrderstatus:   record[2],
			OTotalprice:    parseFloat64(record[3]),
			OOrderdate:     record[4],
			OOrderpriority: record[5],
			OClerk:         record[6],
			OShippriority:  int32(parseInt64(record[7])),
			OComment:       record[8],
		}

		batch = append(batch, order)
		rowCount++

		if len(batch) >= batchSize {
			writeOrdersBatch(batch, outDir, batchNum, format)
			batch = nil
			batchNum++
		}
	}

	// Write remaining batch
	if len(batch) > 0 {
		writeOrdersBatch(batch, outDir, batchNum, format)
		batchNum++
	}

	fmt.Printf("    Wrote %d batches (%d rows)\n", batchNum, rowCount)
}

// processOrdersArrow processes Orders using the ArrowProducer.
// - arrow: One producer, reused across batches (incremental dictionary deltas)
// - arrowdictperfile: New producer per batch (fresh dictionary each batch)
// - arrownodict: New producer per batch with non-dictionary schema
func processOrdersArrow(tblFile, outDir string, batchSize int, format string) {
	// Read all rows first
	allOrders, err := readAllOrders(tblFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading file: %v\n", err)
		return
	}

	if len(allOrders) == 0 {
		fmt.Fprintf(os.Stderr, "No rows found in %s\n", tblFile)
		return
	}

	useDictionary := format != "arrownodict"
	schema := getOrdersSchema(useDictionary)

	// For "arrow" format, create one producer and reuse (incremental dictionaries)
	var producer *arrowutil.Producer
	if format == "arrow" {
		producer = arrowutil.NewProducer(schema)
		defer producer.Close()
	}

	batchNum := 0
	for start := 0; start < len(allOrders); start += batchSize {
		end := start + batchSize
		if end > len(allOrders) {
			end = len(allOrders)
		}
		batchOrders := allOrders[start:end]

		// For dictperfile/nodict, create fresh producer each batch
		if format != "arrow" {
			producer = arrowutil.NewProducer(schema)
		}

		// Build Arrow record
		rec := buildOrdersRecord(batchOrders, schema, useDictionary)

		// Produce IPC bytes
		data, err := producer.Produce(rec)
		rec.Release()
		if err != nil {
			if format != "arrow" {
				producer.Close()
			}
			fmt.Fprintf(os.Stderr, "Error producing Arrow batch: %v\n", err)
			continue
		}

		// Close per-batch producer
		if format != "arrow" {
			producer.Close()
		}

		// Write to file
		outputFile := filepath.Join(outDir, fmt.Sprintf("payload_%04d.bin", batchNum))
		if err := os.WriteFile(outputFile, data, 0644); err != nil {
			fmt.Fprintf(os.Stderr, "Error writing file: %v\n", err)
		}
		batchNum++
	}

	modeStr := "incremental dictionaries"
	if !useDictionary {
		modeStr = "no dictionary"
	} else if format == "arrowdictperfile" {
		modeStr = "dictionary per file"
	}
	fmt.Printf("    Wrote %d batches (%d rows) with %s\n", batchNum, len(allOrders), modeStr)
}

// readAllOrders reads all Orders rows from a TPC-H .tbl file.
func readAllOrders(tblFile string) ([]*tpch.Orders, error) {
	f, err := os.Open(tblFile)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	reader := csv.NewReader(bufio.NewReader(f))
	reader.Comma = '|'
	reader.FieldsPerRecord = -1

	var orders []*tpch.Orders
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue
		}
		if len(record) < 9 {
			continue
		}

		orders = append(orders, &tpch.Orders{
			OOrderkey:      parseInt64(record[0]),
			OCustkey:       parseInt64(record[1]),
			OOrderstatus:   record[2],
			OTotalprice:    parseFloat64(record[3]),
			OOrderdate:     record[4],
			OOrderpriority: record[5],
			OClerk:         record[6],
			OShippriority:  int32(parseInt64(record[7])),
			OComment:       record[8],
		})
	}
	return orders, nil
}

// writeLineItemBatch writes a proto batch for LineItem (only used for proto format)
func writeLineItemBatch(items []*tpch.LineItem, outDir string, batchNum int, format string) {
	outputFile := filepath.Join(outDir, fmt.Sprintf("payload_%04d.bin", batchNum))

	batch := &tpch.TpchBatch{
		Batch: &tpch.TpchBatch_Lineitem{
			Lineitem: &tpch.LineItemBatch{Items: items},
		},
	}
	data, err := proto.Marshal(batch)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error marshaling proto: %v\n", err)
		return
	}
	if err := os.WriteFile(outputFile, data, 0644); err != nil {
		fmt.Fprintf(os.Stderr, "Error writing file: %v\n", err)
	}
}

// writeOrdersBatch writes a proto batch for Orders (only used for proto format)
func writeOrdersBatch(orders []*tpch.Orders, outDir string, batchNum int, format string) {
	outputFile := filepath.Join(outDir, fmt.Sprintf("payload_%04d.bin", batchNum))

	batch := &tpch.TpchBatch{
		Batch: &tpch.TpchBatch_Orders{
			Orders: &tpch.OrdersBatch{Orders: orders},
		},
	}
	data, err := proto.Marshal(batch)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error marshaling proto: %v\n", err)
		return
	}
	if err := os.WriteFile(outputFile, data, 0644); err != nil {
		fmt.Fprintf(os.Stderr, "Error writing file: %v\n", err)
	}
}

func getLineItemSchema(useDictionary bool) *arrow.Schema {
	if useDictionary {
		return arrow.NewSchema([]arrow.Field{
			{Name: "l_orderkey", Type: arrow.PrimitiveTypes.Int64},
			{Name: "l_partkey", Type: arrow.PrimitiveTypes.Int64},
			{Name: "l_suppkey", Type: arrow.PrimitiveTypes.Int64},
			{Name: "l_linenumber", Type: arrow.PrimitiveTypes.Int32},
			{Name: "l_quantity", Type: arrow.PrimitiveTypes.Float64},
			{Name: "l_extendedprice", Type: arrow.PrimitiveTypes.Float64},
			{Name: "l_discount", Type: arrow.PrimitiveTypes.Float64},
			{Name: "l_tax", Type: arrow.PrimitiveTypes.Float64},
			{Name: "l_returnflag", Type: &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int8, ValueType: arrow.BinaryTypes.String}},
			{Name: "l_linestatus", Type: &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int8, ValueType: arrow.BinaryTypes.String}},
			{Name: "l_shipdate", Type: arrow.BinaryTypes.String},
			{Name: "l_commitdate", Type: arrow.BinaryTypes.String},
			{Name: "l_receiptdate", Type: arrow.BinaryTypes.String},
			{Name: "l_shipinstruct", Type: &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int8, ValueType: arrow.BinaryTypes.String}},
			{Name: "l_shipmode", Type: &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int8, ValueType: arrow.BinaryTypes.String}},
			{Name: "l_comment", Type: arrow.BinaryTypes.String},
		}, nil)
	}
	return arrow.NewSchema([]arrow.Field{
		{Name: "l_orderkey", Type: arrow.PrimitiveTypes.Int64},
		{Name: "l_partkey", Type: arrow.PrimitiveTypes.Int64},
		{Name: "l_suppkey", Type: arrow.PrimitiveTypes.Int64},
		{Name: "l_linenumber", Type: arrow.PrimitiveTypes.Int32},
		{Name: "l_quantity", Type: arrow.PrimitiveTypes.Float64},
		{Name: "l_extendedprice", Type: arrow.PrimitiveTypes.Float64},
		{Name: "l_discount", Type: arrow.PrimitiveTypes.Float64},
		{Name: "l_tax", Type: arrow.PrimitiveTypes.Float64},
		{Name: "l_returnflag", Type: arrow.BinaryTypes.String},
		{Name: "l_linestatus", Type: arrow.BinaryTypes.String},
		{Name: "l_shipdate", Type: arrow.BinaryTypes.String},
		{Name: "l_commitdate", Type: arrow.BinaryTypes.String},
		{Name: "l_receiptdate", Type: arrow.BinaryTypes.String},
		{Name: "l_shipinstruct", Type: arrow.BinaryTypes.String},
		{Name: "l_shipmode", Type: arrow.BinaryTypes.String},
		{Name: "l_comment", Type: arrow.BinaryTypes.String},
	}, nil)
}

func buildLineItemRecord(items []*tpch.LineItem, schema *arrow.Schema, useDictionary bool) arrow.Record {
	alloc := memory.NewGoAllocator()
	b := array.NewRecordBuilder(alloc, schema)
	defer b.Release()

	for _, item := range items {
		b.Field(0).(*array.Int64Builder).Append(item.LOrderkey)
		b.Field(1).(*array.Int64Builder).Append(item.LPartkey)
		b.Field(2).(*array.Int64Builder).Append(item.LSuppkey)
		b.Field(3).(*array.Int32Builder).Append(item.LLinenumber)
		b.Field(4).(*array.Float64Builder).Append(item.LQuantity)
		b.Field(5).(*array.Float64Builder).Append(item.LExtendedprice)
		b.Field(6).(*array.Float64Builder).Append(item.LDiscount)
		b.Field(7).(*array.Float64Builder).Append(item.LTax)

		if useDictionary {
			b.Field(8).(*array.BinaryDictionaryBuilder).AppendString(item.LReturnflag)
			b.Field(9).(*array.BinaryDictionaryBuilder).AppendString(item.LLinestatus)
			b.Field(10).(*array.StringBuilder).Append(item.LShipdate)
			b.Field(11).(*array.StringBuilder).Append(item.LCommitdate)
			b.Field(12).(*array.StringBuilder).Append(item.LReceiptdate)
			b.Field(13).(*array.BinaryDictionaryBuilder).AppendString(item.LShipinstruct)
			b.Field(14).(*array.BinaryDictionaryBuilder).AppendString(item.LShipmode)
			b.Field(15).(*array.StringBuilder).Append(item.LComment)
		} else {
			b.Field(8).(*array.StringBuilder).Append(item.LReturnflag)
			b.Field(9).(*array.StringBuilder).Append(item.LLinestatus)
			b.Field(10).(*array.StringBuilder).Append(item.LShipdate)
			b.Field(11).(*array.StringBuilder).Append(item.LCommitdate)
			b.Field(12).(*array.StringBuilder).Append(item.LReceiptdate)
			b.Field(13).(*array.StringBuilder).Append(item.LShipinstruct)
			b.Field(14).(*array.StringBuilder).Append(item.LShipmode)
			b.Field(15).(*array.StringBuilder).Append(item.LComment)
		}
	}

	return b.NewRecord()
}

func getOrdersSchema(useDictionary bool) *arrow.Schema {
	if useDictionary {
		return arrow.NewSchema([]arrow.Field{
			{Name: "o_orderkey", Type: arrow.PrimitiveTypes.Int64},
			{Name: "o_custkey", Type: arrow.PrimitiveTypes.Int64},
			{Name: "o_orderstatus", Type: &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int8, ValueType: arrow.BinaryTypes.String}},
			{Name: "o_totalprice", Type: arrow.PrimitiveTypes.Float64},
			{Name: "o_orderdate", Type: arrow.BinaryTypes.String},
			{Name: "o_orderpriority", Type: &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int8, ValueType: arrow.BinaryTypes.String}},
			{Name: "o_clerk", Type: &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int16, ValueType: arrow.BinaryTypes.String}},
			{Name: "o_shippriority", Type: arrow.PrimitiveTypes.Int32},
			{Name: "o_comment", Type: arrow.BinaryTypes.String},
		}, nil)
	}
	return arrow.NewSchema([]arrow.Field{
		{Name: "o_orderkey", Type: arrow.PrimitiveTypes.Int64},
		{Name: "o_custkey", Type: arrow.PrimitiveTypes.Int64},
		{Name: "o_orderstatus", Type: arrow.BinaryTypes.String},
		{Name: "o_totalprice", Type: arrow.PrimitiveTypes.Float64},
		{Name: "o_orderdate", Type: arrow.BinaryTypes.String},
		{Name: "o_orderpriority", Type: arrow.BinaryTypes.String},
		{Name: "o_clerk", Type: arrow.BinaryTypes.String},
		{Name: "o_shippriority", Type: arrow.PrimitiveTypes.Int32},
		{Name: "o_comment", Type: arrow.BinaryTypes.String},
	}, nil)
}

func buildOrdersRecord(orders []*tpch.Orders, schema *arrow.Schema, useDictionary bool) arrow.Record {
	alloc := memory.NewGoAllocator()
	b := array.NewRecordBuilder(alloc, schema)
	defer b.Release()

	for _, order := range orders {
		b.Field(0).(*array.Int64Builder).Append(order.OOrderkey)
		b.Field(1).(*array.Int64Builder).Append(order.OCustkey)

		if useDictionary {
			b.Field(2).(*array.BinaryDictionaryBuilder).AppendString(order.OOrderstatus)
			b.Field(3).(*array.Float64Builder).Append(order.OTotalprice)
			b.Field(4).(*array.StringBuilder).Append(order.OOrderdate)
			b.Field(5).(*array.BinaryDictionaryBuilder).AppendString(order.OOrderpriority)
			b.Field(6).(*array.BinaryDictionaryBuilder).AppendString(order.OClerk)
			b.Field(7).(*array.Int32Builder).Append(order.OShippriority)
			b.Field(8).(*array.StringBuilder).Append(order.OComment)
		} else {
			b.Field(2).(*array.StringBuilder).Append(order.OOrderstatus)
			b.Field(3).(*array.Float64Builder).Append(order.OTotalprice)
			b.Field(4).(*array.StringBuilder).Append(order.OOrderdate)
			b.Field(5).(*array.StringBuilder).Append(order.OOrderpriority)
			b.Field(6).(*array.StringBuilder).Append(order.OClerk)
			b.Field(7).(*array.Int32Builder).Append(order.OShippriority)
			b.Field(8).(*array.StringBuilder).Append(order.OComment)
		}
	}

	return b.NewRecord()
}

func parseInt64(s string) int64 {
	v, _ := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	return v
}

func parseFloat64(s string) float64 {
	v, _ := strconv.ParseFloat(strings.TrimSpace(s), 64)
	return v
}
