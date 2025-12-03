package otlptracesdict

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
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

// Convert transforms ptrace.Traces to TracesDictBatch
func Convert(traces ptrace.Traces) *TracesDictBatch {
	st := NewStringTable()
	batch := &TracesDictBatch{}

	// Single pass: convert traces, building string table as we go
	batch.ResourceSpans = convertResourceSpans(traces, st)
	batch.StringTable = st.Strings()

	return batch
}

func convertResourceSpans(traces ptrace.Traces, st *StringTable) []*ResourceSpans {
	result := make([]*ResourceSpans, 0, traces.ResourceSpans().Len())

	for i := 0; i < traces.ResourceSpans().Len(); i++ {
		rs := traces.ResourceSpans().At(i)
		result = append(result, &ResourceSpans{
			Resource:   convertResource(rs.Resource(), st),
			ScopeSpans: convertScopeSpans(rs.ScopeSpans(), st),
			SchemaUrl:  rs.SchemaUrl(),
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

func convertScopeSpans(sss ptrace.ScopeSpansSlice, st *StringTable) []*ScopeSpans {
	result := make([]*ScopeSpans, 0, sss.Len())

	for i := 0; i < sss.Len(); i++ {
		ss := sss.At(i)
		result = append(result, &ScopeSpans{
			Scope:     convertScope(ss.Scope(), st),
			Spans:     convertSpans(ss.Spans(), st),
			SchemaUrl: ss.SchemaUrl(),
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

func convertSpans(spans ptrace.SpanSlice, st *StringTable) []*Span {
	result := make([]*Span, 0, spans.Len())

	for i := 0; i < spans.Len(); i++ {
		s := spans.At(i)
		traceID := s.TraceID()
		spanID := s.SpanID()
		parentSpanID := s.ParentSpanID()

		span := &Span{
			TraceId:                traceID[:],
			SpanId:                 spanID[:],
			TraceState:             s.TraceState().AsRaw(),
			ParentSpanId:           parentSpanID[:],
			Flags:                  uint32(s.Flags()),
			Name:                   s.Name(),
			Kind:                   convertSpanKind(s.Kind()),
			StartTimeUnixNano:      uint64(s.StartTimestamp()),
			EndTimeUnixNano:        uint64(s.EndTimestamp()),
			Attributes:             convertAttributes(s.Attributes(), st),
			DroppedAttributesCount: s.DroppedAttributesCount(),
			Events:                 convertEvents(s.Events(), st),
			DroppedEventsCount:     s.DroppedEventsCount(),
			Links:                  convertLinks(s.Links(), st),
			DroppedLinksCount:      s.DroppedLinksCount(),
			Status:                 convertStatus(s.Status()),
		}

		result = append(result, span)
	}

	return result
}

func convertSpanKind(kind ptrace.SpanKind) Span_SpanKind {
	switch kind {
	case ptrace.SpanKindInternal:
		return Span_SPAN_KIND_INTERNAL
	case ptrace.SpanKindServer:
		return Span_SPAN_KIND_SERVER
	case ptrace.SpanKindClient:
		return Span_SPAN_KIND_CLIENT
	case ptrace.SpanKindProducer:
		return Span_SPAN_KIND_PRODUCER
	case ptrace.SpanKindConsumer:
		return Span_SPAN_KIND_CONSUMER
	default:
		return Span_SPAN_KIND_UNSPECIFIED
	}
}

func convertEvents(events ptrace.SpanEventSlice, st *StringTable) []*Span_Event {
	result := make([]*Span_Event, 0, events.Len())

	for i := 0; i < events.Len(); i++ {
		e := events.At(i)
		result = append(result, &Span_Event{
			TimeUnixNano:           uint64(e.Timestamp()),
			Name:                   e.Name(),
			Attributes:             convertAttributes(e.Attributes(), st),
			DroppedAttributesCount: e.DroppedAttributesCount(),
		})
	}

	return result
}

func convertLinks(links ptrace.SpanLinkSlice, st *StringTable) []*Span_Link {
	result := make([]*Span_Link, 0, links.Len())

	for i := 0; i < links.Len(); i++ {
		l := links.At(i)
		traceID := l.TraceID()
		spanID := l.SpanID()

		result = append(result, &Span_Link{
			TraceId:                traceID[:],
			SpanId:                 spanID[:],
			TraceState:             l.TraceState().AsRaw(),
			Attributes:             convertAttributes(l.Attributes(), st),
			DroppedAttributesCount: l.DroppedAttributesCount(),
			Flags:                  uint32(l.Flags()),
		})
	}

	return result
}

func convertStatus(status ptrace.Status) *Status {
	return &Status{
		Message: status.Message(),
		Code:    convertStatusCode(status.Code()),
	}
}

func convertStatusCode(code ptrace.StatusCode) Status_StatusCode {
	switch code {
	case ptrace.StatusCodeOk:
		return Status_STATUS_CODE_OK
	case ptrace.StatusCodeError:
		return Status_STATUS_CODE_ERROR
	default:
		return Status_STATUS_CODE_UNSET
	}
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
