package exporter

import (
	"log"

	"github.com/aws/aws-sdk-go-v2/service/xray/types"
	"github.com/ojkelly/xray-to-otel/exporter/awsxray"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

func parseTrace(trace types.Trace) ([]*tracepb.ResourceSpans, error) {
	rspans := []*tracepb.ResourceSpans{}

	if trace.Id == nil {
		log.Printf("[skip] trace has no Id")
	}
	for _, s := range trace.Segments {
		seg, err := parseSegmentDocument(*s.Document)
		if err != nil || seg == nil {
			log.Printf("unable to parse segment for xray trace %s\n%s", *trace.Id, err)
		}

		rspn, err := segmentToResourceSpan(seg)
		if err != nil {
			log.Printf("unable to parse segment for xray trace %s\n%s", *trace.Id, err)
		} else {
			rspans = append(rspans, rspn...)
		}
	}

	return rspans, nil
}

func segmentToResourceSpan(seg *awsxray.Segment) ([]*tracepb.ResourceSpans, error) {
	rspans := []*tracepb.ResourceSpans{}
	if seg.Origin == nil {
		return nil, nil
	}
	scopeSpans := []*tracepb.ScopeSpans{}

	spns, err := segmentToSpans(seg, nil, nil)
	if err != nil {
		return nil, err
	}

	instName := "AWS::Xray"
	instVersion := "xray-to-otel"

	if seg.AWS != nil && seg.AWS.XRay != nil {
		if seg.AWS.XRay.SDK != nil {
			instName = *seg.AWS.XRay.SDK
		}
		if seg.AWS.XRay.SDKVersion != nil {
			instVersion = *seg.AWS.XRay.SDKVersion
		}
	}
	ss := tracepb.ScopeSpans{
		Spans: spns,
		Scope: &commonpb.InstrumentationScope{
			Name:    instName,
			Version: instVersion,
		},
	}
	scopeSpans = append(scopeSpans, &ss)

	rs := &tracepb.ResourceSpans{
		Resource: &resourcepb.Resource{
			Attributes: []*commonpb.KeyValue{
				{
					Key:   "service.name",
					Value: Value(attribute.StringValue(*seg.Origin)),
				},
			},
		},
		ScopeSpans: scopeSpans,
		SchemaUrl:  "",
	}

	rspans = append(rspans, rs)
	return rspans, nil
}

func segmentToSpans(seg *awsxray.Segment, traceId *trace.TraceID, parentId *trace.SpanID) (spans []*tracepb.Span, err error) {
	if seg == nil {
		return nil, nil
	}

	var tid trace.TraceID
	if traceId == nil {
		xrayTid, err := parseXrayTraceID(*seg.TraceID)

		if err != nil {
			return nil, err
		}

		tid = xrayTid
	} else {
		tid = *traceId
	}

	spanId, err := trace.SpanIDFromHex(*seg.ID)
	if err != nil {
		return nil, err
	}

	var name = "unknown"
	if seg.Name != nil {
		name = *seg.Name
	}
	if seg.StartTime == nil || seg.EndTime == nil {
		log.Println("skip span missing start/end time")
		return nil, nil
	}
	startTime := uint64(parseXrayTimestamp(*seg.StartTime).UnixNano())
	endTime := uint64(parseXrayTimestamp(*seg.EndTime).UnixNano())

	s := &tracepb.Span{
		TraceId:                tid[:],
		SpanId:                 spanId[:],
		Status:                 getStatusFromXraySegment(*seg),
		StartTimeUnixNano:      startTime,
		EndTimeUnixNano:        endTime,
		Kind:                   tracepb.Span_SPAN_KIND_INTERNAL,
		Name:                   name,
		Attributes:             KeyValues(getAttributesFromXraySegment(*seg)),
		Events:                 getEventsFromXraySegment(*seg),
		DroppedAttributesCount: 0,
		DroppedEventsCount:     0,
		DroppedLinksCount:      0,
	}

	if seg.ParentID != nil {
		pid, err := trace.SpanIDFromHex(*seg.ParentID)

		if err != nil {
			return nil, err
		}

		s.ParentSpanId = pid[:]
	} else if parentId != nil {
		s.ParentSpanId = parentId[:]
	}

	if len(seg.Subsegments) >= 1 {
		for _, sub := range seg.Subsegments {
			subspn, err := segmentToSpans(&sub, &tid, &spanId)
			if err != nil {
				return nil, err
			}
			spans = append(spans, subspn...)
		}
	}

	spans = append(spans, s)
	return spans, nil
}
