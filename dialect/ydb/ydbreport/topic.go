package ydbreport

import (
	"context"
	"fmt"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbtopic"
)

// TopicService reports standalone topics and their consumers. A changefeed's
// topic is its table's, and its owner reports it.
type TopicService struct{}

// TopicDefinitions declares the inventory metrics for standalone topics.
func TopicDefinitions() []schemaext.ReportDefinition {
	return []schemaext.ReportDefinition{{Kind: ydbtopic.Kind, DisplayName: "topics",
		Metrics: []schemaext.MetricDefinition{
			{Name: "topics", Help: "Standalone topics"},
			{Name: "topic_consumers", Help: "Consumers of standalone topics"},
		}}}
}

// ReportValues validates each value through its codec and counts the topic
// and its consumers.
func (TopicService) ReportValues(ctx context.Context, request schemaext.ReportingRequest) ([]schemaext.ValueReport, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: reporting requires a context", schemaext.ErrInvalidValue)
	}
	var codec schemaext.Codec
	for _, candidate := range ydbtopic.Codecs() {
		if candidate.Representation == request.Representation {
			codec = candidate
		}
	}
	if codec.Prototype == nil {
		return nil, fmt.Errorf("%w: topic reporting requires desired or observed state", schemaext.ErrInvalidValue)
	}
	result := make([]schemaext.ValueReport, 0, len(request.Values))
	for _, value := range request.Values {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		snapshot, err := codec.Clone(value)
		if err != nil {
			return nil, err
		}
		var consumers int
		switch topic := snapshot.(type) {
		case *ydbtopic.Desired:
			consumers = len(topic.Spec.Consumers)
		case *ydbtopic.Observed:
			consumers = len(topic.Spec.Consumers)
		}
		result = append(result, schemaext.ValueReport{Kind: ydbtopic.Kind, Counts: []schemaext.MetricCount{
			{Name: "topics", Value: 1}, {Name: "topic_consumers", Value: consumers},
		}})
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return result, nil
}
