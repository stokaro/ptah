package ydbreport

import (
	"context"
	"fmt"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbcoordination"
)

// CoordinationService reports standalone nodes from captured configuration.
type CoordinationService struct{}

// CoordinationDefinitions declares the inventory metric for coordination nodes.
func CoordinationDefinitions() []schemaext.ReportDefinition {
	return []schemaext.ReportDefinition{{Kind: ydbcoordination.Kind, DisplayName: "coordination nodes",
		Metrics: []schemaext.MetricDefinition{{Name: "coordination_nodes", Help: "Standalone coordination nodes"}}}}
}

// ReportValues validates representation and counts each named value once.
func (CoordinationService) ReportValues(ctx context.Context, request schemaext.ReportingRequest) ([]schemaext.ValueReport, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: reporting requires a context", schemaext.ErrInvalidValue)
	}
	var codec schemaext.Codec
	for _, candidate := range ydbcoordination.Codecs() {
		if candidate.Representation == request.Representation {
			codec = candidate
		}
	}
	if codec.Prototype == nil {
		return nil, fmt.Errorf("%w: coordination reporting requires desired or observed state", schemaext.ErrInvalidValue)
	}
	result := make([]schemaext.ValueReport, 0, len(request.Values))
	for _, value := range request.Values {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if _, err := codec.Clone(value); err != nil {
			return nil, err
		}
		result = append(result, schemaext.ValueReport{Kind: ydbcoordination.Kind, Counts: []schemaext.MetricCount{{Name: "coordination_nodes", Value: 1}}})
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return result, nil
}
