package ydbreport

import (
	"context"

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
	return reportStandalone(ctx, request, "coordination", ydbcoordination.Kind, "coordination_nodes", ydbcoordination.Codecs())
}
