package ydbreport

import (
	"context"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbstreaming"
)

// StreamingService reports standalone queries from captured configuration.
type StreamingService struct{}

// StreamingDefinitions declares the inventory metric for streaming queries.
func StreamingDefinitions() []schemaext.ReportDefinition {
	return []schemaext.ReportDefinition{{Kind: ydbstreaming.Kind, DisplayName: "streaming queries",
		Metrics: []schemaext.MetricDefinition{{Name: "streaming_queries", Help: "Standalone streaming queries"}}}}
}

// ReportValues validates representation and counts each named value once.
func (StreamingService) ReportValues(ctx context.Context, request schemaext.ReportingRequest) ([]schemaext.ValueReport, error) {
	return reportStandalone(ctx, request, "streaming", ydbstreaming.Kind, "streaming_queries", ydbstreaming.Codecs())
}
