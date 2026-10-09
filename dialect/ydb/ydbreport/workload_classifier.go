package ydbreport

import (
	"context"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbworkload"
)

// ClassifierService reports captured database-wide resource pool classifier configuration.
type ClassifierService struct{}

// ClassifierDefinitions declares the workload object's inventory metric.
func ClassifierDefinitions() []schemaext.ReportDefinition {
	return []schemaext.ReportDefinition{{Kind: ydbworkload.ClassifierKind, DisplayName: "resource pool classifiers",
		Metrics: []schemaext.MetricDefinition{{Name: "resource_pool_classifiers", Help: "Resource pool classifiers"}}}}
}

// ReportValues validates the selected representation and counts each object once.
func (ClassifierService) ReportValues(ctx context.Context, request schemaext.ReportingRequest) ([]schemaext.ValueReport, error) {
	return reportStandalone(ctx, request, "resource pool classifier", ydbworkload.ClassifierKind, "resource_pool_classifiers", ydbworkload.ClassifierCodecs())
}
