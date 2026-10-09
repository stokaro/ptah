package ydbreport

import (
	"context"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbworkload"
)

// PoolService reports captured database-wide resource pool configuration.
type PoolService struct{}

// PoolDefinitions declares the workload object's inventory metric.
func PoolDefinitions() []schemaext.ReportDefinition {
	return []schemaext.ReportDefinition{{Kind: ydbworkload.PoolKind, DisplayName: "resource pools",
		Metrics: []schemaext.MetricDefinition{{Name: "resource_pools", Help: "Resource pools"}}}}
}

// ReportValues validates the selected representation and counts each object once.
func (PoolService) ReportValues(ctx context.Context, request schemaext.ReportingRequest) ([]schemaext.ValueReport, error) {
	return reportStandalone(ctx, request, "resource pool", ydbworkload.PoolKind, "resource_pools", ydbworkload.PoolCodecs())
}
