package chreport

import (
	"context"
	"fmt"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chschema"
)

// RefreshService reports captured refresh schedules without inspecting a
// database. Its zero value supports concurrent use.
type RefreshService struct{}

// RefreshDefinitions returns omission labels and count metadata for captured
// refresh schedules. Counts do not claim completeness of a database inspection.
func RefreshDefinitions() []schemaext.ReportDefinition {
	return []schemaext.ReportDefinition{{Kind: chschema.RefreshKind, DisplayName: "ClickHouse refresh schedule", Metrics: []schemaext.MetricDefinition{
		{Name: "clickhouse_refreshable_materialized_views", Help: "Materialized views with a captured ClickHouse refresh schedule"},
	}}}
}

// ReportValues returns one ordered count per valid schedule in the requested
// representation. Invalid values, nil context, and cancellation return no
// partial batch.
func (RefreshService) ReportValues(ctx context.Context, request schemaext.ReportingRequest) ([]schemaext.ValueReport, error) {
	return countValues(ctx, request, "refresh", chschema.RefreshKind, "clickhouse_refreshable_materialized_views", validateRefreshValue)
}

func validateRefreshValue(value schemaext.Value, representation schemaext.Representation) error {
	switch value := value.(type) {
	case *chschema.DesiredRefresh:
		if representation == schemaext.Desired {
			return chschema.ValidateDesiredRefresh(value)
		}
	case *chschema.ObservedRefresh:
		if representation == schemaext.Observed {
			return chschema.ValidateObservedRefresh(value)
		}
	}
	return fmt.Errorf("%w: ClickHouse refresh report has mismatched value %T for %q", schemaext.ErrInvalidValue, value, representation)
}
