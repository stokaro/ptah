// Package tsreport describes captured TimescaleDB values for inventory and
// export-loss reports. It reads captured values only and never a server.
package tsreport

import (
	"context"
	"fmt"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/timescaledb/tsschema"
)

// Service counts captured hypertables and continuous aggregates. Its zero
// value is ready for concurrent use.
type Service struct{}

// Definitions returns independent display labels and count metadata for both
// models. The labels are the family names export-loss reports print.
func Definitions() []schemaext.ReportDefinition {
	return []schemaext.ReportDefinition{
		{Kind: tsschema.HypertableKind, DisplayName: "hypertables", Metrics: []schemaext.MetricDefinition{
			{Name: "hypertables", Help: "Tables partitioned as TimescaleDB hypertables"},
		}},
		{Kind: tsschema.ContinuousAggregateKind, DisplayName: "continuous aggregates", Metrics: []schemaext.MetricDefinition{
			{Name: "continuous_aggregates", Help: "TimescaleDB continuous aggregates"},
		}},
	}
}

// ReportValues validates each value in its representation and returns one
// ordered report per value. Errors and cancellation expose no partial report.
func (Service) ReportValues(ctx context.Context, request schemaext.ReportingRequest) ([]schemaext.ValueReport, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: reporting requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	reports := make([]schemaext.ValueReport, 0, len(request.Values))
	for _, value := range request.Values {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		metric, err := validate(value, request.Representation)
		if err != nil {
			return nil, err
		}
		reports = append(reports, schemaext.ValueReport{Kind: value.Kind(), Counts: []schemaext.MetricCount{{Name: metric, Value: 1}}})
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return reports, nil
}

func validate(value schemaext.Value, representation schemaext.Representation) (string, error) {
	switch typed := value.(type) {
	case *tsschema.DesiredHypertable:
		if representation == schemaext.Desired {
			return "hypertables", tsschema.ValidateDesiredHypertable(typed)
		}
	case *tsschema.ObservedHypertable:
		if representation == schemaext.Observed {
			return "hypertables", tsschema.ValidateObservedHypertable(typed)
		}
	case *tsschema.DesiredContinuousAggregate:
		if representation == schemaext.Desired {
			return "continuous_aggregates", tsschema.ValidateDesiredContinuousAggregate(typed)
		}
	case *tsschema.ObservedContinuousAggregate:
		if representation == schemaext.Observed {
			return "continuous_aggregates", tsschema.ValidateObservedContinuousAggregate(typed)
		}
	}
	return "", fmt.Errorf("%w: TimescaleDB report has mismatched value %T for %q", schemaext.ErrInvalidValue, value, representation)
}
