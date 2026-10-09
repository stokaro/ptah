package chreport

import (
	"context"
	"fmt"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chschema"
)

// IndexService reports captured skipping-index settings without inspecting a
// database. Its zero value supports concurrent use.
type IndexService struct{}

// IndexDefinitions returns omission labels and count metadata for captured
// index settings. Counts do not claim completeness of a database inspection.
func IndexDefinitions() []schemaext.ReportDefinition {
	return []schemaext.ReportDefinition{{Kind: chschema.IndexKind, DisplayName: "ClickHouse index settings", Metrics: []schemaext.MetricDefinition{
		{Name: "clickhouse_index_settings", Help: "Indexes with captured ClickHouse data-skipping settings"},
	}}}
}

// ReportValues returns one ordered count per valid index value in the requested
// representation. Invalid values, nil context, and cancellation return no
// partial batch. Reporting preserves unresolved desired intent.
func (IndexService) ReportValues(ctx context.Context, request schemaext.ReportingRequest) ([]schemaext.ValueReport, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: index reporting requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if request.Representation != schemaext.Desired && request.Representation != schemaext.Observed {
		return nil, fmt.Errorf("%w: index reporting requires a schema representation", schemaext.ErrInvalidValue)
	}
	reports := make([]schemaext.ValueReport, 0, len(request.Values))
	for _, value := range request.Values {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := validateIndexValue(value, request.Representation); err != nil {
			return nil, err
		}
		reports = append(reports, schemaext.ValueReport{Kind: chschema.IndexKind, Counts: []schemaext.MetricCount{{Name: "clickhouse_index_settings", Value: 1}}})
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return reports, nil
}

func validateIndexValue(value schemaext.Value, representation schemaext.Representation) error {
	switch value := value.(type) {
	case *chschema.DesiredIndex:
		if representation == schemaext.Desired {
			return chschema.ValidateDesiredIndex(value)
		}
	case *chschema.ObservedIndex:
		if representation == schemaext.Observed {
			return chschema.ValidateObservedIndex(value)
		}
	}
	return fmt.Errorf("%w: ClickHouse index report has mismatched value %T for %q", schemaext.ErrInvalidValue, value, representation)
}
