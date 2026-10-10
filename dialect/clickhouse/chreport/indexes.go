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
	return countValues(ctx, request, "index", chschema.IndexKind, "clickhouse_index_settings", validateIndexValue)
}

// countValues returns one ordered count of metric per value validate accepts
// in the requested representation. Invalid values, nil context, and
// cancellation return no partial batch; feature names the values in errors.
func countValues(ctx context.Context, request schemaext.ReportingRequest, feature string, kind schemaext.Kind, metric string,
	validate func(schemaext.Value, schemaext.Representation) error,
) ([]schemaext.ValueReport, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: %s reporting requires a context", schemaext.ErrInvalidValue, feature)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if request.Representation != schemaext.Desired && request.Representation != schemaext.Observed {
		return nil, fmt.Errorf("%w: %s reporting requires a schema representation", schemaext.ErrInvalidValue, feature)
	}
	reports := make([]schemaext.ValueReport, 0, len(request.Values))
	for _, value := range request.Values {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := validate(value, request.Representation); err != nil {
			return nil, err
		}
		reports = append(reports, schemaext.ValueReport{Kind: kind, Counts: []schemaext.MetricCount{{Name: metric, Value: 1}}})
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return reports, nil
}

func validateIndexValue(value schemaext.Value, representation schemaext.Representation) error {
	return validateRepresented(value, representation, "index", chschema.ValidateDesiredIndex, chschema.ValidateObservedIndex)
}

// validateRepresented accepts a desired value only in the desired
// representation and an observed value only in the observed one, each through
// the model's own validation; feature names the values in errors.
func validateRepresented[D, O schemaext.Value](value schemaext.Value, representation schemaext.Representation, feature string,
	desired func(D) error, observed func(O) error,
) error {
	if typed, ok := value.(D); ok && representation == schemaext.Desired {
		return desired(typed)
	}
	if typed, ok := value.(O); ok && representation == schemaext.Observed {
		return observed(typed)
	}
	return fmt.Errorf("%w: ClickHouse %s report has mismatched value %T for %q", schemaext.ErrInvalidValue, feature, value, representation)
}
