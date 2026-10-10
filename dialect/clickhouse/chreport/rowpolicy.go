package chreport

import (
	"context"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chschema"
)

// RowPolicyService reports captured row policies without inspecting a
// database. Its zero value supports concurrent use.
type RowPolicyService struct{}

// RowPolicyDefinitions returns the omission label and count metadata for
// captured row policies. Counts do not claim completeness of a database
// inspection.
func RowPolicyDefinitions() []schemaext.ReportDefinition {
	return []schemaext.ReportDefinition{{Kind: chschema.RowPolicyKind, DisplayName: "ClickHouse row policies", Metrics: []schemaext.MetricDefinition{
		{Name: "clickhouse_row_policies", Help: "ClickHouse row policies captured as owned feature objects"},
	}}}
}

// ReportValues returns one ordered count per valid policy in the requested
// representation. Invalid values, nil context, and cancellation return no
// partial batch.
func (RowPolicyService) ReportValues(ctx context.Context, request schemaext.ReportingRequest) ([]schemaext.ValueReport, error) {
	return countValues(ctx, request, "row policy", chschema.RowPolicyKind, "clickhouse_row_policies", validateRowPolicyValue)
}

func validateRowPolicyValue(value schemaext.Value, representation schemaext.Representation) error {
	return validateRepresented(value, representation, "row policy", chschema.ValidateDesiredRowPolicy, chschema.ValidateObservedRowPolicy)
}
