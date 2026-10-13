package pgtable

import (
	"context"

	"ptah.run/core/schemaext"
)

// DeleteColumnsReports names the feature and counts captured foreign-key column selections.
// Counts describe values, not the completeness of a schema inspection.
func DeleteColumnsReports() []schemaext.ReportDefinition {
	return []schemaext.ReportDefinition{{Kind: DeleteColumnsKind, DisplayName: "PostgreSQL foreign-key ON DELETE column lists",
		Metrics: []schemaext.MetricDefinition{{Name: "postgres_foreign_key_delete_column_lists", Help: "Foreign keys with captured ON DELETE column selections"}}}}
}

// ReportValues reports an ordered batch without inspecting a server. Invalid
// representations, values, nil contexts and cancellation return no partial batch.
func (DeleteColumnsService) ReportValues(ctx context.Context, request schemaext.ReportingRequest) ([]schemaext.ValueReport, error) {
	return reportTableValues(ctx, request, DeleteColumnsKind, "ON DELETE selection", "postgres_foreign_key_delete_column_lists", deleteColumnsCount)
}

func deleteColumnsCount(representation schemaext.Representation, value schemaext.Value) (int, error) {
	converted, err := convertDeleteColumns(representation, value)
	if err != nil {
		return 0, err
	}
	var selected bool
	switch typed := converted.(type) {
	case *DesiredDeleteColumns:
		selected = len(typed.Columns) > 0
	case *ObservedDeleteColumns:
		selected = len(typed.Columns) > 0
	}
	if selected {
		return 1, nil
	}
	return 0, nil
}
