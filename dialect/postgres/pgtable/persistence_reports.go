package pgtable

import (
	"context"
	"fmt"

	"ptah.run/core/schemaext"
)

// PersistenceReports names the feature and counts captured unlogged tables.
// Counts describe values, not the completeness of a schema inspection.
func PersistenceReports() []schemaext.ReportDefinition {
	return []schemaext.ReportDefinition{{Kind: PersistenceKind, DisplayName: "PostgreSQL table persistence settings",
		Metrics: []schemaext.MetricDefinition{{Name: "postgres_unlogged_tables", Help: "Tables with captured PostgreSQL UNLOGGED persistence"}}}}
}

// ReportValues reports an ordered batch without inspecting a server. Invalid
// representations, values, nil contexts and cancellation return no partial batch.
func (PersistenceService) ReportValues(ctx context.Context, request schemaext.ReportingRequest) ([]schemaext.ValueReport, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: persistence reporting requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if request.Representation != schemaext.Desired && request.Representation != schemaext.Observed {
		return nil, fmt.Errorf("%w: persistence reporting requires a schema representation", schemaext.ErrInvalidValue)
	}
	reports := make([]schemaext.ValueReport, 0, len(request.Values))
	for _, value := range request.Values {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		count, err := persistenceCount(request.Representation, value)
		if err != nil {
			return nil, err
		}
		reports = append(reports, schemaext.ValueReport{Kind: PersistenceKind, Counts: []schemaext.MetricCount{{Name: "postgres_unlogged_tables", Value: count}}})
	}
	return completedPersistence(ctx, reports)
}

func persistenceCount(representation schemaext.Representation, value schemaext.Value) (int, error) {
	converted, err := convertPersistence(representation, value)
	if err != nil {
		return 0, err
	}
	var unlogged bool
	switch typed := converted.(type) {
	case *DesiredPersistence:
		unlogged = typed.Unlogged
	case *ObservedPersistence:
		unlogged = typed.Unlogged
	}
	if unlogged {
		return 1, nil
	}
	return 0, nil
}
