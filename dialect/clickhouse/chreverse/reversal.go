// Package chreverse reverses captured ClickHouse storage changes and reports
// data that restoring the definition cannot recover.
package chreverse

import (
	"context"
	"fmt"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chdiff"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/dialect/clickhouse/internal/chsql"
)

// Service reconstructs TTL definitions from complete operands. Its zero value
// is ready for concurrent use and never reads a database.
type Service struct{}

// ReverseChanges preserves input order and returns no partial result on error
// or cancellation. ForwardState is a prediction for planning, never inspection
// evidence. Unsupported storage transitions wrap ErrUnsupportedFeature.
func (Service) ReverseChanges(ctx context.Context, request schemaext.ReversalRequest) ([]schemaext.Reversal, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: reversal requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if request.Target != platform.ClickHouse {
		return nil, fmt.Errorf("%w: ClickHouse reversal on %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	result := make([]schemaext.Reversal, 0, len(request.Changes))
	for _, record := range request.Changes {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		change, ok := record.Value.(*chdiff.Table)
		if !ok {
			return nil, fmt.Errorf("%w: reversal requires a ClickHouse table change", schemaext.ErrInvalidValue)
		}
		if err := chsql.ValidateTTLChange(change); err != nil {
			return nil, err
		}
		after, err := change.After.Observed()
		if err != nil {
			return nil, err
		}
		result = append(result, schemaext.Reversal{
			Change:       schemaext.ChangeRecord{Subject: record.Subject, Value: &chdiff.Table{Before: after, After: change.Before.Desired()}},
			ForwardState: []schemaext.ProjectedValue{{Placement: schemaext.FacetPlacement, Kind: chschema.TableKind, Value: after.Clone()}},
			Strategy:     "restore the captured TTL rules in place",
			Limitations:  []string{"Restoring TTL rules cannot recover rows or values already expired or aggregated, or undo data movement caused by TTL processing."},
		})
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return result, nil
}
