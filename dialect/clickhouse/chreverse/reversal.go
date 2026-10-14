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

// Service reconstructs the storage clauses of a table from complete operands.
// Its zero value is ready for concurrent use and never reads a database.
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
		if err := chsql.ValidateStorageChange(change); err != nil {
			return nil, err
		}
		declared, err := change.After.Observed()
		if err != nil {
			return nil, err
		}
		after := chsql.Predicted(change.Before, declared)
		result = append(result, schemaext.Reversal{
			Change:       schemaext.ChangeRecord{Subject: record.Subject, Value: &chdiff.Table{Before: after, After: change.Before.Desired()}},
			ForwardState: []schemaext.ProjectedValue{{Placement: schemaext.FacetPlacement, Kind: chschema.TableKind, Value: after.Clone()}},
			Strategy:     "restore the captured storage clauses in place",
			Limitations:  limitations(change.Before, after),
		})
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

// limitations says what restoring each changed clause cannot recover. A
// sorting key that gained expressions has nothing to recover: parts sorted by
// the longer key are sorted by the shorter one too. Restoring the sorting key
// does not decide whether the reverse is possible in place; the reverse plan
// checks it against the columns it adds and removes, like any other.
func limitations(before, after *chschema.ObservedTable) []string {
	var result []string
	for _, clause := range chsql.Changed(before, after) {
		switch clause {
		case chsql.OrderBy:
			if key, _ := chsql.SortingKey(before.OrderBy, after.OrderBy, after.PrimaryKey); key.Dropped > 0 && chsql.MergesByKey(after.Engine) {
				result = append(result, "Restoring the sorting key cannot separate rows the engine merged while the key lacked the dropped expressions.")
			}
		case chsql.TTL:
			result = append(result, "Restoring TTL rules cannot recover rows or values already expired or aggregated, or undo data movement caused by TTL processing.")
		case chsql.Settings:
			result = append(result, "Restoring table settings cannot undo merges, moves, or expirations done under the planned settings.")
		}
	}
	return result
}
