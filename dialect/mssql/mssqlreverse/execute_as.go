package mssqlreverse

import (
	"context"
	"fmt"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/mssql/mssqldiff"
	"ptah.run/dialect/mssql/mssqlschema"
)

// ExecuteAsService reverses a change of the principal a routine runs as: the
// rollback replaces the routine again, with the principal it ran as. Its zero
// value is ready for concurrent use.
type ExecuteAsService struct{}

// ReverseChanges returns the inverse of each change, in order, with the
// principal the forward change leaves as its predicted state. It performs no
// inspection. Errors and cancellation return no partial batch, and no input
// operand is changed.
func (ExecuteAsService) ReverseChanges(ctx context.Context, request schemaext.ReversalRequest) ([]schemaext.Reversal, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: reversal requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if platform.NormalizeDialect(request.Target) != platform.SQLServer {
		return nil, fmt.Errorf("%w: SQL Server EXECUTE AS reversal on %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	result := make([]schemaext.Reversal, 0, len(request.Changes))
	for _, record := range request.Changes {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		change, ok := record.Value.(*mssqldiff.ExecuteAs)
		if !ok || !mssqlschema.HoldsExecuteAs(record.Subject) {
			return nil, fmt.Errorf("%w: reversal requires an EXECUTE AS change on a routine", schemaext.ErrInvalidValue)
		}
		inverse, err := mssqldiff.ExecuteAsReversal(change)
		if err != nil {
			return nil, err
		}
		reversal := schemaext.Reversal{
			Change:   schemaext.ChangeRecord{Subject: record.Subject, Value: inverse},
			Strategy: "replace the routine with the principal it ran as",
		}
		if inverse.Before != nil {
			reversal.ForwardState = []schemaext.ProjectedValue{{Placement: schemaext.FacetPlacement, Kind: mssqlschema.ExecuteAsKind, Value: inverse.Before.Clone()}}
		}
		result = append(result, reversal)
	}
	return result, ctx.Err()
}
