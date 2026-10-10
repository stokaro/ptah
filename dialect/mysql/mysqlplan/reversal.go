package mysqlplan

import (
	"context"
	"fmt"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/mysql/mysqldiff"
	"ptah.run/dialect/mysql/mysqlschema"
)

// ReversalService reverses the changes [IndexBlockSizeService] plans: the
// rollback replaces the index again with the hint it held. Its zero value is
// ready for concurrent use.
type ReversalService struct{}

// ReverseChanges returns the inverse of each change, in order, with the hint
// the forward change leaves as its predicted state. It performs no
// inspection. Errors and cancellation return no partial batch, and no input
// operand is changed.
func (ReversalService) ReverseChanges(ctx context.Context, request schemaext.ReversalRequest) ([]schemaext.Reversal, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: reversal requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if request.Target != platform.MySQL && request.Target != platform.MariaDB {
		return nil, fmt.Errorf("%w: MySQL index block size reversal on %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	result := make([]schemaext.Reversal, 0, len(request.Changes))
	for _, record := range request.Changes {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		change, ok := record.Value.(*mysqldiff.IndexBlockSize)
		if !ok || record.Subject.Kind != objectidentity.KindIndex || record.Subject.Name.Empty() || record.Subject.Parent.Empty() {
			return nil, fmt.Errorf("%w: reversal requires a MySQL block size change on a table's index", schemaext.ErrInvalidValue)
		}
		inverse, err := mysqldiff.IndexBlockSizeReversal(change)
		if err != nil {
			return nil, err
		}
		result = append(result, schemaext.Reversal{
			Change:       schemaext.ChangeRecord{Subject: record.Subject, Value: inverse},
			ForwardState: []schemaext.ProjectedValue{{Placement: schemaext.FacetPlacement, Kind: mysqlschema.IndexBlockSizeKind, Value: inverse.Before.Clone()}},
			Strategy:     "replace the index with the block size it held",
		})
	}
	return result, ctx.Err()
}
