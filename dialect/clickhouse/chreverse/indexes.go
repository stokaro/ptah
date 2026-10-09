package chreverse

import (
	"context"
	"fmt"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chdiff"
	"ptah.run/dialect/clickhouse/chschema"
)

// IndexService reconstructs prior skipping-index settings from captured
// operands. It performs no inspection and does not recover materialized index
// data. Its zero value is ready for concurrent use.
type IndexService struct{}

// ReverseChanges returns ordered inverse settings and predicted forward
// settings. Both directions require replacement of the index definition; the
// common parent snapshot supplies its key expression. Errors and cancellation
// return no partial batch, and neither input operand is changed.
func (IndexService) ReverseChanges(ctx context.Context, request schemaext.ReversalRequest) ([]schemaext.Reversal, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: index reversal requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if request.Target != platform.ClickHouse {
		return nil, fmt.Errorf("%w: ClickHouse index reversal on %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	result := make([]schemaext.Reversal, 0, len(request.Changes))
	for _, record := range request.Changes {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		change, ok := record.Value.(*chdiff.Index)
		if !ok || record.Subject.Kind != objectidentity.KindIndex || record.Subject.Name.Empty() || record.Subject.Parent.Empty() {
			return nil, fmt.Errorf("%w: reversal requires a ClickHouse change on a table-owned index", schemaext.ErrInvalidValue)
		}
		if err := chdiff.ValidateIndex(change); err != nil {
			return nil, err
		}
		after, err := change.After.Observed()
		if err != nil {
			return nil, err
		}
		result = append(result, schemaext.Reversal{
			Change:       schemaext.ChangeRecord{Subject: record.Subject, Value: &chdiff.Index{Before: after, After: change.Before.Desired()}},
			ForwardState: []schemaext.ProjectedValue{{Placement: schemaext.FacetPlacement, Kind: chschema.IndexKind, Value: after.Clone()}},
			Strategy:     "replace the index with its captured definition",
			Limitations:  []string{"Replacing a skipping index restores its definition but does not restore materialized index data for existing table parts."},
		})
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return result, nil
}
