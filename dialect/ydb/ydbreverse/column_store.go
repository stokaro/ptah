package ydbreverse

import (
	"context"
	"fmt"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbschema"
)

// ColumnStoreService reconstructs a column table's tiered TTL from complete
// operands. Its zero value is ready for concurrent use and never reads a
// database.
type ColumnStoreService struct{}

// ReverseChanges preserves input order and returns no partial result on error
// or cancellation. Each reversal asks for the captured prior TTL in place.
// ForwardState predicts the storage the forward change leaves: the layout the
// table holds, which no planned change moves, with the declared TTL. It is a
// planning input, never inspection evidence. A conversion between row and column
// storage has no reversal, since no plan makes one; it is refused.
func (ColumnStoreService) ReverseChanges(ctx context.Context, request schemaext.ReversalRequest) ([]schemaext.Reversal, error) {
	return reverseFacet(ctx, request, "YDB column storage", reverseColumnStore)
}

func reverseColumnStore(record schemaext.ChangeRecord, change *ydbdiff.ColumnStore) (schemaext.Reversal, error) {
	if err := ydbdiff.ValidateColumnStore(change); err != nil {
		return schemaext.Reversal{}, err
	}
	if change.Before == nil || change.After == nil {
		return schemaext.Reversal{}, fmt.Errorf("%w: a conversion between row and column storage has no reversal", schemaext.ErrInvalidValue)
	}
	after := &ydbschema.ObservedColumnStore{ColumnStore: change.Before.ColumnStore.Clone()}
	after.TTL = change.After.TTL.Clone()
	if err := ydbschema.ValidateObservedColumnStore(after); err != nil {
		return schemaext.Reversal{}, err
	}
	return schemaext.Reversal{
		Change:       schemaext.ChangeRecord{Subject: record.Subject, Value: &ydbdiff.ColumnStore{Before: after, After: change.Before.Desired()}},
		ForwardState: []schemaext.ProjectedValue{{Placement: schemaext.FacetPlacement, Kind: ydbschema.ColumnStoreKind, Value: after.Clone()}},
		Strategy:     "restore the prior tiered TTL in place",
	}, nil
}
