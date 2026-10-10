package ydbreverse

import (
	"context"
	"fmt"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/ydbpartition"
)

// TablePartitioningService reconstructs a row table's settings from complete
// operands. Its zero value is ready for concurrent use and never reads a
// database.
type TablePartitioningService struct{}

// ReverseChanges preserves input order and returns no partial result on error
// or cancellation. Each reversal restores every setting the table held, in
// place, from every setting the forward change leaves. ForwardState predicts
// what the forward change leaves; it is a planning input, never inspection
// evidence.
//
// The restored declaration names every setting. Swapping the two sides would
// not do: an observation leaves out each setting at YDB's documented default,
// and as a declaration a setting left out keeps what the table holds, so a
// rollback of a minimum raised from 1 would plan nothing. YDB cannot remove a
// maximum partition count, so a rollback keeps one the forward change set on
// a table that had none, and says so.
func (TablePartitioningService) ReverseChanges(ctx context.Context, request schemaext.ReversalRequest) ([]schemaext.Reversal, error) {
	return reverseFacet(ctx, request, "YDB table partitioning", reverseTablePartitioning)
}

func reverseTablePartitioning(record schemaext.ChangeRecord, change *ydbdiff.TablePartitioning) (schemaext.Reversal, error) {
	if err := ydbdiff.ValidateTablePartitioning(change); err != nil {
		return schemaext.Reversal{}, err
	}
	held, after, err := (&ydbast.AlterTablePartitioning{Change: *change}).Resolve()
	if err != nil {
		return schemaext.Reversal{}, fmt.Errorf("%w: %w", schemaext.ErrInvalidValue, err)
	}
	left := &ydbschema.ObservedTablePartitioning{}
	if spec := ydbpartition.TableSpec(after); spec != nil {
		left.TablePartitioning = *spec
	}
	reversed := &ydbdiff.TablePartitioning{After: &ydbschema.DesiredTablePartitioning{TablePartitioning: *held.Explicit()}}
	if !left.IsZero() {
		reversed.Before = left
	}
	result := schemaext.Reversal{
		Change:       schemaext.ChangeRecord{Subject: record.Subject, Value: reversed},
		ForwardState: []schemaext.ProjectedValue{{Placement: schemaext.FacetPlacement, Kind: ydbschema.TablePartitioningKind, Value: left.Clone()}},
		Strategy:     "restore every setting the table held in place",
	}
	if held.MaxPartitions == 0 && after.MaxPartitions != 0 {
		result.Limitations = []string{fmt.Sprintf("YDB cannot remove a maximum partition count, so the rollback keeps "+
			"AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = %d, which the forward change set.", after.MaxPartitions)}
	}
	return result, nil
}
