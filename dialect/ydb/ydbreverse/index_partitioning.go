package ydbreverse

import (
	"context"
	"fmt"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/ydbindex"
)

// IndexPartitioningService reconstructs a global index's settings from
// complete operands, as [TablePartitioningService] does a table's. Its zero
// value is ready for concurrent use and never reads a database.
type IndexPartitioningService struct{}

// ReverseChanges preserves input order and returns no partial result on error
// or cancellation; see [TablePartitioningService.ReverseChanges].
func (IndexPartitioningService) ReverseChanges(ctx context.Context, request schemaext.ReversalRequest) ([]schemaext.Reversal, error) {
	return reverseFacet(ctx, request, "YDB index partitioning", reverseIndexPartitioning)
}

func reverseIndexPartitioning(record schemaext.ChangeRecord, change *ydbdiff.IndexPartitioning) (schemaext.Reversal, error) {
	if err := ydbdiff.ValidateIndexPartitioning(change); err != nil {
		return schemaext.Reversal{}, err
	}
	held, after, err := (&ydbast.AlterIndexPartitioning{Index: record.Subject.Name.Source, Change: *change}).Resolve()
	if err != nil {
		return schemaext.Reversal{}, fmt.Errorf("%w: %w", schemaext.ErrInvalidValue, err)
	}
	left := &ydbschema.ObservedIndexPartitioning{}
	if spec := ydbindex.Spec(after); spec != nil {
		left.IndexPartitioning = *spec
	}
	reversed := &ydbdiff.IndexPartitioning{After: &ydbschema.DesiredIndexPartitioning{IndexPartitioning: *ydbindex.Explicit(held)}}
	if !left.IsZero() {
		reversed.Before = left
	}
	result := schemaext.Reversal{
		Change:       schemaext.ChangeRecord{Subject: record.Subject, Value: reversed},
		ForwardState: []schemaext.ProjectedValue{{Placement: schemaext.FacetPlacement, Kind: ydbschema.IndexPartitioningKind, Value: left.Clone()}},
		Strategy:     "restore every setting the index held in place",
	}
	if held.MaxPartitions == 0 && after.MaxPartitions != 0 {
		result.Limitations = []string{fmt.Sprintf("YDB cannot remove a maximum partition count, so the rollback keeps "+
			"AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = %d, which the forward change set.", after.MaxPartitions)}
	}
	return result, nil
}
