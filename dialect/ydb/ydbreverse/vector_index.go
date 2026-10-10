package ydbreverse

import (
	"context"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/ydbindex"
)

// VectorIndexService reconstructs the settings a vector index was built with
// from complete operands. Its zero value is ready for concurrent use and never
// reads a database.
type VectorIndexService struct{}

// ReverseChanges preserves input order and returns no partial result on error
// or cancellation. Each reversal asks for the captured prior settings, which
// builds the index again, as the forward change does. ForwardState predicts
// the settings the forward change leaves; it is a planning input, never
// inspection evidence. A forward change whose declaration does not resolve
// has no forward state to predict and is refused, as its plan is.
func (VectorIndexService) ReverseChanges(ctx context.Context, request schemaext.ReversalRequest) ([]schemaext.Reversal, error) {
	return reverseFacet(ctx, request, "YDB vector index", reverseVectorIndex)
}

func reverseVectorIndex(record schemaext.ChangeRecord, change *ydbdiff.VectorIndex) (schemaext.Reversal, error) {
	if err := ydbdiff.ValidateVectorIndex(change); err != nil {
		return schemaext.Reversal{}, err
	}
	reversed := &ydbdiff.VectorIndex{After: change.Before.Desired()}
	result := schemaext.Reversal{
		Change:      schemaext.ChangeRecord{Subject: record.Subject, Value: reversed},
		Strategy:    "drop the index and add it again with its captured settings",
		Limitations: []string{"Building a vector index again reads every row of its table; searches through the index fail until it is built."},
	}
	projected := schemaext.ProjectedValue{Placement: schemaext.FacetPlacement, Kind: ydbschema.VectorIndexKind}
	if change.After != nil {
		settings, err := ydbindex.ResolveVector(new(change.After.Settings()), "")
		if err != nil {
			return schemaext.Reversal{}, err
		}
		reversed.Before = new(ydbschema.ObservedVectorIndex(settings))
		projected.Value = reversed.Before.Clone()
	}
	result.ForwardState = []schemaext.ProjectedValue{projected}
	return result, nil
}
