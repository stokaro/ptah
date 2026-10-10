// Package spannerreverse reverses captured Spanner row deletion policy changes and
// reports the data that restoring a policy cannot recover.
package spannerreverse

import (
	"context"
	"fmt"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/spanner/spannerast"
	"ptah.run/dialect/spanner/spannerdiff"
	"ptah.run/dialect/spanner/spannerschema"
)

// Service reconstructs the row deletion policy from complete operands. Its zero value is
// ready for concurrent use and never reads a database.
type Service struct{}

// ReverseChanges preserves input order and returns no partial result on error
// or cancellation. Each reversal restores the captured prior policy, or its
// absence, in place. ForwardState predicts the policy the forward change
// leaves; it is a planning input, never inspection evidence.
func (Service) ReverseChanges(ctx context.Context, request schemaext.ReversalRequest) ([]schemaext.Reversal, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: reversal requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if request.Target != platform.Spanner {
		return nil, fmt.Errorf("%w: Spanner reversal on %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	result := make([]schemaext.Reversal, 0, len(request.Changes))
	for _, record := range request.Changes {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		change, ok := record.Value.(*spannerdiff.RowDeletion)
		if !ok {
			return nil, fmt.Errorf("%w: reversal requires a Spanner row deletion policy change", schemaext.ErrInvalidValue)
		}
		reversal, err := reverse(record, change)
		if err != nil {
			return nil, err
		}
		result = append(result, reversal)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func reverse(record schemaext.ChangeRecord, change *spannerdiff.RowDeletion) (schemaext.Reversal, error) {
	if err := spannerast.ValidateChange(change); err != nil {
		return schemaext.Reversal{}, err
	}
	reversed := &spannerdiff.RowDeletion{After: change.Before.Desired()}
	forward := schemaext.ProjectedValue{Placement: schemaext.FacetPlacement, Kind: spannerschema.RowDeletionKind}
	if change.After != nil {
		after, err := change.After.Observed()
		if err != nil {
			return schemaext.Reversal{}, err
		}
		reversed.Before, forward.Value = after, after.Clone()
	}
	result := schemaext.Reversal{
		Change:       schemaext.ChangeRecord{Subject: record.Subject, Value: reversed},
		ForwardState: []schemaext.ProjectedValue{forward},
		Strategy:     "restore the captured row deletion policy in place",
	}
	if change.Before == nil {
		result.Strategy = "remove the row deletion policy the forward change added"
	}
	if change.After != nil {
		result.Limitations = []string{"Restoring the prior row deletion policy cannot recover rows the server deleted under the forward policy."}
	}
	return result, nil
}
