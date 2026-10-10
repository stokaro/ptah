package ydbreverse

import (
	"context"
	"fmt"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbschema"
)

// TTLService reconstructs a TTL from complete operands. Its zero value is
// ready for concurrent use and never reads a database.
type TTLService struct{}

// ReverseChanges preserves input order and returns no partial result on error
// or cancellation. Each reversal restores the captured prior policy, or its
// absence, in place. ForwardState predicts the policy the forward change
// leaves; it is a planning input, never inspection evidence.
func (TTLService) ReverseChanges(ctx context.Context, request schemaext.ReversalRequest) ([]schemaext.Reversal, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: reversal requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if request.Target != platform.YDB {
		return nil, fmt.Errorf("%w: YDB TTL reversal on %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	result := make([]schemaext.Reversal, 0, len(request.Changes))
	for _, record := range request.Changes {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		change, ok := record.Value.(*ydbdiff.TTL)
		if !ok {
			return nil, fmt.Errorf("%w: reversal requires a YDB TTL change", schemaext.ErrInvalidValue)
		}
		reversal, err := reverseTTL(record, change)
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

func reverseTTL(record schemaext.ChangeRecord, change *ydbdiff.TTL) (schemaext.Reversal, error) {
	if err := ydbast.ValidateTTLChange(change); err != nil {
		return schemaext.Reversal{}, err
	}
	reversed := &ydbdiff.TTL{After: change.Before.Desired()}
	forward := schemaext.ProjectedValue{Placement: schemaext.FacetPlacement, Kind: ydbschema.TTLKind}
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
		Strategy:     "restore the captured TTL in place",
	}
	if change.Before == nil {
		result.Strategy = "remove the TTL the forward change added"
	}
	if change.After != nil {
		result.Limitations = []string{"Restoring the prior TTL cannot recover rows YDB deleted under the forward TTL."}
	}
	return result, nil
}
