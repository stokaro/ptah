package chreverse

import (
	"context"
	"fmt"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chdiff"
	"ptah.run/dialect/clickhouse/chschema"
)

// RowPolicyService reconstructs prior row policies from captured operands. It
// performs no inspection. Its zero value is ready for concurrent use.
type RowPolicyService struct{}

// rowPolicyLimitation is what no reverse plan can undo. A policy restored to
// its captured definition filters reads from then on; rows a user read while
// the forward plan applied stay read.
const rowPolicyLimitation = "Restoring a row policy changes what its users can read from then on; it cannot undo rows they read while the forward plan applied."

// ReverseChanges returns ordered inverse changes and the policy each forward
// change leaves. A created policy is dropped, a dropped one is created again
// from its captured definition, and a changed one is changed back in place.
// Each inverse carries its own access assessment. Errors and cancellation
// return no partial batch, and no input operand is changed.
func (RowPolicyService) ReverseChanges(ctx context.Context, request schemaext.ReversalRequest) ([]schemaext.Reversal, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: row policy reversal requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if request.Target != platform.ClickHouse {
		return nil, fmt.Errorf("%w: ClickHouse row policy reversal on %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	result := make([]schemaext.Reversal, 0, len(request.Changes))
	for _, record := range request.Changes {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		change, ok := record.Value.(*chdiff.RowPolicy)
		if !ok {
			return nil, fmt.Errorf("%w: reversal requires a ClickHouse row policy change, got %T", schemaext.ErrInvalidValue, record.Value)
		}
		if err := chschema.ValidateRowPolicyRef(record.Subject); err != nil {
			return nil, err
		}
		if err := chdiff.ValidateRowPolicy(change); err != nil {
			return nil, err
		}
		reversal, err := reverseRowPolicy(record, change)
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

func reverseRowPolicy(record schemaext.ChangeRecord, change *chdiff.RowPolicy) (schemaext.Reversal, error) {
	// A nil Value states that the forward change leaves no policy.
	projection := schemaext.ProjectedValue{Placement: schemaext.ObjectPlacement, Kind: chschema.RowPolicyKind}
	var before *chschema.ObservedRowPolicy
	if change.After != nil {
		after, err := change.After.Observed()
		if err != nil {
			return schemaext.Reversal{}, err
		}
		before, projection.Value = after, after.Clone()
	}
	inverse := chdiff.NewRowPolicy(before, change.Before.Desired())
	strategy := "change the row policy back in place with ALTER ROW POLICY"
	switch {
	case change.Before == nil:
		strategy = "drop the created row policy"
	case change.After == nil:
		strategy = "create the dropped row policy again from its captured definition"
	}
	return schemaext.Reversal{
		Change:       schemaext.ChangeRecord{Subject: record.Subject, Value: inverse},
		ForwardState: []schemaext.ProjectedValue{projection},
		Strategy:     strategy,
		Limitations:  []string{rowPolicyLimitation},
	}, nil
}
