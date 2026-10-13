package pgtable

import (
	"context"
	"fmt"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
)

// ReverseChanges reconstructs inverse changes from captured operands and
// predicts their forward state. It performs no inspection. Missing operands,
// unsupported targets and cancellation discard the whole batch.
func (ExclusionService) ReverseChanges(ctx context.Context, request schemaext.ReversalRequest) ([]schemaext.Reversal, error) {
	if err := exclusionContext(ctx, request.Target); err != nil {
		return nil, err
	}
	result := make([]schemaext.Reversal, 0, len(request.Changes))
	for _, record := range request.Changes {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		reverse, err := reverseExclusion(record)
		if err != nil {
			return nil, err
		}
		result = append(result, reverse)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func exclusionContext(ctx context.Context, target string) error {
	if ctx == nil {
		return fmt.Errorf("%w: EXCLUDE requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !platform.IsPostgresFamily(target) {
		return fmt.Errorf("%w: EXCLUDE on %q", ptaherr.ErrUnsupportedDialect, target)
	}
	return nil
}

func reverseExclusion(record schemaext.ChangeRecord) (schemaext.Reversal, error) {
	change, ok := record.Value.(*ExclusionChange)
	if !ok {
		return schemaext.Reversal{}, exclusionModelError(schemaext.Change, "EXCLUDE reversal requires an exclusion change")
	}
	if err := ValidateExclusionChange(change); err != nil {
		return schemaext.Reversal{}, err
	}
	if err := ValidateExclusionRef(record.Subject); err != nil {
		return schemaext.Reversal{}, err
	}
	inverse := &ExclusionChange{CommentOnly: change.CommentOnly}
	projected := schemaext.ProjectedValue{Placement: schemaext.ObjectPlacement, Kind: ExclusionKind}
	if change.After != nil {
		inverse.Before = observeExclusion(change.After)
		projected.Value = inverse.Before.Clone()
	}
	if change.Before != nil {
		inverse.After = declareExclusion(change.Before)
	}
	if err := ValidateExclusionChange(inverse); err != nil {
		return schemaext.Reversal{}, err
	}
	strategy := "replace the exclusion constraint with its captured definition"
	var limitations []string
	switch {
	case change.CommentOnly:
		strategy = "restore the exclusion constraint comment"
	case change.Before == nil:
		strategy = "drop the created exclusion constraint"
	case change.After == nil:
		strategy = "recreate the dropped exclusion constraint"
	}
	if change.Before != nil && !change.CommentOnly {
		limitations = []string{"Rows written under the changed constraints can prevent restoration; the plan does not remove those rows."}
	}
	return schemaext.Reversal{Change: schemaext.ChangeRecord{Subject: record.Subject, Value: inverse}, ForwardState: []schemaext.ProjectedValue{projected}, Strategy: strategy, Limitations: limitations}, nil
}
