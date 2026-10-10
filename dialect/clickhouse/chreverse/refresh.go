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

// RefreshService reconstructs prior refresh schedules from captured operands.
// It performs no inspection. Its zero value is ready for concurrent use.
type RefreshService struct{}

// ReverseChanges returns ordered inverse schedules and the schedule each
// forward change leaves. A schedule changed to another is changed back in
// place. A schedule gained or lost is undone by replacing the view again,
// which restores its definition and schedule but not the rows it held. Errors
// and cancellation return no partial batch, and no input operand is changed.
func (RefreshService) ReverseChanges(ctx context.Context, request schemaext.ReversalRequest) ([]schemaext.Reversal, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: refresh reversal requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if request.Target != platform.ClickHouse {
		return nil, fmt.Errorf("%w: ClickHouse refresh reversal on %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	result := make([]schemaext.Reversal, 0, len(request.Changes))
	for _, record := range request.Changes {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		change, ok := record.Value.(*chdiff.Refresh)
		if !ok || record.Subject.Kind != objectidentity.KindMatView || record.Subject.Name.Empty() {
			return nil, fmt.Errorf("%w: reversal requires a ClickHouse change on a materialized view", schemaext.ErrInvalidValue)
		}
		if err := chdiff.ValidateRefresh(change); err != nil {
			return nil, err
		}
		reversal, err := reverseRefresh(record.Subject, change)
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

func reverseRefresh(subject objectidentity.ID, change *chdiff.Refresh) (schemaext.Reversal, error) {
	inverse := &chdiff.Refresh{Before: nil, After: change.Before.Desired()}
	// A nil Value states that the forward change leaves no schedule.
	forward := schemaext.ProjectedValue{Placement: schemaext.FacetPlacement, Kind: chschema.RefreshKind}
	if change.After != nil {
		after, err := change.After.Observed()
		if err != nil {
			return schemaext.Reversal{}, err
		}
		inverse.Before = after
		forward.Value = after.Clone()
	}
	reversal := schemaext.Reversal{
		Change:       schemaext.ChangeRecord{Subject: subject, Value: inverse},
		ForwardState: []schemaext.ProjectedValue{forward},
		Strategy:     "change the refresh schedule back in place",
	}
	if change.ReplacesOwner() {
		reversal.Strategy = "replace the materialized view with its captured definition and refresh schedule"
		reversal.Limitations = []string{"Replacing a materialized view restores its definition and refresh schedule but not the rows it held."}
	}
	return reversal, nil
}
