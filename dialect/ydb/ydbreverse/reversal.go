// Package ydbreverse reconstructs YDB feature changes and reports recovery limits.
package ydbreverse

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/ydbchangefeed"
)

// Service reverses captured changefeed definitions without reading a database.
// It does not recover messages or consumer positions. Recreating a prior disabled
// stream is refused because YDB cannot recreate its disabled state.
type Service struct{}

// ReverseChanges returns one reverse change and recovery assessment per input.
// Input observations are reconstructed as declarations, and forward declarations
// become predictions for the reverse operand, never new inspection evidence.
func (Service) ReverseChanges(ctx context.Context, request schemaext.ReversalRequest) ([]schemaext.Reversal, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: reversal requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if request.Target != "ydb" {
		return nil, fmt.Errorf("%w: YDB reversal on %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	if len(request.Changes) > 0 && !request.Capabilities.Has(capability.Changefeeds) {
		return nil, fmt.Errorf("%w: reversing changefeeds requires %s", ptaherr.ErrUnsupportedFeature, capability.Changefeeds)
	}
	result := make([]schemaext.Reversal, 0, len(request.Changes))
	for _, record := range request.Changes {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		reversed, err := reverseChangefeed(record, request.Capabilities)
		if err != nil {
			return nil, err
		}
		result = append(result, reversed)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func reverseChangefeed(record schemaext.ChangeRecord, caps capability.Capabilities) (schemaext.Reversal, error) {
	cloned, err := record.Clone()
	if err != nil {
		return schemaext.Reversal{}, err
	}
	change, ok := cloned.Value.(*ydbdiff.Changefeed)
	if !ok || (change.Before == nil && change.After == nil) {
		return schemaext.Reversal{}, fmt.Errorf("%w: reversal requires captured changefeed operands", schemaext.ErrInvalidValue)
	}
	if change.ReplicationManaged() {
		return schemaext.Reversal{}, fmt.Errorf("%w: a replication-managed changefeed cannot be reversed independently of its controller", schemaext.ErrIrreversible)
	}
	before, after := changefeedOperands(change)
	for _, spec := range []*ydbschema.ChangefeedSpec{before, after} {
		if spec != nil {
			if err := validateOperand(record.Subject, *spec); err != nil {
				return schemaext.Reversal{}, err
			}
		}
	}
	if before != nil {
		if err := validateTransition(record.Subject, *before, after, caps); err != nil {
			return schemaext.Reversal{}, fmt.Errorf("%w: cannot restore prior stream: %w", schemaext.ErrIrreversible, err)
		}
	}
	if after != nil {
		if err := validateTransition(record.Subject, *after, before, caps); err != nil {
			return schemaext.Reversal{}, err
		}
	}
	if change.Before != nil && change.After != nil && ydbchangefeed.Equal(change.After.Spec, change.Before.Spec) {
		return schemaext.Reversal{}, fmt.Errorf("%w: changefeed operands contain no change", schemaext.ErrInvalidValue)
	}
	reverse := reversedChangefeed(change)
	strategy, limitations := recovery(change)
	return schemaext.Reversal{
		Change:       schemaext.ChangeRecord{Subject: record.Subject, Value: reverse},
		ForwardState: []schemaext.ProjectedValue{projectedChangefeed(reverse.Before)}, Strategy: strategy, Limitations: limitations,
	}, nil
}

func reversedChangefeed(change *ydbdiff.Changefeed) *ydbdiff.Changefeed {
	reverse := &ydbdiff.Changefeed{}
	if change.Before != nil {
		reverse.After = change.Before.Desired()
	}
	if change.After != nil {
		reverse.Before = change.After.Observed()
		// An omitted starting partition count keeps the existing stream's count
		// on an in-place topic change. A recreated stream's count remains unknown
		// when its declaration did not specify one; it is not copied from history.
		if change.Before != nil && !ydbchangefeed.Recreated(change.After.Spec, change.Before.Spec) && reverse.Before.Spec.TopicMinActivePartitions == 0 {
			reverse.Before.Spec.TopicMinActivePartitions = change.Before.Spec.TopicMinActivePartitions
		}
	}
	return reverse
}

func projectedChangefeed(after *ydbschema.ObservedChangefeed) schemaext.ProjectedValue {
	projected := schemaext.ProjectedValue{Placement: schemaext.ObjectPlacement, Kind: ydbschema.ChangefeedKind}
	if after != nil {
		projected.Value = after.Clone()
	}
	return projected
}

func changefeedOperands(change *ydbdiff.Changefeed) (before, after *ydbschema.ChangefeedSpec) {
	if change.Before != nil {
		before = &change.Before.Spec
	}
	if change.After != nil {
		after = &change.After.Spec
	}
	return before, after
}

func validateOperand(ref objectidentity.ID, spec ydbschema.ChangefeedSpec) error {
	if ref.Parent.Empty() || ref.Name.Source != spec.Name || ref.Key() != ydbschema.ChangefeedRef(ref.Schema.Source, ref.Parent.Source, spec.Name).Key() {
		return fmt.Errorf("%w: changefeed operand disagrees with its structured subject", schemaext.ErrInvalidValue)
	}
	return ydbschema.ValidateChangefeed(spec)
}

func validateTransition(ref objectidentity.ID, spec ydbschema.ChangefeedSpec, previous *ydbschema.ChangefeedSpec, caps capability.Capabilities) error {
	// A topic alteration retains a disabled stream without trying to disable it.
	if previous != nil && !ydbchangefeed.Recreated(spec, *previous) {
		spec.Disabled = false
	}
	if refusal := ydbchangefeed.Check(ref.Parent.Source, spec, caps); refusal != nil {
		if refusal.Key != "" {
			return fmt.Errorf("%w: %s requires %s", ptaherr.ErrUnsupportedFeature, refusal.Subject, refusal.Key)
		}
		return fmt.Errorf("%w: %s: %s", ptaherr.ErrUnsupportedFeature, refusal.Subject, refusal.Reason)
	}
	return nil
}

func recovery(change *ydbdiff.Changefeed) (string, []string) {
	switch {
	case change.Before == nil:
		return "drop the created changefeed", []string{"Dropping the new stream discards its unread messages and consumer positions."}
	case change.After == nil:
		return "recreate the dropped changefeed", []string{"Recreating the definition cannot recover messages or consumer positions lost when the original stream was dropped."}
	case ydbchangefeed.Recreated(change.After.Spec, change.Before.Spec):
		return "recreate the prior changefeed definition", []string{"Stream recreation cannot recover the original messages or consumer positions and discards the replacement stream's unread messages and positions."}
	default:
		var limitations []string
		if ydbchangefeed.RetentionChanged(change.After.Spec, change.Before.Spec) {
			limitations = append(limitations, "Restoring retention cannot recover messages that have already expired.")
		}
		lost := append(ydbchangefeed.ConsumerPositionsLost(change.After.Spec, change.Before.Spec), ydbchangefeed.ConsumerPositionsLost(change.Before.Spec, change.After.Spec)...)
		slices.Sort(lost)
		lost = slices.Compact(lost)
		if len(lost) > 0 {
			quoted := make([]string, len(lost))
			for i, name := range lost {
				quoted[i] = fmt.Sprintf("%q", name)
			}
			limitations = append(limitations, "Consumer positions lost by dropping or recreating "+strings.Join(quoted, ", ")+" cannot be recovered.")
		}
		return "restore topic settings in place", limitations
	}
}
