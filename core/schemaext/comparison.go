package schemaext

import (
	"context"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
)

// ObjectState is the named feature state captured by a source. Coverage remains
// independent of the object collection: an empty collection alone proves no
// absence, and an unrepresentable object may have only a coverage record.
type ObjectState struct {
	Objects  Objects
	Coverage Coverage
}

// ParentState records a common object's lifecycle in a feature comparison.
// Its identity uses the target's identifier semantics. Feature comparers do not
// create or remove that object; the common change captures its attached state.
type ParentState struct {
	Subject objectidentity.ID
	Desired bool
	Current bool
}

// ObjectComparisonRequest contains one target's complete batch for the kinds
// assigned to a comparison service. Parents includes common table lifecycles,
// including tables whose child namespace is empty. Inputs are snapshots.
type ObjectComparisonRequest struct {
	Target       string
	Identifiers  identifier.Semantics
	Capabilities capability.Capabilities
	Kinds        []Kind
	Desired      ObjectState
	Current      ObjectState
	Parents      []ParentState
}

// UndecidedChange is a requested state the available evidence cannot safely
// compare. It is a diagnostic, not an executable change or a successful no-op.
type UndecidedChange struct {
	// Kind identifies the model whose state is unknown, including when Subject
	// names its common parent namespace rather than an individual feature object.
	Kind    Kind              `json:"kind"`
	Subject objectidentity.ID `json:"subject"`
	Reason  string            `json:"reason"`
}

// ObjectComparisonResult carries independently named changes and the effective
// desired state used for captures. A comparer may preserve inspected children
// omitted by an incomplete source. It must retain all explicit declarations and
// all remaining knowledge limits. Parent creation/removal owns its child state;
// Changes contains no duplicate child operation for those parent transitions.
// A successful reply sets Complete, including a completed no-op or diagnostics.
type ObjectComparisonResult struct {
	Complete  bool
	Desired   ObjectState
	Changes   []ChangeRecord
	Undecided []UndecidedChange
}

// ObjectComparisonService compares named feature objects as a contextual batch.
// It returns no partial result after an error or cancellation, performs no
// database mutation, and must not mutate the supplied snapshots.
type ObjectComparisonService interface {
	CompareObjects(context.Context, ObjectComparisonRequest) (ObjectComparisonResult, error)
}

// ComparisonRuntime supplies selected comparison and conversion services with
// the local codecs needed to capture their schema and change representations.
type ComparisonRuntime interface {
	TargetResolver
	ConversionRuntime
	ComparisonService
}

// SnapshotObjectState validates source definitions and concrete representations
// before a semantic service runs. An empty collection adds no coverage claims.
func (r Registry) SnapshotObjectState(ctx context.Context, representation Representation, state ObjectState) (ObjectState, error) {
	if _, err := r.EncodeCoverage(ctx, representation, state.Coverage); err != nil {
		return ObjectState{}, err
	}
	objects, err := state.Objects.All()
	if err != nil {
		return ObjectState{}, err
	}
	values := make([]Value, len(objects))
	for i, object := range objects {
		values[i] = object.Value
	}
	values, err = r.SnapshotValues(ctx, representation, values)
	if err != nil {
		return ObjectState{}, err
	}
	for i := range objects {
		objects[i].Value = values[i]
	}
	cloned, err := NewObjects(objects...)
	if err != nil {
		return ObjectState{}, err
	}
	return ObjectState{Objects: cloned, Coverage: state.Coverage}, nil
}
