package schemaext

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/objectidentity"
)

// ObjectProjection is the complete predicted value of one named subject after
// accepted changes. Nil means explicit absence; a typed nil is invalid. Its
// subject and value use the same model kind, and a present value uses Observed.
type ObjectProjection struct {
	Subject objectidentity.ID
	Value   Value
}

// ProjectObjects applies owner-provided predictions to a captured object state.
// Only the named subjects change. Unlisted siblings and all namespace knowledge
// limits survive, including kinds a newer runtime happens to support. The
// source must establish the affected subjects' prior state; projection cannot
// turn unreadable state into known state. The result describes what an accepted
// plan predicts, not a database inspection or evidence that it executed.
// Errors and cancellation return no partial state.
func (r Registry) ProjectObjects(ctx context.Context, current ObjectState, projections []ObjectProjection) (ObjectState, error) {
	state, err := r.SnapshotObjectState(ctx, Observed, current)
	if err != nil {
		return ObjectState{}, err
	}
	projections, err = r.snapshotObjectProjections(ctx, projections)
	if err != nil {
		return ObjectState{}, err
	}
	claims := state.Coverage.SubjectRecords()
	for _, projection := range projections {
		if err := ctx.Err(); err != nil {
			return ObjectState{}, err
		}
		if err := knownProjectionSource(state, projection.Subject); err != nil {
			return ObjectState{}, err
		}
		state.Objects = state.Objects.Without(projection.Subject)
		knowledge := Knowledge{State: Absent}
		if projection.Value != nil {
			state.Objects, err = state.Objects.With(Object{Ref: projection.Subject, Value: projection.Value})
			if err != nil {
				return ObjectState{}, err
			}
			knowledge.State = Complete
		}
		kind := Kind(projection.Subject.Kind)
		claims = slices.DeleteFunc(claims, func(claim SubjectCoverage) bool {
			return claim.Kind == kind && claim.Subject.Key() == projection.Subject.Key()
		})
		claims = append(claims, SubjectCoverage{Kind: kind, Subject: projection.Subject, Knowledge: knowledge})
	}
	if len(projections) > 0 {
		state.Coverage, err = NewCoverage(Observed, state.Coverage.KindRecords(), claims)
		if err != nil {
			return ObjectState{}, err
		}
	}
	if err := ctx.Err(); err != nil {
		return ObjectState{}, err
	}
	return state, nil
}

func (r Registry) snapshotObjectProjections(ctx context.Context, projections []ObjectProjection) ([]ObjectProjection, error) {
	result := make([]ObjectProjection, len(projections))
	seen := make(map[objectidentity.Key]bool)
	for i, projection := range projections {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		ref := projection.Subject
		kind := Kind(ref.Kind)
		if !kind.Valid() || ref.Name.Source == "" || ref.Name.Normalized == "" || seen[ref.Key()] {
			return nil, fmt.Errorf("%w: missing or duplicate projected subject", ErrInvalidValue)
		}
		seen[ref.Key()] = true
		if _, found := r.codecs[codecKey{kind: kind, representation: Observed}]; !found {
			return nil, &UnknownCodecError{Kind: kind, Representation: Observed}
		}
		result[i] = projection
		if projection.Value == nil {
			continue
		}
		values, err := r.SnapshotValues(ctx, Observed, []Value{projection.Value})
		if err != nil {
			return nil, err
		}
		if values[0].Kind() != kind {
			return nil, fmt.Errorf("%w: projected value kind disagrees with its subject", ErrInvalidValue)
		}
		result[i].Value = values[0]
	}
	return result, nil
}

func knownProjectionSource(state ObjectState, ref objectidentity.ID) error {
	kind := Kind(ref.Kind)
	if _, enrolled := state.Coverage.kinds[kind]; !enrolled {
		return fmt.Errorf("%w: cannot project an unenrolled model %q", ErrInvalidValue, kind)
	}
	_, found, err := state.Objects.Get(ref)
	if err != nil {
		return err
	}
	knowledge, explicit := state.Coverage.SubjectKnowledge(kind, ref)
	if found && !explicit {
		// A concrete definition can be known in a partly enumerated namespace.
		return nil
	}
	if !explicit {
		knowledge = state.Coverage.Lookup(kind, ref)
	}
	if knowledge.State != Complete && (found || knowledge.State != Absent) {
		return fmt.Errorf("%w: cannot project unknown prior state for %s: %s", ErrInvalidValue, ref, knowledge.Reason)
	}
	return nil
}
