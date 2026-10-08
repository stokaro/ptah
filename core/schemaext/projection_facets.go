package schemaext

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/objectidentity"
)

// FacetProjection predicts one complete attached model after accepted changes.
// Subject names the common owner; Kind names its model. Nil Value establishes
// absence. A present value uses Observed; typed nil and mismatched kinds fail.
type FacetProjection struct {
	Subject objectidentity.ID
	Kind    Kind
	Value   Value
}

// ProjectFacets applies predictions to known prior state without changing the
// source. Unlisted values, subjects, target bindings, and knowledge limits
// survive. Replacement retains the affected value's binding; removal drops it.
// Excluded values cannot be projected. Predictions are planning inputs, never
// inspection evidence. Errors and cancellation return no partial state.
func (r Registry) ProjectFacets(ctx context.Context, current FacetState, projections []FacetProjection) (FacetState, error) {
	state, err := r.SnapshotFacetState(ctx, Observed, current)
	if err != nil {
		return FacetState{}, err
	}
	seen := make(map[subjectKey]bool)
	claims := state.Coverage.SubjectRecords()
	for _, projection := range projections {
		if err := ctx.Err(); err != nil {
			return FacetState{}, err
		}
		key := subjectKey{kind: projection.Kind, ref: projection.Subject.Key()}
		if seen[key] || projection.Subject.Kind == "" || projection.Subject.Name.Source == "" || projection.Subject.Name.Normalized == "" || !projection.Kind.Valid() {
			return FacetState{}, fmt.Errorf("%w: invalid or duplicate facet projection", ErrInvalidValue)
		}
		seen[key] = true
		index := slices.IndexFunc(state.Records, func(record FacetRecord) bool { return record.Subject.Key() == projection.Subject.Key() })
		if index < 0 {
			index = len(state.Records)
			state.Records = append(state.Records, FacetRecord{Subject: projection.Subject})
		}
		facets, err := r.projectFacet(ctx, state.Records[index].Values, state.Coverage, projection)
		if err != nil {
			return FacetState{}, err
		}
		state.Records[index].Values = facets
		knowledge := Knowledge{State: Absent}
		if projection.Value != nil {
			knowledge.State = Complete
		}
		claims = slices.DeleteFunc(claims, func(claim SubjectCoverage) bool {
			return claim.Kind == projection.Kind && claim.Subject.Key() == projection.Subject.Key()
		})
		claims = append(claims, SubjectCoverage{Kind: projection.Kind, Subject: projection.Subject, Knowledge: knowledge})
	}
	if len(projections) > 0 {
		state.Coverage, err = NewCoverage(Observed, state.Coverage.KindRecords(), claims)
		if err != nil {
			return FacetState{}, err
		}
	}
	slices.SortFunc(state.Records, func(a, b FacetRecord) int { return CompareRefs(a.Subject, b.Subject) })
	if err := ctx.Err(); err != nil {
		return FacetState{}, err
	}
	return state, nil
}

func (r Registry) projectFacet(ctx context.Context, facets Facets, coverage Coverage, projection FacetProjection) (Facets, error) {
	current, found := facets.values[projection.Kind]
	if !found && facets.declares(projection.Kind) {
		return Facets{}, fmt.Errorf("%w: cannot project an excluded facet %q", ErrInvalidValue, projection.Kind)
	}
	if err := knownProjectionValue(coverage, projection.Kind, projection.Subject, current); err != nil {
		return Facets{}, err
	}
	if projection.Value == nil {
		return facets.Without(projection.Kind), nil
	}
	values, err := r.SnapshotValues(ctx, Observed, []Value{projection.Value})
	if err != nil {
		return Facets{}, err
	}
	if values[0].Kind() != projection.Kind {
		return Facets{}, fmt.Errorf("%w: projected facet value disagrees with its model kind", ErrInvalidValue)
	}
	if found {
		return facets.Replace(values[0])
	}
	return facets.With(values[0])
}
