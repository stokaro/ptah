package generator

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/difftypes"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/internal/convert/features"
)

// A desired document cannot predict observation-only state. The reverse target
// comes from conversion of the capture; its current state uses only accepted
// owner projections, retaining unmodified siblings and knowledge limits.
func reverseObjectContext(ctx context.Context, captured difftypes.FeatureContext, recovery []schemaext.Reversal, target string, runtime Runtime) (difftypes.FeatureContext, error) {
	desired, coverage, err := features.Convert(ctx, runtime, target, schemaext.Observed, schemaext.Desired, captured.CurrentObjects, captured.CurrentCoverage, nil)
	if err != nil {
		return difftypes.FeatureContext{}, err
	}
	inventory := featureplan.ObjectContext{
		Desired: schemaext.ObjectState{Objects: captured.DesiredObjects, Coverage: captured.DesiredCoverage},
		Current: schemaext.ObjectState{Objects: captured.CurrentObjects, Coverage: captured.CurrentCoverage},
	}
	capturedSubjects := make(map[objectidentity.Key]bool)
	for _, subject := range inventory.AllSubjects() {
		capturedSubjects[subject.Key()] = true
	}
	var projections []schemaext.ObjectProjection
	var unknown []objectidentity.ID
	for _, reversal := range recovery {
		if !capturedSubjects[reversal.Change.Subject.Key()] {
			continue
		}
		count := 0
		for _, value := range reversal.ForwardState {
			if value.Placement != schemaext.ObjectPlacement {
				continue
			}
			if value.Kind != schemaext.Kind(reversal.Change.Subject.Kind) {
				return difftypes.FeatureContext{}, fmt.Errorf("%w: native context projection changes its subject kind", schemaext.ErrInvalidValue)
			}
			count++
			projections = append(projections, schemaext.ObjectProjection{Subject: reversal.Change.Subject, Value: value.Value})
		}
		if count > 1 {
			return difftypes.FeatureContext{}, fmt.Errorf("%w: changed native object has duplicate forward projections", schemaext.ErrInvalidValue)
		}
		if count == 0 {
			unknown = append(unknown, reversal.Change.Subject)
		}
	}
	current, err := runtime.Codecs().ProjectObjects(ctx, schemaext.ObjectState{Objects: captured.CurrentObjects, Coverage: captured.CurrentCoverage}, projections)
	if err != nil {
		return difftypes.FeatureContext{}, err
	}
	current, err = unknownObjectProjections(current, unknown, runtime.Codecs())
	if err != nil {
		return difftypes.FeatureContext{}, err
	}
	return difftypes.FeatureContext{DesiredObjects: desired, DesiredCoverage: coverage, CurrentObjects: current.Objects, CurrentCoverage: current.Coverage}, nil
}

// A reversal can restore a definition without predicting every observation.
// An omitted prediction remains explicitly unknown; it is never prior state,
// desired state cast as an observation, or a claim that the object is absent.
func unknownObjectProjections(state schemaext.ObjectState, subjects []objectidentity.ID, codecs schemaext.Registry) (schemaext.ObjectState, error) {
	if len(subjects) == 0 {
		return state, nil
	}
	kinds, claims := state.Coverage.KindRecords(), state.Coverage.SubjectRecords()
	for _, subject := range subjects {
		kind := schemaext.Kind(subject.Kind)
		if !slices.ContainsFunc(kinds, func(record schemaext.KindCoverage) bool { return record.Model.Kind == kind }) {
			model, found := codecs.Identity(kind, schemaext.Observed)
			if !found {
				return schemaext.ObjectState{}, fmt.Errorf("%w: projected object has no observation codec", schemaext.ErrInvalidValue)
			}
			kinds = append(kinds, schemaext.KindCoverage{Model: model, Knowledge: schemaext.GlobalKnowledge(schemaext.Knowledge{State: schemaext.Uninspected, Reason: "the captured source did not enumerate this model"})})
		}
		state.Objects = state.Objects.Without(subject)
		claims = slices.DeleteFunc(claims, func(record schemaext.SubjectCoverage) bool {
			return record.Kind == kind && record.Subject.Key() == subject.Key()
		})
		claims = append(claims, schemaext.SubjectCoverage{Kind: kind, Subject: subject, Knowledge: schemaext.Knowledge{State: schemaext.Uninspected, Reason: "the change owner did not predict the complete forward observation"}})
	}
	coverage, err := schemaext.NewCoverage(schemaext.Observed, kinds, claims)
	if err != nil {
		return schemaext.ObjectState{}, err
	}
	state.Coverage = coverage
	return state, nil
}
