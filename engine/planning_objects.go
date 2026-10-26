package engine

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/schemaext"
)

func (r *Runtime) snapshotPlanningObjects(ctx context.Context, target string, objects featureplan.ObjectContext) (featureplan.ObjectContext, error) {
	var err error
	selection, err := r.ResolveTarget(target)
	if err != nil {
		return featureplan.ObjectContext{}, err
	}
	for _, state := range []*schemaext.ObjectState{&objects.Desired, &objects.Current} {
		state.Objects, err = state.Objects.ForTarget(selection)
		if err != nil {
			return featureplan.ObjectContext{}, err
		}
		state.Coverage, err = state.Coverage.ForTarget(selection)
		if err != nil {
			return featureplan.ObjectContext{}, err
		}
	}
	objects.Desired, err = r.codecs.SnapshotObjectState(ctx, schemaext.Desired, objects.Desired)
	if err != nil {
		return featureplan.ObjectContext{}, err
	}
	objects.Current, err = r.codecs.SnapshotObjectState(ctx, schemaext.Observed, objects.Current)
	if err != nil {
		return featureplan.ObjectContext{}, err
	}
	objects.ChangedSubjects = slices.Clone(objects.ChangedSubjects)
	for _, state := range []schemaext.ObjectState{objects.Desired, objects.Current} {
		for _, ref := range state.Objects.Refs() {
			object, _, err := state.Objects.Get(ref)
			if err != nil {
				return featureplan.ObjectContext{}, err
			}
			if requirer, ok := object.Value.(schemaext.Requirer); ok {
				if _, err := requirer.Requires(); err != nil {
					return featureplan.ObjectContext{}, err
				}
			}
			if state.Coverage.Lookup(schemaext.Kind(ref.Kind), ref).State == schemaext.Absent {
				return featureplan.ObjectContext{}, fmt.Errorf("%w: present planning object is marked absent: %s", schemaext.ErrInvalidValue, ref)
			}
		}
	}
	return objects, ctx.Err()
}

func changedObjectSubjects(request featureplan.Request) []objectidentity.ID {
	objects := make(map[objectidentity.Key]objectidentity.ID)
	for _, subject := range request.Objects.AllSubjects() {
		objects[subject.Key()] = subject
	}
	changed := make(map[objectidentity.Key]objectidentity.ID)
	for _, change := range request.Changes {
		if subject, found := objects[change.Subject.Key()]; found {
			changed[subject.Key()] = subject
		}
	}
	result := make([]objectidentity.ID, 0, len(changed))
	for _, subject := range changed {
		result = append(result, subject)
	}
	slices.SortFunc(result, schemaext.CompareRefs)
	return result
}

// Object comparisons and explicit context assignments identify the named model
// vocabulary of this target. Facet namespace coverage stays with table context.
func (r *Runtime) planningObjectsFor(owner, target string, objects featureplan.ObjectContext) featureplan.ObjectContext {
	var kinds []schemaext.Kind
	for key := range r.comparisons {
		if key.target == target && r.ownsCodec(owner, key.kind, schemaext.Desired) && r.ownsCodec(owner, key.kind, schemaext.Observed) {
			kinds = append(kinds, key.kind)
		}
	}
	for key := range r.objectPlanning {
		if key.target == target && r.ownsCodec(owner, key.kind, schemaext.Desired) && !slices.Contains(kinds, key.kind) {
			kinds = append(kinds, key.kind)
		}
	}
	result := featureplan.ObjectContext{Desired: selectObjectState(objects.Desired, kinds), Current: selectObjectState(objects.Current, kinds)}
	for _, subject := range objects.ChangedSubjects {
		if slices.Contains(kinds, schemaext.Kind(subject.Kind)) {
			result.ChangedSubjects = append(result.ChangedSubjects, subject)
		}
	}
	return result
}

func validatePlannedObjects(request featureplan.Request, receipts []featureplan.ObjectPlan, steps, covered map[plangraph.StepID]bool) error {
	subjects := request.Objects.Subjects(request.ContextKinds)
	if len(receipts) != len(subjects) {
		return fmt.Errorf("%w: planning changed the native object receipt count", schemaext.ErrInvalidValue)
	}
	for index, receipt := range receipts {
		if receipt.Subject != subjects[index] || receipt.Kind != schemaext.Kind(subjects[index].Kind) || !reversalText(receipt.Strategy) {
			return fmt.Errorf("%w: planning changed a context object or omitted its strategy", schemaext.ErrInvalidValue)
		}
		seen := make(map[plangraph.StepID]bool)
		for _, step := range receipt.Steps {
			if !steps[step] || seen[step] {
				return fmt.Errorf("%w: context object names a missing or duplicate planning step", schemaext.ErrInvalidValue)
			}
			seen[step], covered[step] = true, true
		}
		receipts[index].Steps = slices.Clone(receipt.Steps)
	}
	return nil
}
