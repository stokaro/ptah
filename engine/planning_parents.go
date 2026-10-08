package engine

import (
	"fmt"
	"slices"

	"ptah.run/core/featureplan"
	"ptah.run/core/plangraph"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
)

func hasParentActions(tables []featureplan.Table) bool {
	return slices.ContainsFunc(tables, func(table featureplan.Table) bool { return table.Action != "" })
}

// Registered models are assessed even when their namespace is empty or absent
// from source coverage. Concrete unassigned state must not bypass that dispatch.
func (r *Runtime) validateParentPlanningOwnership(request featureplan.Request) error {
	if hasParentActions(request.Tables) && !slices.ContainsFunc(r.planningServices, func(service ownedPlanning) bool {
		return service.Target == request.Target && len(service.ParentKinds) > 0
	}) {
		return fmt.Errorf("%w: no parent planning service for %q", ptaherr.ErrUnsupportedFeature, request.Target)
	}
	for _, table := range request.Tables {
		if table.Action == "" {
			continue
		}
		kinds, err := capturedPlanningKinds(table)
		if err != nil {
			return err
		}
		for _, kind := range kinds {
			if _, found := r.parentPlanning[conversionKey{request.Target, kind}]; !found {
				return fmt.Errorf("%w: no parent planning service for %q/%q on %s", ptaherr.ErrUnsupportedFeature, request.Target, kind, table.Subject)
			}
		}
	}
	return nil
}

func capturedPlanningKinds(table featureplan.Table) ([]schemaext.Kind, error) {
	var kinds []schemaext.Kind
	for _, group := range capturedTableModelGroups(table.Desired, table.Current) {
		objects, err := group.state.Objects.All()
		if err != nil {
			return nil, err
		}
		for _, object := range objects {
			kinds = append(kinds, object.Value.Kind())
		}
		for _, facets := range group.facets {
			kinds = append(kinds, facets.Kinds()...)
		}
		for _, record := range group.state.Coverage.ForParent(table.Subject).SubjectRecords() {
			kinds = append(kinds, record.Kind)
		}
	}
	slices.Sort(kinds)
	return slices.Compact(kinds), nil
}

func validatePlannedParents(request featureplan.Request, parents []featureplan.ParentPlan, steps, covered map[plangraph.StepID]bool) error {
	index := 0
	for _, table := range request.Tables {
		if table.Action == "" {
			continue
		}
		for _, kind := range request.ParentKinds {
			if index >= len(parents) {
				return fmt.Errorf("%w: planning omitted a parent receipt", schemaext.ErrInvalidValue)
			}
			parent := parents[index]
			if parent.Subject != table.Subject || parent.Kind != kind || parent.Action != table.Action || !reversalText(parent.Strategy) {
				return fmt.Errorf("%w: invalid parent planning receipt", schemaext.ErrInvalidValue)
			}
			seen := make(map[plangraph.StepID]bool)
			for _, step := range parent.Steps {
				if !steps[step] || seen[step] {
					return fmt.Errorf("%w: parent names a missing or duplicate planning step", schemaext.ErrInvalidValue)
				}
				seen[step], covered[step] = true, true
			}
			parents[index].Steps = slices.Clone(parent.Steps)
			index++
		}
	}
	if index != len(parents) {
		return fmt.Errorf("%w: planning returned an unrequested parent receipt", schemaext.ErrInvalidValue)
	}
	return nil
}
