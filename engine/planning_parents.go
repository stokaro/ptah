package engine

import (
	"fmt"
	"slices"

	"ptah.run/core/featureplan"
	"ptah.run/core/plangraph"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
)

// ParentKinds returns, sorted and once each, the model kinds the parent
// planning services of a target are assigned: the attached models whose state
// a table on that target holds, and which an owner accounts for when the host
// creates, alters, rebuilds or drops the table
// ([featureplan.Request.ParentKinds]). [Runtime.PlanFeatures] refuses such an
// action on a table that holds state of a kind outside the list. An unknown
// target is refused with [ptaherr.ErrUnsupportedDialect]; a nil runtime knows
// no kinds. It implements [featureplan.ParentVocabulary].
func (r *Runtime) ParentKinds(targetName string) ([]schemaext.Kind, error) {
	if r == nil {
		return nil, nil
	}
	target, found := r.lookup(targetName)
	if !found {
		return nil, fmt.Errorf("%w: %q", ptaherr.ErrUnsupportedDialect, targetName)
	}
	// Registration refuses a second parent planning service for a kind on a
	// target, so each kind is listed once.
	var kinds []schemaext.Kind
	for _, service := range r.planningServices {
		if service.Target == target.name {
			kinds = append(kinds, service.ParentKinds...)
		}
	}
	slices.Sort(kinds)
	return kinds, nil
}

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
		kinds, err := table.CapturedKinds()
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
