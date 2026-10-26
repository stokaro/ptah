package provider

import (
	"fmt"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

// PlanningLifecycleFeature enables operations placed across the complete
// common sequence, subject to resource, explicit and rewrite constraints.
// DEPENDENT retains its point placement. Both peers negotiate this behavior.
const PlanningLifecycleFeature = "planning.lifecycle"

func validatePlanningLifecycleEngine(description *protocol.Description, engine *protocol.Engine) error {
	if slices.Contains(engine.GetFeatures(), PlanningLifecycleFeature) &&
		(!slices.Contains(description.GetFeatures(), PlanningLifecycleFeature) || len(engine.GetPlanning())+len(engine.GetDeclarations()) == 0) {
		return fmt.Errorf("%w: lifecycle placement requires the provider's feature and an engine planner", ErrProtocol)
	}
	return nil
}

func planningLifecycleAvailable(description *protocol.Description, id string) bool {
	if !slices.Contains(description.GetFeatures(), PlanningLifecycleFeature) {
		return false
	}
	for _, engine := range description.GetEngines() {
		if engine.GetId() == id {
			return slices.Contains(engine.GetFeatures(), PlanningLifecycleFeature)
		}
	}
	return false
}
