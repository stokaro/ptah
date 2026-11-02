package provider

import (
	"fmt"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

// PlanningContextInventoryFeature lets a filtered read carry the observed
// values of the engine's planning context kinds that it leaves out of its
// scope, with their coverage across the database, and lets the host pass them
// to the planning service as context it never compares (ADR 0053). It
// requires PlanningObjectContextFeature.
const PlanningContextInventoryFeature = "planning.context-inventory"

func contextInventoryError(reason string) error {
	return fmt.Errorf("%w: planning context inventory: %s", ErrProtocol, reason)
}

func validatePlanningContextInventoryFeature(description *protocol.Description, engine *protocol.Engine) error {
	if !slices.Contains(engine.GetFeatures(), PlanningContextInventoryFeature) {
		return nil
	}
	if !EngineOffers(description, engine.GetId(), PlanningContextInventoryFeature) || !EngineOffers(description, engine.GetId(), PlanningObjectContextFeature) {
		return contextInventoryError("the engine requires the negotiated object context")
	}
	return nil
}

// engineContextKinds are the kinds the engine's planning services assess as
// context.
func engineContextKinds(engine *protocol.Engine) []string {
	var kinds []string
	for _, service := range engine.GetPlanning() {
		kinds = append(kinds, service.GetContextKinds()...)
	}
	return kinds
}

// validateInspectedPlanningContext checks a snapshot's planning context: it is
// negotiated, carried by a filtered read only, limited to the engine's context
// kinds, and names only objects outside the requested scope, each covered
// completely.
func validateInspectedPlanningContext(description *protocol.Description, engine *protocol.Engine, request *protocol.InspectRequest, snapshot *protocol.Schema) error {
	inventory := snapshot.GetPlanningContext()
	if inventory == nil {
		return nil
	}
	if !slices.Contains(snapshot.GetTargetFeatures(), PlanningContextInventoryFeature) || !EngineOffers(description, engine.GetId(), PlanningContextInventoryFeature) {
		return contextInventoryError("the feature was not negotiated")
	}
	if len(request.GetSchemas()) == 0 {
		return contextInventoryError("an unfiltered read carries no planning context; its own coverage spans the database")
	}
	kinds := engineContextKinds(engine)
	coverage := inventory.GetCoverage()
	if coverage.GetRepresentation() != protocol.ValueInput_OBSERVED {
		return contextInventoryError("coverage must be observed")
	}
	if err := ValidateModelCoverage(description, snapshot.GetEngine(), coverage); err != nil {
		return err
	}
	for _, record := range coverage.GetKinds() {
		if !slices.Contains(kinds, record.GetModel().GetKind()) {
			return contextInventoryError("coverage names a kind no planning service assesses")
		}
	}
	for _, record := range coverage.GetSubjects() {
		if !slices.Contains(kinds, record.GetKind()) {
			return contextInventoryError("coverage names a kind no planning service assesses")
		}
	}
	var scopes []*protocol.ModelNamespace
	for _, binding := range snapshot.GetScopeBindings() {
		scopes = append(scopes, binding.GetNamespaces()...)
	}
	for _, value := range inventory.GetObjects() {
		switch {
		case value == nil || value.GetOwner().GetKind() != value.GetKind() || !slices.Contains(kinds, value.GetKind()):
			return contextInventoryError("an object must be of a context kind, named by its kind")
		case withinInspectionScopes(scopes, value.GetOwner()):
			return contextInventoryError("an object inside the requested scope belongs to the snapshot's native objects")
		case !inspectionSubjectCovered(coverage, value):
			return contextInventoryError("an object lacks complete subject coverage")
		}
	}
	return nil
}

// PlanningContextValues returns the observed values of a snapshot's planning
// context, in order, for the host to normalize. A snapshot without one returns
// an empty batch. It validates the inventory as InspectionValues does.
func PlanningContextValues(description *protocol.Description, request *protocol.InspectRequest, snapshot *protocol.Schema) (*protocol.NormalizeValuesRequest, error) {
	engine, _, err := valueTarget(description, snapshot.GetEngine(), snapshot.GetTargetFeatures())
	if err != nil {
		return nil, err
	}
	if err := validateInspectedPlanningContext(description, engine, request, snapshot); err != nil {
		return nil, err
	}
	result := &protocol.NormalizeValuesRequest{Engine: snapshot.GetEngine(), TargetFeatures: slices.Clone(snapshot.GetTargetFeatures())}
	for i, value := range snapshot.GetPlanningContext().GetObjects() {
		result.Values = append(result.Values, &protocol.ValueInput{Id: fmt.Sprintf("context/%d", i), Representation: protocol.ValueInput_OBSERVED, Value: value})
	}
	return result, nil
}

// validatePlanningContextInventory checks a planning request's context
// inventory: negotiated, of the assigned context kinds, observed, and disjoint
// from the captured desired and current objects.
func validatePlanningContextInventory(description *protocol.Description, engine *protocol.Engine, service *protocol.ModelPlanning, request *protocol.PlanFeaturesRequest) error {
	inventory := request.GetContextInventory()
	if inventory == nil {
		return nil
	}
	if !slices.Contains(request.GetTargetFeatures(), PlanningContextInventoryFeature) || !EngineOffers(description, engine.GetId(), PlanningContextInventoryFeature) ||
		len(service.GetContextKinds()) == 0 {
		return contextInventoryError("the feature was not negotiated for this service")
	}
	if err := validatePlanningObjectState(description, engine, request, inventory, protocol.ValueInput_OBSERVED); err != nil {
		return err
	}
	captured := planningObjectSubjects(request, nil)
	for _, record := range inventory.GetRecords() {
		if !slices.Contains(service.GetContextKinds(), record.GetKind()) {
			return contextInventoryError("a record is of a kind the service does not assess")
		}
		repeated := func(subject *protocol.ComparisonIdentity) bool {
			return CompareIdentities(subject, record.GetSubject()) == 0
		}
		if slices.ContainsFunc(captured, repeated) {
			return contextInventoryError("a record repeats a captured object")
		}
	}
	return nil
}
