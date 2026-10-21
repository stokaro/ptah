package provider

import (
	"fmt"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

type planningStepKey struct{ owner, name string }

func stepKey(id *protocol.StepIdentity) planningStepKey {
	return planningStepKey{id.GetOwner(), id.GetName()}
}
func validateStepIdentity(id *protocol.StepIdentity) error {
	if !comparisonReason(id.GetOwner()) || !comparisonReason(id.GetName()) {
		return fmt.Errorf("%w: invalid planning step identity", ErrProtocol)
	}
	return nil
}

func validatePlanningContext(request *protocol.PlanFeaturesRequest) error {
	// Identifier facts and capability names have the same contract in both
	// contextual services; neither service infers a missing capability value.
	context := &protocol.CompareModelsRequest{Identifiers: request.GetIdentifiers(), Capabilities: request.GetCapabilities()}
	if err := validateComparisonContext(context, &protocol.ModelComparison{Placement: protocol.ModelComparison_OBJECTS}); err != nil {
		return err
	}
	for _, types := range [][]*protocol.PlanningType{request.GetDesiredTypes(), request.GetCurrentTypes()} {
		identities := make([]*protocol.ComparisonIdentity, 0, len(types))
		for _, value := range types {
			identities = append(identities, value.GetIdentity())
			if err := validateComparisonNames(value.GetReferences()); err != nil {
				return err
			}
		}
		if err := validateComparisonIdentities(identities); err != nil {
			return err
		}
	}
	seen := make(map[planningStepKey]bool)
	for _, step := range request.GetCommonSteps() {
		if err := validateStepIdentity(step.GetId()); err != nil {
			return err
		}
		key := stepKey(step.GetId())
		if seen[key] {
			return fmt.Errorf("%w: duplicate common planning step", ErrProtocol)
		}
		seen[key] = true
		if err := validatePlanningMetadata(step.GetMetadata()); err != nil {
			return err
		}
		if column := step.GetAddedColumn(); column != nil {
			if err := validateDeclaredColumn(column); err != nil {
				return err
			}
		}
	}
	return nil
}

func validatePlanningMetadata(metadata *protocol.PlanningMetadata) error {
	if metadata == nil {
		return fmt.Errorf("%w: missing planning metadata", ErrProtocol)
	}
	if _, ok := protocol.PlanningMetadata_Transaction_name[int32(metadata.GetTransaction())]; !ok {
		return fmt.Errorf("%w: unknown planning transaction requirement", ErrProtocol)
	}
	if _, ok := protocol.PlanningMetadata_Placement_name[int32(metadata.GetPlacement())]; !ok {
		return fmt.Errorf("%w: unknown planning placement", ErrProtocol)
	}
	switch metadata.GetImpact() {
	case protocol.ChangeSemantics_UNKNOWN, protocol.ChangeSemantics_ADDITIVE, protocol.ChangeSemantics_BEHAVIORAL, protocol.ChangeSemantics_DESTRUCTIVE:
	default:
		return fmt.Errorf("%w: missing planning impact", ErrProtocol)
	}
	identities := make([]*protocol.ComparisonIdentity, 0, len(metadata.GetEffects()))
	for _, effect := range metadata.GetEffects() {
		switch effect.GetAction() {
		case protocol.PlanningEffect_READ, protocol.PlanningEffect_CREATE, protocol.PlanningEffect_ALTER, protocol.PlanningEffect_DROP:
		default:
			return fmt.Errorf("%w: unknown planning effect", ErrProtocol)
		}
		identities = append(identities, effect.GetSubject())
	}
	return validateComparisonIdentities(identities)
}

func validatePlanningTables(description *protocol.Description, request *protocol.PlanFeaturesRequest, engine *protocol.Engine) error {
	identities := make([]*protocol.ComparisonIdentity, 0, len(request.GetTables()))
	for _, table := range request.GetTables() {
		identities = append(identities, table.GetSubject())
		if table.GetSubject().GetKind() != "table" {
			return fmt.Errorf("%w: planning parent is not a table", ErrProtocol)
		}
		desired, current := table.GetDesired(), table.GetCurrent()
		if err := validatePlanningLifecycle(table); err != nil {
			return err
		}
		if desired != nil {
			if !comparisonReason(desired.GetName()) {
				return fmt.Errorf("%w: missing declared table name", ErrProtocol)
			}
			if err := validateDeclaredTable(desired); err != nil {
				return err
			}
			if err := validateCapturedFeatures(description, request, engine, protocol.ValueInput_DESIRED, desired.GetFeatures()); err != nil {
				return err
			}
		}
		if current != nil {
			if !comparisonReason(current.GetName()) {
				return fmt.Errorf("%w: missing observed table name", ErrProtocol)
			}
			if err := validateObservedTable(current); err != nil {
				return err
			}
			if err := validateCapturedFeatures(description, request, engine, protocol.ValueInput_OBSERVED, current.GetFeatures()); err != nil {
				return err
			}
		}
	}
	return validateComparisonIdentities(identities)
}

func validatePlanningLifecycle(table *protocol.PlanningTable) error {
	desired, current := table.GetDesired() != nil, table.GetCurrent() != nil
	valid := false
	switch table.GetAction() {
	case protocol.PlanningTable_CONTEXT:
		valid = desired || current
	case protocol.PlanningTable_CREATE:
		valid = desired && !current
	case protocol.PlanningTable_DROP:
		valid = current && !desired
	case protocol.PlanningTable_ALTER, protocol.PlanningTable_REBUILD:
		valid = desired && current
	}
	if !valid {
		return fmt.Errorf("%w: planning table action disagrees with captured state", ErrProtocol)
	}
	return nil
}

func validateCapturedFeatures(description *protocol.Description, request *protocol.PlanFeaturesRequest, engine *protocol.Engine, representation protocol.ValueInput_Representation, captured *protocol.CapturedFeatures) error {
	if captured == nil {
		return fmt.Errorf("%w: missing captured feature knowledge", ErrProtocol)
	}
	context := planningValueContext(request)
	for _, model := range engine.GetValueModels() {
		if model.GetRepresentation() == representation {
			context.Kinds = append(context.Kinds, model.GetKind())
		}
	}
	for _, entry := range []struct {
		state     *protocol.ComparisonState
		placement protocol.ModelComparison_Placement
	}{{captured.GetObjects(), protocol.ModelComparison_OBJECTS}, {captured.GetFacets(), protocol.ModelComparison_FACETS}} {
		service := &protocol.ModelComparison{Placement: entry.placement}
		for _, record := range entry.state.GetRecords() {
			service.OwnerKinds = append(service.OwnerKinds, record.GetSubject().GetKind())
		}
		slices.Sort(service.OwnerKinds)
		service.OwnerKinds = slices.Compact(service.OwnerKinds)
		if err := validateComparisonState(description, context, service, representation, entry.state); err != nil {
			return err
		}
	}
	return nil
}
