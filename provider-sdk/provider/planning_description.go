package provider

import (
	"fmt"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

func validateModelPlanning(description *protocol.Description, engine *protocol.Engine) error {
	if len(engine.GetPlanning()) > 0 && (!slices.Contains(description.GetFeatures(), PlanFeaturesFeature) || !slices.Contains(description.GetFeatures(), NormalizeValuesFeature)) {
		return fmt.Errorf("%w: planning requires advertised planning and normalization services", ErrProtocol)
	}
	services, changes, parents := make(map[string]bool), make(map[string]bool), make(map[string]bool)
	for _, service := range engine.GetPlanning() {
		if !validName(service.GetId()) || services[service.GetId()] || len(service.GetChangeKinds())+len(service.GetParentKinds())+len(service.GetContextKinds()) == 0 {
			return fmt.Errorf("%w: missing or duplicate planning service", ErrProtocol)
		}
		services[service.GetId()] = true
		if len(service.GetChangeKinds()) > 0 && len(service.GetOperationKinds()) == 0 {
			return fmt.Errorf("%w: change planning requires operation models", ErrProtocol)
		}
		if err := validatePlanningVocabulary(description, engine, service); err != nil {
			return err
		}
		for _, kind := range service.GetChangeKinds() {
			if changes[kind] || findValueModel(description, engine.GetId(), kind, protocol.ValueInput_CHANGE) == nil {
				return fmt.Errorf("%w: planning change ownership is missing or duplicated", ErrProtocol)
			}
			changes[kind] = true
		}
		for _, kind := range service.GetParentKinds() {
			if parents[kind] || findValueModel(description, engine.GetId(), kind, protocol.ValueInput_DESIRED) == nil || findValueModel(description, engine.GetId(), kind, protocol.ValueInput_OBSERVED) == nil {
				return fmt.Errorf("%w: planning parent ownership requires both representations", ErrProtocol)
			}
			parents[kind] = true
		}
	}
	return validatePlanningObjectAssignments(description, engine)
}

func findModelPlanning(description *protocol.Description, engine, id string) *protocol.ModelPlanning {
	for _, target := range description.GetEngines() {
		if target.GetId() != engine {
			continue
		}
		for _, service := range target.GetPlanning() {
			if service.GetId() == id {
				return service
			}
		}
	}
	return nil
}

func validatePlanningVocabulary(description *protocol.Description, engine *protocol.Engine, service *protocol.ModelPlanning) error {
	for _, kinds := range [][]string{service.GetChangeKinds(), service.GetParentKinds(), service.GetOperationKinds(), service.GetContextKinds()} {
		if err := validateComparisonNames(kinds); err != nil {
			return err
		}
	}
	for _, kind := range service.GetOperationKinds() {
		if findValueModel(description, engine.GetId(), kind, protocol.ValueInput_OPERATION) == nil {
			return fmt.Errorf("%w: planning operation model is unavailable", ErrProtocol)
		}
	}
	return nil
}
