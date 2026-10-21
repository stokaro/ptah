package provider

import (
	"fmt"
	"slices"
	"strings"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

func validateModelComparisons(description *protocol.Description, engine *protocol.Engine) error {
	if len(engine.GetComparisons()) > 0 && !slices.Contains(description.GetFeatures(), CompareModelsFeature) {
		return fmt.Errorf("%w: model comparisons require their advertised service", ErrProtocol)
	}
	services, models := make(map[string]bool), make(map[string]bool)
	for _, service := range engine.GetComparisons() {
		if !validName(service.GetId()) || services[service.GetId()] || len(service.GetKinds()) == 0 {
			return fmt.Errorf("%w: invalid or duplicate model comparison", ErrProtocol)
		}
		services[service.GetId()] = true
		if err := validateComparisonPlacement(service); err != nil {
			return err
		}
		for _, kind := range service.GetKinds() {
			if models[kind] || findValueModel(description, engine.GetId(), kind, protocol.ValueInput_DESIRED) == nil || findValueModel(description, engine.GetId(), kind, protocol.ValueInput_OBSERVED) == nil {
				return fmt.Errorf("%w: comparison requires exclusive ownership of both schema representations", ErrProtocol)
			}
			models[kind] = true
		}
		for _, kind := range service.GetChangeKinds() {
			if findValueModel(description, engine.GetId(), kind, protocol.ValueInput_CHANGE) == nil {
				return fmt.Errorf("%w: comparison change model is unavailable", ErrProtocol)
			}
		}
		for _, values := range [][]string{service.GetChangeKinds(), service.GetOwnerKinds(), service.GetActions()} {
			if err := validateComparisonNames(values); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateComparisonNames(values []string) error {
	seen := make(map[string]bool)
	for _, value := range values {
		if strings.TrimSpace(value) == "" || seen[value] {
			return fmt.Errorf("%w: missing or duplicate comparison name", ErrProtocol)
		}
		seen[value] = true
	}
	return nil
}

func findModelComparison(description *protocol.Description, engine, id string) *protocol.ModelComparison {
	for _, target := range description.GetEngines() {
		if target.GetId() != engine {
			continue
		}
		for _, comparison := range target.GetComparisons() {
			if comparison.GetId() == id {
				return comparison
			}
		}
	}
	return nil
}

func validateComparisonPlacement(service *protocol.ModelComparison) error {
	switch service.GetPlacement() {
	case protocol.ModelComparison_OBJECTS:
		if len(service.GetOwnerKinds()) != 0 || len(service.GetChangeKinds()) == 0 {
			return fmt.Errorf("%w: object comparison requires changes and no common owner kinds", ErrProtocol)
		}
	case protocol.ModelComparison_FACETS:
		if len(service.GetOwnerKinds()) == 0 || len(service.GetActions()) != 0 {
			return fmt.Errorf("%w: facet comparison requires common owners and no object actions", ErrProtocol)
		}
	default:
		return fmt.Errorf("%w: unknown comparison placement", ErrProtocol)
	}
	return nil
}
