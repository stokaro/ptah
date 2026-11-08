package provider

import (
	"fmt"
	"slices"

	protocol "ptah.run/provider-sdk/protocol/experimental"
)

// CurrentContextFeature enables CompareModelsRequest.current_context: a
// comparison that names declared kinds or declared facet kinds receives the
// current state of those models too, each current object and facet with its
// value and the current coverage of the kinds. A comparison of a transfer then
// knows that the read recorded the topic or the changefeed it reads without
// describing it, which a plan keeps, where declared objects and declared
// facets say only what the declaration names. A host that does not list the
// feature sends no current context. A two-way feature: the provider lists it,
// and so does each engine whose comparisons read it. Contract 25.40.
const CurrentContextFeature = "models.current-context"

// validateCurrentContextFeature refuses an engine that lists the feature
// without the provider's or without a model comparison.
func validateCurrentContextFeature(description *protocol.Description, engine *protocol.Engine) error {
	if !slices.Contains(engine.GetFeatures(), CurrentContextFeature) {
		return nil
	}
	if !slices.Contains(description.GetFeatures(), CurrentContextFeature) || !slices.Contains(description.GetFeatures(), CompareModelsFeature) ||
		len(engine.GetComparisons()) == 0 {
		return fmt.Errorf("%w: a current context requires the provider's feature and a model comparison", ErrProtocol)
	}
	return nil
}

// validateCurrentContext checks the current context of a comparison request:
// sent only where the engine and the request's target negotiated the
// feature, only to an object comparison that names declared kinds or declared
// facet kinds, in the observed representation. Each record is of one of
// those kinds, carries a value of its kind and is listed once; a facet's
// owner is of a kind the facet's comparison owns. The coverage enrolls only
// those kinds, each with the model the engine declares for it.
func validateCurrentContext(description *protocol.Description, request *protocol.CompareModelsRequest, engine *protocol.Engine, service *protocol.ModelComparison) error {
	state := request.GetCurrentContext()
	if state == nil {
		return nil
	}
	if !EngineOffers(description, request.GetEngine(), CurrentContextFeature) || !slices.Contains(request.GetTargetFeatures(), CurrentContextFeature) {
		return fmt.Errorf("%w: a current context needs %s on both sides", ErrProtocol, CurrentContextFeature)
	}
	kinds := slices.Concat(service.GetDeclaredKinds(), service.GetDeclaredFacetKinds())
	if service.GetPlacement() != protocol.ModelComparison_OBJECTS || len(kinds) == 0 {
		return fmt.Errorf("%w: comparison %q names no declared kind or declared facet kind, so it receives no current context", ErrProtocol, service.GetId())
	}
	for _, record := range state.GetRecords() {
		if !slices.Contains(kinds, record.GetKind()) {
			return fmt.Errorf("%w: a current context record of kind %q is not one comparison %q reads", ErrProtocol, record.GetKind(), service.GetId())
		}
		if !slices.Contains(service.GetDeclaredFacetKinds(), record.GetKind()) {
			continue
		}
		if owner := facetComparisonOf(engine, record.GetKind()); owner == nil || !slices.Contains(owner.GetOwnerKinds(), record.GetSubject().GetKind()) {
			return fmt.Errorf("%w: a current context facet of kind %q has an owner of kind %q", ErrProtocol, record.GetKind(), record.GetSubject().GetKind())
		}
	}
	// The state is checked as an object comparison's own current state is,
	// against the kinds it reads beside its own: a record without a value
	// is refused, since only a declaration excludes a facet on purpose.
	context := &protocol.CompareModelsRequest{Engine: request.GetEngine(), ComparisonId: request.GetComparisonId(), TargetFeatures: request.GetTargetFeatures(),
		Identifiers: request.GetIdentifiers(), Capabilities: request.GetCapabilities(), Kinds: kinds, DatabasePath: request.GetDatabasePath()}
	if err := validateComparisonState(description, context, &protocol.ModelComparison{Id: service.GetId(), Placement: protocol.ModelComparison_OBJECTS},
		protocol.ValueInput_OBSERVED, state); err != nil {
		return fmt.Errorf("current context: %w", err)
	}
	return nil
}
