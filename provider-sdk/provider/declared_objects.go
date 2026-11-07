package provider

import (
	"fmt"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

// DeclaredObjectsFeature enables ModelComparison.declared_kinds and
// CompareModelsRequest.declared_objects: a comparison names the object
// models of its engine whose declared objects it reads, and the host sends
// it the identity of each. A comparison of a transfer that reads a topic,
// where another comparison of the engine owns topics, then knows whether the
// topic is declared. A host that does not list the feature sees no declared
// kinds and sends no declared object. A two-way feature: the provider lists
// it, and so does each engine with a comparison that names such kinds.
// Contract 25.33.
const DeclaredObjectsFeature = "models.declared-objects"

// validateDeclaredObjectsFeature refuses an engine that lists the feature
// without the provider's or without a model comparison, and a comparison
// whose declared kinds the engine cannot honor: kinds named by an engine
// that does not list the feature, by a facet comparison, which receives no
// object context, a kind named twice or blank, a kind the comparison owns
// itself, whose declared objects its desired state already holds, and a kind
// no other object comparison of the engine owns.
func validateDeclaredObjectsFeature(description *protocol.Description, engine *protocol.Engine) error {
	listed := slices.Contains(engine.GetFeatures(), DeclaredObjectsFeature)
	if listed && (!slices.Contains(description.GetFeatures(), DeclaredObjectsFeature) || !slices.Contains(description.GetFeatures(), CompareModelsFeature) ||
		len(engine.GetComparisons()) == 0) {
		return fmt.Errorf("%w: declared objects require the provider's feature and a model comparison", ErrProtocol)
	}
	for _, service := range engine.GetComparisons() {
		if len(service.GetDeclaredKinds()) == 0 {
			continue
		}
		if !listed {
			return fmt.Errorf("%w: comparison %q names declared kinds, which need %s", ErrProtocol, service.GetId(), DeclaredObjectsFeature)
		}
		if service.GetPlacement() != protocol.ModelComparison_OBJECTS {
			return fmt.Errorf("%w: facet comparison %q names declared kinds, and a facet comparison receives no object context", ErrProtocol, service.GetId())
		}
		if err := validateComparisonNames(service.GetDeclaredKinds()); err != nil {
			return err
		}
		for _, kind := range service.GetDeclaredKinds() {
			if slices.Contains(service.GetKinds(), kind) {
				return fmt.Errorf("%w: comparison %q names its own model %q among its declared kinds", ErrProtocol, service.GetId(), kind)
			}
			if !slices.ContainsFunc(engine.GetComparisons(), func(other *protocol.ModelComparison) bool {
				return other.GetPlacement() == protocol.ModelComparison_OBJECTS && slices.Contains(other.GetKinds(), kind)
			}) {
				return fmt.Errorf("%w: comparison %q names declared kind %q, which no object comparison of the engine owns", ErrProtocol, service.GetId(), kind)
			}
		}
	}
	return nil
}

// negotiateDeclaredObjects removes the declared kinds of every comparison
// unless the host lists DeclaredObjectsFeature.
func negotiateDeclaredObjects(engine *protocol.Engine, hostFeatures []string) {
	if slices.Contains(hostFeatures, DeclaredObjectsFeature) {
		return
	}
	for _, service := range engine.GetComparisons() {
		service.DeclaredKinds = nil
	}
}

// validateDeclaredObjects checks the declared objects of a comparison request:
// sent only where the engine and the request's target negotiated the
// feature, each a valid identity named once, of a kind the comparison names
// in its declared kinds.
func validateDeclaredObjects(description *protocol.Description, request *protocol.CompareModelsRequest, service *protocol.ModelComparison) error {
	objects := request.GetDeclaredObjects()
	if len(objects) == 0 {
		return nil
	}
	if !EngineOffers(description, request.GetEngine(), DeclaredObjectsFeature) || !slices.Contains(request.GetTargetFeatures(), DeclaredObjectsFeature) {
		return fmt.Errorf("%w: declared objects need %s on both sides", ErrProtocol, DeclaredObjectsFeature)
	}
	if err := validateComparisonIdentities(objects); err != nil {
		return err
	}
	for _, object := range objects {
		if !slices.Contains(service.GetDeclaredKinds(), object.GetKind()) {
			return fmt.Errorf("%w: a declared object of kind %q is not one comparison %q reads", ErrProtocol, object.GetKind(), service.GetId())
		}
	}
	return nil
}
