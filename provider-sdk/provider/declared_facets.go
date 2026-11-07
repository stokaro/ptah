package provider

import (
	"fmt"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

// DeclaredFacetsFeature enables ModelComparison.declared_facet_kinds and
// CompareModelsRequest.declared_facets: an object comparison names the facet
// models of its engine whose declared facets it reads, and the host sends it
// each desired facet of those kinds, with its owner and its value. A
// comparison of a transfer that reads a table's changefeed, where the
// changefeeds are a facet of the table that a facet comparison owns, then
// knows whether the changefeed is declared. A host that does not list the
// feature sees no declared facet kinds and sends no declared facet. A two-way
// feature: the provider lists it, and so does each engine with a comparison
// that names such kinds. Contract 25.36.
const DeclaredFacetsFeature = "models.declared-facets"

// validateDeclaredFacetsFeature refuses an engine that lists the feature
// without the provider's or without a model comparison, and a comparison
// whose declared facet kinds the engine cannot honor: kinds named by an
// engine that does not list the feature, by a facet comparison, a kind named
// twice or blank, and a kind no facet comparison of the engine owns.
func validateDeclaredFacetsFeature(description *protocol.Description, engine *protocol.Engine) error {
	listed := slices.Contains(engine.GetFeatures(), DeclaredFacetsFeature)
	if listed && (!slices.Contains(description.GetFeatures(), DeclaredFacetsFeature) || !slices.Contains(description.GetFeatures(), CompareModelsFeature) ||
		len(engine.GetComparisons()) == 0) {
		return fmt.Errorf("%w: declared facets require the provider's feature and a model comparison", ErrProtocol)
	}
	for _, service := range engine.GetComparisons() {
		if len(service.GetDeclaredFacetKinds()) == 0 {
			continue
		}
		if !listed {
			return fmt.Errorf("%w: comparison %q names declared facet kinds, which need %s", ErrProtocol, service.GetId(), DeclaredFacetsFeature)
		}
		if service.GetPlacement() != protocol.ModelComparison_OBJECTS {
			return fmt.Errorf("%w: facet comparison %q names declared facet kinds, and only an object comparison reads them", ErrProtocol, service.GetId())
		}
		if err := validateComparisonNames(service.GetDeclaredFacetKinds()); err != nil {
			return err
		}
		for _, kind := range service.GetDeclaredFacetKinds() {
			if facetComparisonOf(engine, kind) == nil {
				return fmt.Errorf("%w: comparison %q names declared facet kind %q, which no facet comparison of the engine owns", ErrProtocol, service.GetId(), kind)
			}
		}
	}
	return nil
}

// facetComparisonOf returns the facet comparison of engine that owns kind, or
// nil.
func facetComparisonOf(engine *protocol.Engine, kind string) *protocol.ModelComparison {
	for _, service := range engine.GetComparisons() {
		if service.GetPlacement() == protocol.ModelComparison_FACETS && slices.Contains(service.GetKinds(), kind) {
			return service
		}
	}
	return nil
}

// negotiateDeclaredFacets removes the declared facet kinds of every
// comparison unless the host lists DeclaredFacetsFeature.
func negotiateDeclaredFacets(engine *protocol.Engine, hostFeatures []string) {
	if slices.Contains(hostFeatures, DeclaredFacetsFeature) {
		return
	}
	for _, service := range engine.GetComparisons() {
		service.DeclaredFacetKinds = nil
	}
}

// validateDeclaredFacets checks the declared facets of a comparison request:
// sent only where the engine and the request's target negotiated the
// feature, each of a kind the comparison names in its declared facet kinds,
// owned by an object of a kind the facet's comparison owns, with a desired
// value of its kind, and each owner's facet of a kind listed once.
func validateDeclaredFacets(description *protocol.Description, request *protocol.CompareModelsRequest, engine *protocol.Engine, service *protocol.ModelComparison) error {
	facets := request.GetDeclaredFacets()
	if len(facets) == 0 {
		return nil
	}
	if !EngineOffers(description, request.GetEngine(), DeclaredFacetsFeature) || !slices.Contains(request.GetTargetFeatures(), DeclaredFacetsFeature) {
		return fmt.Errorf("%w: declared facets need %s on both sides", ErrProtocol, DeclaredFacetsFeature)
	}
	values := make([]*protocol.NormalizedValue, 0, len(facets))
	for _, facet := range facets {
		if !slices.Contains(service.GetDeclaredFacetKinds(), facet.GetKind()) {
			return fmt.Errorf("%w: a declared facet of kind %q is not one comparison %q reads", ErrProtocol, facet.GetKind(), service.GetId())
		}
		if err := ValidateComparisonIdentity(facet.GetSubject()); err != nil {
			return err
		}
		if owner := facetComparisonOf(engine, facet.GetKind()); owner == nil || !slices.Contains(owner.GetOwnerKinds(), facet.GetSubject().GetKind()) {
			return fmt.Errorf("%w: a declared facet of kind %q has an owner of kind %q", ErrProtocol, facet.GetKind(), facet.GetSubject().GetKind())
		}
		if err := validateComparisonNames(facet.GetTargets()); err != nil {
			return err
		}
		if facet.GetValue() == nil || facet.GetValue().GetValue().GetKind() != facet.GetKind() {
			return fmt.Errorf("%w: a declared facet of kind %q carries no value of its kind", ErrProtocol, facet.GetKind())
		}
		values = append(values, facet.GetValue())
	}
	ordered := slices.Clone(facets)
	slices.SortFunc(ordered, compareComparisonRecords)
	for i := 1; i < len(ordered); i++ {
		if compareComparisonRecords(ordered[i-1], ordered[i]) == 0 {
			return fmt.Errorf("%w: a declared facet is listed twice", ErrProtocol)
		}
	}
	return validateComparisonValues(description, request, protocol.ValueInput_DESIRED, values)
}
