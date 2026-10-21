package schemaext

import "fmt"

// MapScopes rebinds every namespace region while retaining its knowledge and
// independent source. bind may split a region, but must not erase it. Each input
// and returned scope is copied; failure returns no partial domain. The caller
// must use the same identity rules for scopes and explicit subjects, and must
// preserve the region's meaning under those rules.
func (d KnowledgeDomain) MapScopes(bind func(NamespaceScope) ([]NamespaceScope, error)) (KnowledgeDomain, error) {
	if bind == nil {
		return KnowledgeDomain{}, fmt.Errorf("%w: namespace binding is missing", ErrInvalidValue)
	}
	if len(d.sources) == 0 {
		return d, nil
	}
	sources := make([]KnowledgeSource, len(d.sources))
	for i, source := range d.sources {
		sources[i].Outside = source.Outside
		for _, region := range source.Regions {
			scopes, err := bind(region.Scope.clone())
			if err != nil {
				return KnowledgeDomain{}, err
			}
			if len(scopes) == 0 || len(scopes) > maxKnowledgeRegions-len(sources[i].Regions) {
				return KnowledgeDomain{}, fmt.Errorf("%w: namespace binding erased a region or exceeded its limit", ErrInvalidValue)
			}
			for _, scope := range scopes {
				normalized, err := scope.normalized()
				if err != nil {
					return KnowledgeDomain{}, err
				}
				sources[i].Regions = append(sources[i].Regions, NamespaceKnowledge{Scope: normalized, Knowledge: region.Knowledge})
			}
		}
	}
	return normalizeKnowledgeDomain(sources)
}
