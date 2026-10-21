package schemaext

// Sources returns independent captures for a transport adapter. The sources
// remain separate: reconstruct each with NewScopedKnowledge and intersect them,
// rather than combining their regions into one source with wider authority.
func (d KnowledgeDomain) Sources() []KnowledgeSource {
	sources := make([]KnowledgeSource, len(d.sources))
	for i, source := range d.sources {
		sources[i].Outside = source.Outside
		sources[i].Regions = make([]NamespaceKnowledge, len(source.Regions))
		for j, region := range source.Regions {
			sources[i].Regions[j] = NamespaceKnowledge{Scope: region.Scope.clone(), Knowledge: region.Knowledge}
		}
	}
	return sources
}
