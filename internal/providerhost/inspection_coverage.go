package providerhost

import (
	"fmt"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
)

func (r reader) observedCoverage(source *protocol.ModelCoverage) (schemaext.Coverage, error) {
	h := r.session.target.host
	var kinds []schemaext.KindCoverage
	for _, record := range source.GetKinds() {
		kind := schemaext.Kind(record.GetModel().GetKind())
		contract := h.models[modelKey{kind: kind, representation: schemaext.Observed}]
		if contract == nil {
			return schemaext.Coverage{}, fmt.Errorf("inspected coverage model is not registered")
		}
		knowledge, err := observedKnowledge(record.GetKnowledge())
		if err != nil {
			return schemaext.Coverage{}, err
		}
		// A session establishes no global absence across other databases or engines.
		// Complete regions retain their explicit database and target boundaries.
		if knowledge.State == schemaext.Complete {
			knowledge = schemaext.Knowledge{State: schemaext.Uninspected, Reason: "inspection covers one session, not the global model namespace"}
		}
		domain, err := r.observedNamespaceKnowledge(knowledge, record.GetRegions())
		if err != nil {
			return schemaext.Coverage{}, err
		}
		kinds = append(kinds, schemaext.KindCoverage{Model: schemaext.CodecIdentity{Owner: h.description.GetProviderId(), Kind: kind,
			Representation: schemaext.Observed, Version: 1, Definition: contract.signature}, Knowledge: domain})
	}
	var subjects []schemaext.SubjectCoverage
	for _, record := range source.GetSubjects() {
		subject, err := r.observedIdentity(record.GetSubject(), record.GetNormalizedSubject())
		if err != nil {
			return schemaext.Coverage{}, err
		}
		knowledge, err := observedKnowledge(record.GetKnowledge())
		if err != nil {
			return schemaext.Coverage{}, err
		}
		subjects = append(subjects, schemaext.SubjectCoverage{Kind: schemaext.Kind(record.GetKind()), Subject: subject,
			Targets: []string{r.session.remote.GetEngine()}, Knowledge: knowledge})
	}
	return schemaext.NewCoverage(schemaext.Observed, kinds, subjects)
}

func observedKnowledge(value *protocol.ModelKnowledge) (schemaext.Knowledge, error) {
	states := map[protocol.ModelKnowledge_State]schemaext.KnowledgeState{
		protocol.ModelKnowledge_COMPLETE: schemaext.Complete, protocol.ModelKnowledge_ABSENT: schemaext.Absent,
		protocol.ModelKnowledge_UNINSPECTED: schemaext.Uninspected, protocol.ModelKnowledge_UNREPRESENTABLE: schemaext.Unrepresentable,
	}
	state, ok := states[value.GetState()]
	if !ok {
		return schemaext.Knowledge{}, fmt.Errorf("invalid observed knowledge")
	}
	return schemaext.Knowledge{State: state, Reason: value.GetReason()}, nil
}

// Native namespace claims retain their database qualification. Common table and
// column identities omit it, so these regions cannot grant them completeness.
// Their explicit subject records remain available until owner binding can map
// namespace boundaries and subjects together.
func (r reader) observedNamespaceKnowledge(outside schemaext.Knowledge, regions []*protocol.ModelNamespaceKnowledge) (schemaext.KnowledgeDomain, error) {
	claims := make([]schemaext.NamespaceKnowledge, 0, len(regions))
	for _, region := range regions {
		scope := region.GetScope()
		database := scope.GetDatabase()
		if database == nil || database.GetSource() == "" || database.GetSource() != r.session.remote.GetDatabase() || database.GetNormalized() != database.GetSource() {
			return schemaext.KnowledgeDomain{}, unsupported("observed namespace coverage outside the exact session database")
		}
		knowledge, err := observedKnowledge(region.GetKnowledge())
		if err != nil {
			return schemaext.KnowledgeDomain{}, err
		}
		path := make([]objectidentity.Part, len(scope.GetPathPrefix()))
		for i, component := range scope.GetPathPrefix() {
			path[i] = objectidentity.Part{Source: component.GetSource(), Normalized: component.GetNormalized()}
		}
		claims = append(claims, schemaext.NamespaceKnowledge{Scope: schemaext.NamespaceScope{
			Targets: []string{r.session.remote.GetEngine()}, Database: namespacePart(database),
			Catalog: namespacePart(scope.GetCatalog()), Schema: namespacePart(scope.GetSchema()), PathPrefix: objectidentity.NewParts(path...),
		}, Knowledge: knowledge})
	}
	return schemaext.NewScopedKnowledge(outside, claims...)
}

func namespacePart(component *protocol.NamespaceComponent) *objectidentity.Part {
	if component == nil {
		return nil
	}
	return &objectidentity.Part{Source: component.GetSource(), Normalized: component.GetNormalized()}
}
