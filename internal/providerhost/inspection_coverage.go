package providerhost

import (
	"fmt"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
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
		domain, err := r.observedNamespaceKnowledge(kind, knowledge, record.GetRegions())
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

// Native selector scopes remove only the verified session database. Other
// native scopes retain wire coordinates. Common tables and columns use disjoint
// subject-kind scopes with the session database removed and the column's final
// path component moved into Parent, as observedIdentity does.
func (r reader) observedNamespaceKnowledge(kind schemaext.Kind, outside schemaext.Knowledge, regions []*protocol.ModelNamespaceKnowledge) (schemaext.KnowledgeDomain, error) {
	var claims []schemaext.NamespaceKnowledge
	for _, region := range regions {
		scope := region.GetScope()
		if err := r.validateInspectionNamespace(scope); err != nil {
			return schemaext.KnowledgeDomain{}, err
		}
		knowledge, err := observedKnowledge(region.GetKnowledge())
		if err != nil {
			return schemaext.KnowledgeDomain{}, err
		}
		scopes, err := namespaceScopes(r.session.target.host.selections, r.session.info.IdentifierSemantics, r.session.remote.GetEngine(), kind, scope)
		if err != nil {
			return schemaext.KnowledgeDomain{}, err
		}
		for _, bound := range scopes {
			claims = append(claims, schemaext.NamespaceKnowledge{Scope: bound, Knowledge: knowledge})
		}
	}
	return schemaext.NewScopedKnowledge(outside, claims...)
}

func (r reader) validateInspectionNamespace(scope *protocol.ModelNamespace) error {
	database := scope.GetDatabase()
	if database == nil || database.GetSource() == "" || database.GetSource() != r.session.remote.GetDatabase() || database.GetNormalized() != database.GetSource() {
		return unsupported("observed namespace coverage outside the exact session database")
	}
	return nil
}

// namespaceScopes is how the host holds one namespace region a provider
// reports for kind on engine: the scope the kind's native objects match, and,
// where the region can hold common tables and columns, the scope their facets
// match. Reading an inspection builds its scopes here, and so does spelling an
// encoding request's regions (encodeBatch.regions), which reads each region it
// spells back through this function: two copies of this rule would agree when
// the second is written and drift when the first changes, and the drift would
// send a provider a region wider or narrower than the one it reported.
func namespaceScopes(selections *scopeSelections, semantics identifier.Semantics, engine string, kind schemaext.Kind, scope *protocol.ModelNamespace) ([]schemaext.NamespaceScope, error) {
	path := make([]objectidentity.Part, len(scope.GetPathPrefix()))
	for i, component := range scope.GetPathPrefix() {
		path[i] = *namespacePart(component)
	}
	native := schemaext.NamespaceScope{SubjectKinds: []objectidentity.Kind{objectidentity.Kind(kind)}, Targets: []string{engine},
		Database: namespacePart(scope.GetDatabase()), Catalog: namespacePart(scope.GetCatalog()), Schema: namespacePart(scope.GetSchema()), PathPrefix: objectidentity.NewParts(path...)}
	if selections.selects(objectidentity.Kind(kind)) {
		native.Database = &objectidentity.Part{}
		// A selection describes standalone objects, never table-owned facets.
		// Preserve that bound when shared comparison binds table identities.
		native.Parent = &objectidentity.Part{}
	}
	common, err := commonNamespaceScopes(semantics, engine, scope)
	if err != nil {
		return nil, err
	}
	return append([]schemaext.NamespaceScope{native}, common...), nil
}

func commonNamespaceScopes(semantics identifier.Semantics, engine string, scope *protocol.ModelNamespace) ([]schemaext.NamespaceScope, error) {
	// These wire regions contain no common identities, whose catalog is empty
	// and whose only path component, on columns, is their owning table.
	if scope.GetCatalog().GetNormalized() != "" || len(scope.GetPathPrefix()) > 1 {
		return nil, nil
	}
	common := schemaext.NamespaceScope{PathExact: true, Targets: []string{engine}, Database: &objectidentity.Part{}, Catalog: &objectidentity.Part{}}
	if schema := scope.GetSchema(); schema != nil {
		if schema.GetNormalized() != semantics.TableIdentityKey(schema.GetSource()) {
			return nil, unsupported("common namespace schema normalization")
		}
		resolved := objectidentity.NewBuilder(semantics).TablePartsVerbatim(schema.GetSource(), "").Schema
		common.Schema = &resolved
	}
	if path := scope.GetPathPrefix(); len(path) == 1 {
		if path[0].GetNormalized() != semantics.TableIdentityKey(path[0].GetSource()) {
			return nil, unsupported("common namespace parent normalization")
		}
		common.Parent = namespacePart(path[0])
		common.SubjectKinds = []objectidentity.Kind{objectidentity.KindColumn}
		return []schemaext.NamespaceScope{common}, nil
	}
	common.SubjectKinds = []objectidentity.Kind{objectidentity.KindTable, objectidentity.KindColumn}
	return []schemaext.NamespaceScope{common}, nil
}

func namespacePart(component *protocol.NamespaceComponent) *objectidentity.Part {
	if component == nil {
		return nil
	}
	return &objectidentity.Part{Source: component.GetSource(), Normalized: component.GetNormalized()}
}
