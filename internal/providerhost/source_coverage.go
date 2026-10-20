package providerhost

import (
	"fmt"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
)

func (h *Host) sourceCoverage(engine string, source *protocol.ModelCoverage) (schemaext.Coverage, error) {
	var kinds []schemaext.KindCoverage
	for _, record := range source.GetKinds() {
		kind := schemaext.Kind(record.GetModel().GetKind())
		contract := h.models[modelKey{kind: kind, representation: schemaext.Desired}]
		if contract == nil {
			return schemaext.Coverage{}, fmt.Errorf("decoded YAML coverage model is not registered")
		}
		kinds = append(kinds, schemaext.KindCoverage{Model: schemaext.CodecIdentity{Owner: h.description.GetProviderId(), Kind: kind,
			Representation: schemaext.Desired, Version: 1, Definition: contract.signature}, Knowledge: yamlModelKnowledge(contract)})
	}
	var subjects []schemaext.SubjectCoverage
	for _, record := range source.GetSubjects() {
		subject, err := sourceIdentity(record.GetSubject(), record.GetNormalizedSubject())
		if err != nil {
			return schemaext.Coverage{}, err
		}
		knowledge, err := sourceKnowledge(record.GetKnowledge())
		if err != nil {
			return schemaext.Coverage{}, err
		}
		subjects = append(subjects, schemaext.SubjectCoverage{Kind: schemaext.Kind(record.GetKind()), Subject: subject, Targets: []string{engine}, Knowledge: knowledge})
	}
	return schemaext.NewCoverage(schemaext.Desired, kinds, subjects)
}

func sourceKnowledge(knowledge *protocol.ModelKnowledge) (schemaext.Knowledge, error) {
	states := map[protocol.ModelKnowledge_State]schemaext.KnowledgeState{
		protocol.ModelKnowledge_COMPLETE: schemaext.Complete, protocol.ModelKnowledge_ABSENT: schemaext.Absent, protocol.ModelKnowledge_DEFAULTED: schemaext.Defaulted,
	}
	state, ok := states[knowledge.GetState()]
	if !ok {
		return schemaext.Knowledge{}, fmt.Errorf("incomplete decoded YAML knowledge")
	}
	return schemaext.Knowledge{State: state, Reason: knowledge.GetReason()}, nil
}

// The shared identity currently has one parent component and no database or
// path tuple. Refuse richer identities instead of folding their components into
// strings or silently dropping them. Extending that identity is separate from
// normalizing the provider-owned payload retained in resolvedModel.
func sourceIdentity(source, normalized *protocol.Reference) (objectidentity.ID, error) {
	if source.GetDatabase() != "" || normalized.GetDatabase() != "" || len(source.GetPath()) != 0 || len(normalized.GetPath()) != 0 || len(source.GetOverload()) != 0 || len(normalized.GetOverload()) != 0 {
		return objectidentity.ID{}, unsupported("structured database, path, or overload identity in native YAML placement")
	}
	return objectidentity.ID{Kind: objectidentity.Kind(source.GetKind()),
		Catalog: objectidentity.Part{Source: source.GetCatalog(), Normalized: normalized.GetCatalog()},
		Schema:  objectidentity.Part{Source: source.GetSchema(), Normalized: normalized.GetSchema()},
		Name:    objectidentity.Part{Source: source.GetName(), Normalized: normalized.GetName()}}, nil
}

// Kind-wide coverage has no target scope in the shared schema. A model another
// registered engine cannot express in YAML must not acquire absence authority
// from an engine that can. Explicit subject records retain their target scope.
func yamlModelKnowledge(contract *modelContract) schemaext.Knowledge {
	for _, model := range contract.engines {
		if !slices.Contains(model.GetSourceFormats(), protocol.SourceDocument_YAML) {
			return schemaext.Knowledge{State: schemaext.Uninspected, Reason: "YAML does not describe this model for every registered engine"}
		}
	}
	return schemaext.Knowledge{State: schemaext.Complete}
}
