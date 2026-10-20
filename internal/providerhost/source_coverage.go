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

// Source and comparison paths must have matching component boundaries. The
// wire permits other normalization shapes; refuse those until native placement
// can preserve both shapes without inventing source spellings.
func sourceIdentity(source, normalized *protocol.Reference) (objectidentity.ID, error) {
	if len(source.GetOverload()) != 0 || len(normalized.GetOverload()) != 0 {
		return objectidentity.ID{}, unsupported("structured overload identity in native YAML placement")
	}
	if len(source.GetPath()) != len(normalized.GetPath()) {
		return objectidentity.ID{}, unsupported("normalization changing path component count in native YAML placement")
	}
	parts := make([]objectidentity.Part, len(source.GetPath()))
	for i, part := range source.GetPath() {
		parts[i] = objectidentity.Part{Source: part, Normalized: normalized.GetPath()[i]}
	}
	return objectidentity.ID{Kind: objectidentity.Kind(source.GetKind()),
		Database:  objectidentity.Part{Source: source.GetDatabase(), Normalized: normalized.GetDatabase()},
		Catalog:   objectidentity.Part{Source: source.GetCatalog(), Normalized: normalized.GetCatalog()},
		Schema:    objectidentity.Part{Source: source.GetSchema(), Normalized: normalized.GetSchema()},
		Namespace: objectidentity.NewParts(parts...),
		Name:      objectidentity.Part{Source: source.GetName(), Normalized: normalized.GetName()}}, nil
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
