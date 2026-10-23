package providerhost

import (
	"fmt"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
)

// sourceCoverage converts the coverage a provider returned for a document of
// format into the host's coverage, with the kind-wide knowledge the format can
// support.
func (h *Host) sourceCoverage(engine string, format protocol.SourceDocument_Format, source *protocol.ModelCoverage) (schemaext.Coverage, error) {
	var kinds []schemaext.KindCoverage
	for _, record := range source.GetKinds() {
		kind := schemaext.Kind(record.GetModel().GetKind())
		contract := h.models[modelKey{kind: kind, representation: schemaext.Desired}]
		if contract == nil {
			return schemaext.Coverage{}, fmt.Errorf("decoded source coverage model is not registered")
		}
		kinds = append(kinds, schemaext.KindCoverage{Model: schemaext.CodecIdentity{Owner: h.description.GetProviderId(), Kind: kind,
			Representation: schemaext.Desired, Version: 1, Definition: contract.signature}, Knowledge: schemaext.GlobalKnowledge(modelKnowledge(contract, format))})
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
		return schemaext.Knowledge{}, fmt.Errorf("incomplete decoded source knowledge")
	}
	return schemaext.Knowledge{State: state, Reason: knowledge.GetReason()}, nil
}

// Source and comparison tuples must have matching component boundaries. The
// wire permits other normalization shapes; refuse those until native placement
// can preserve both shapes without inventing source spellings.
func sourceIdentity(source, normalized *protocol.Reference) (objectidentity.ID, error) {
	path, err := sourceParts(source.GetPath(), normalized.GetPath(), "path")
	if err != nil {
		return objectidentity.ID{}, err
	}
	overload, err := sourceParts(source.GetOverload(), normalized.GetOverload(), "overload")
	if err != nil {
		return objectidentity.ID{}, err
	}
	return objectidentity.ID{Kind: objectidentity.Kind(source.GetKind()),
		Database:  objectidentity.Part{Source: source.GetDatabase(), Normalized: normalized.GetDatabase()},
		Catalog:   objectidentity.Part{Source: source.GetCatalog(), Normalized: normalized.GetCatalog()},
		Schema:    objectidentity.Part{Source: source.GetSchema(), Normalized: normalized.GetSchema()},
		Namespace: path, Signature: overload,
		Name: objectidentity.Part{Source: source.GetName(), Normalized: normalized.GetName()}}, nil
}

func sourceParts(source, normalized []string, label string) (objectidentity.Parts, error) {
	if len(source) != len(normalized) {
		return objectidentity.Parts{}, unsupported("normalization changing " + label + " component count in native YAML placement")
	}
	parts := make([]objectidentity.Part, len(source))
	for i, part := range source {
		parts[i] = objectidentity.Part{Source: part, Normalized: normalized[i]}
	}
	return objectidentity.NewParts(parts...), nil
}

// Kind-wide coverage has no target scope in the shared schema. A model another
// registered engine cannot express in the source format must not acquire
// absence authority from an engine that can. Explicit subject records retain
// their target scope.
func modelKnowledge(contract *modelContract, format protocol.SourceDocument_Format) schemaext.Knowledge {
	for _, model := range contract.engines {
		if !slices.Contains(model.GetSourceFormats(), format) {
			return schemaext.Knowledge{State: schemaext.Uninspected, Reason: sourceFormatName(format) + " does not describe this model for every registered engine"}
		}
	}
	return schemaext.Knowledge{State: schemaext.Complete}
}

func sourceFormatName(format protocol.SourceDocument_Format) string {
	if format == protocol.SourceDocument_GO_ANNOTATIONS {
		return "Go annotations"
	}
	return format.String()
}
