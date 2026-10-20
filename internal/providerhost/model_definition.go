package providerhost

import (
	"encoding/json"
	"slices"
	"strings"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"

	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
)

type modelIdentity struct {
	Kind             string          `json:"kind"`
	Representation   string          `json:"representation"`
	Version          uint32          `json:"version"`
	Encoding         string          `json:"encoding"`
	DefinitionSHA256 string          `json:"definition_sha256"`
	Definition       json.RawMessage `json:"definition"`
	CanonicalFormat  string          `json:"canonical_format"`
	SourceFormats    []string        `json:"source_formats"`
}

func identityOfModel(model *protocol.ValueModel) modelIdentity {
	formats := make([]string, 0, len(model.GetSourceFormats()))
	for _, format := range model.GetSourceFormats() {
		formats = append(formats, format.String())
	}
	slices.Sort(formats)
	return modelIdentity{Kind: model.GetKind(), Representation: model.GetRepresentation().String(), Version: model.GetSchemaVersion(),
		Encoding: model.GetEncoding(), DefinitionSHA256: model.GetDefinitionSha256(), Definition: slices.Clone(model.GetDefinitionJson()),
		CanonicalFormat: model.GetCanonicalFormat(), SourceFormats: formats}
}

func modelIdentities(models []*protocol.ValueModel) []modelIdentity {
	result := make([]modelIdentity, 0, len(models))
	for _, model := range models {
		result = append(result, identityOfModel(model))
	}
	slices.SortFunc(result, func(a, b modelIdentity) int {
		if a.Kind != b.Kind {
			return strings.Compare(a.Kind, b.Kind)
		}
		return strings.Compare(a.Representation, b.Representation)
	})
	return result
}

func (h *Host) modelDefinition(contract *modelContract) (json.RawMessage, error) {
	type engineModel struct {
		Model       modelIdentity        `json:"model"`
		Features    []string             `json:"features"`
		Identifiers identifier.Semantics `json:"identifiers"`
	}
	engines := make(map[string]engineModel, len(contract.engines))
	for name, model := range contract.engines {
		declared := h.targets[name].engine
		rules, err := identifierRules(declared.GetIdentifiers(), declared.GetDefaultSchema())
		if err != nil {
			return nil, err
		}
		engines[name] = engineModel{Model: identityOfModel(model), Features: featureSet(declared.GetFeatures()), Identifiers: rules}
	}
	definition := struct {
		Format           string                 `json:"format"`
		Provider         string                 `json:"provider"`
		Version          string                 `json:"version"`
		SemanticIdentity string                 `json:"semantic_identity"`
		ArtifactSHA256   string                 `json:"artifact_sha256"`
		ProtocolRevision uint32                 `json:"protocol_revision"`
		ProviderFeatures []string               `json:"provider_features"`
		Engines          map[string]engineModel `json:"engines"`
	}{"ptah.provider.resolved-model.v1", h.description.GetProviderId(), h.description.GetVersion(), h.description.GetSemanticIdentity(), h.artifact, h.description.GetRevision(), featureSet(h.description.GetFeatures()), engines}
	data, err := json.Marshal(definition)
	if err != nil {
		return nil, err
	}
	return schemaext.CanonicalJSON(data)
}
