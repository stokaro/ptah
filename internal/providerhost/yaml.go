package providerhost

import (
	"context"
	"fmt"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"

	"ptah.run/core/schemaext"
	"ptah.run/core/yamlext"
	"ptah.run/internal/providersource"
)

func (h *Host) yamlExtension() []yamlext.Extension {
	extension := yamlext.Extension{Owner: h.description.GetProviderId(), Document: h.decodeYAML}
	documents, tables := make(map[string]bool), make(map[string]bool)
	for _, engine := range h.description.GetEngines() {
		for _, claim := range engine.GetYamlSourceKeys() {
			for _, kind := range claim.GetModels() {
				if !slices.Contains(extension.Kinds, schemaext.Kind(kind)) {
					extension.Kinds = append(extension.Kinds, schemaext.Kind(kind))
				}
			}
			switch claim.GetScope() {
			case protocol.YAMLSourceKey_DOCUMENT:
				if !documents[claim.GetKey()] {
					documents[claim.GetKey()] = true
					extension.Sections = append(extension.Sections, yamlext.Section{Key: claim.GetKey()})
				}
			case protocol.YAMLSourceKey_TABLE:
				if !tables[claim.GetKey()] {
					tables[claim.GetKey()] = true
					extension.EntrySections = append(extension.EntrySections, yamlext.EntrySection{Key: claim.GetKey()})
				}
			}
		}
	}
	if len(extension.Kinds) == 0 {
		return nil
	}
	return []yamlext.Extension{extension}
}

func (h *Host) decodeYAML(ctx context.Context, input yamlext.Document) (yamlext.DocumentResult, error) {
	var result yamlext.DocumentResult
	for _, engine := range h.description.GetEngines() {
		if len(engine.GetYamlSourceKeys()) == 0 {
			continue
		}
		request, err := providersource.YAML(ctx, engine, input)
		if err != nil {
			return yamlext.DocumentResult{}, err
		}
		decoded, err := h.client.DecodeSource(ctx, request)
		if err != nil {
			return yamlext.DocumentResult{}, err
		}
		document := decoded.GetDocuments()[0]
		coverage, err := h.sourceCoverage(engine.GetId(), protocol.SourceDocument_YAML, document.GetCoverage())
		if err != nil {
			return yamlext.DocumentResult{}, err
		}
		result.Coverage, err = result.Coverage.Combine(coverage)
		if err != nil {
			return yamlext.DocumentResult{}, err
		}
		for _, fragment := range document.GetFragments() {
			for _, value := range fragment.GetValues() {
				contribution, err := h.sourceContribution(engine.GetId(), input.Tables, value)
				if err != nil {
					return yamlext.DocumentResult{}, err
				}
				result.Contributions = append(result.Contributions, contribution)
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return yamlext.DocumentResult{}, err
	}
	return result, nil
}

func (h *Host) sourceContribution(engine string, tables yamlext.Tables, source *protocol.SourceModelValue) (yamlext.Contribution, error) {
	decoded, err := h.decodedValue(engine, source)
	if err != nil {
		return yamlext.Contribution{}, err
	}
	result := yamlext.Contribution{Label: string(decoded.kind), Targets: []string{engine}}
	switch source.GetPlacement() {
	case protocol.SourceModelValue_NATIVE_OBJECT:
		result.Object = &schemaext.Object{Ref: decoded.subject, Value: decoded.value, Targets: []string{engine}}
	case protocol.SourceModelValue_FACET:
		if err := decoded.requireTable("YAML"); err != nil {
			return yamlext.Contribution{}, err
		}
		index, err := sourceTable(tables, source.GetValue().GetValue().GetOwner())
		if err != nil {
			return yamlext.Contribution{}, err
		}
		result.Table, result.Facet = index, decoded.value
	default:
		return yamlext.Contribution{}, fmt.Errorf("unknown decoded YAML placement")
	}
	return result, nil
}

func sourceTable(tables yamlext.Tables, subject *protocol.Reference) (int, error) {
	if subject.GetKind() != "table" {
		return 0, unsupported("YAML facet attachment to non-table objects")
	}
	found := -1
	for i, table := range tables {
		if table.Name == subject.GetName() && table.Schema == subject.GetSchema() {
			if found >= 0 {
				return 0, fmt.Errorf("decoded YAML facet has ambiguous table attachment")
			}
			found = i
		}
	}
	if found < 0 {
		return 0, fmt.Errorf("decoded YAML facet names no declared table")
	}
	return found, nil
}
