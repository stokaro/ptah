package providerhost

import (
	"context"
	"fmt"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/yamlext"
	"ptah.run/internal/providersource"
)

func (h *Host) yamlExtension() []yamlext.Extension {
	extension := yamlext.Extension{Owner: h.description.GetProviderId(), Document: h.decodeYAML}
	documents := make(map[string]bool)
	entries := make(map[[2]string]bool)
	for _, engine := range h.description.GetEngines() {
		for _, claim := range engine.GetYamlSourceKeys() {
			for _, kind := range claim.GetModels() {
				if !slices.Contains(extension.Kinds, schemaext.Kind(kind)) {
					extension.Kinds = append(extension.Kinds, schemaext.Kind(kind))
				}
			}
			if claim.GetScope() == protocol.YAMLSourceKey_DOCUMENT {
				if !documents[claim.GetKey()] {
					documents[claim.GetKey()] = true
					extension.Sections = append(extension.Sections, yamlext.Section{Key: claim.GetKey()})
				}
				continue
			}
			// A key of an entry. The SDK keeps a column and an index scope,
			// and an inherited list, only where both sides listed
			// provider.EntryKeysFeature.
			entry := yamlEntries[claim.GetScope()]
			if identity := [2]string{entry, claim.GetKey()}; !entries[identity] {
				entries[identity] = true
				extension.EntrySections = append(extension.EntrySections, yamlext.EntrySection{Entry: entry, Key: claim.GetKey(), Reads: slices.Clone(claim.GetInherited())})
			}
		}
	}
	if len(extension.Kinds) == 0 {
		return nil
	}
	return []yamlext.Extension{extension}
}

// yamlEntries is the frontend's entry a key of each entry scope belongs to.
var yamlEntries = map[protocol.YAMLSourceKey_Scope]string{
	protocol.YAMLSourceKey_TABLE:  yamlext.EntryTable,
	protocol.YAMLSourceKey_COLUMN: yamlext.EntryColumn,
	protocol.YAMLSourceKey_INDEX:  yamlext.EntryIndex,
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
		sent := make(map[string]yamlFragment, len(request.GetDocuments()[0].GetFragments()))
		for _, fragment := range request.GetDocuments()[0].GetFragments() {
			written := yamlFragment{fragment: fragment}
			for _, binding := range request.GetDocuments()[0].GetBindings() {
				if binding.GetId() == fragment.GetBindingId() {
					written.table = binding.GetObject()
				}
			}
			sent[fragment.GetId()] = written
		}
		for _, fragment := range document.GetFragments() {
			for _, value := range fragment.GetValues() {
				contribution, err := h.sourceContribution(engine.GetId(), input.Tables, sent[fragment.GetId()], value)
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

// sourceContribution joins a decoded value to the document. A facet belongs
// to its owner: a table, a column or an index of a table the document declares.
// The fragment that wrote it limits which: a column or an index key declares
// only its own entry, a table key its table or a column of that table, and a
// document key a table.
// yamlFragment is a fragment the host sent, with the table its binding
// names, or nil for a document key.
type yamlFragment struct {
	fragment *protocol.SourceFragment
	table    *protocol.Reference
}

func (h *Host) sourceContribution(engine string, tables yamlext.Tables, fragment yamlFragment, source *protocol.SourceModelValue) (yamlext.Contribution, error) {
	decoded, err := h.decodedValue(engine, source)
	if err != nil {
		return yamlext.Contribution{}, err
	}
	result := yamlext.Contribution{Label: string(decoded.kind), Targets: []string{engine}}
	switch source.GetPlacement() {
	case protocol.SourceModelValue_NATIVE_OBJECT:
		result.Object = &schemaext.Object{Ref: decoded.subject, Value: decoded.value, Targets: []string{engine}}
	case protocol.SourceModelValue_FACET:
		owner := source.GetValue().GetValue().GetOwner()
		if err := yamlFacetOwner(fragment, owner); err != nil {
			return yamlext.Contribution{}, err
		}
		if owner.GetKind() == "table" {
			if err := decoded.requireTable("YAML"); err != nil {
				return yamlext.Contribution{}, err
			}
		}
		table := owner
		switch owner.GetKind() {
		case "column":
			table, result.Column = &protocol.Reference{Kind: "table", Schema: owner.GetSchema(), Name: owner.GetPath()[0]}, owner.GetName()
		case "index":
			table, result.Index = &protocol.Reference{Kind: "table", Schema: owner.GetSchema(), Name: owner.GetPath()[0]}, owner.GetName()
		}
		index, err := sourceTable(tables, table)
		if err != nil {
			return yamlext.Contribution{}, err
		}
		result.Table, result.Facet = index, decoded.value
	default:
		return yamlext.Contribution{}, fmt.Errorf("unknown decoded YAML placement")
	}
	return result, nil
}

// yamlFacetOwner refuses a facet owned by anything but a table, a column or
// an index of a table, and one the fragment that wrote it does not declare.
func yamlFacetOwner(written yamlFragment, owner *protocol.Reference) error {
	kind := owner.GetKind()
	if kind != "table" && ((kind != "column" && kind != "index") || len(owner.GetPath()) != 1 ||
		owner.GetDatabase() != "" || owner.GetCatalog() != "" || len(owner.GetOverload()) != 0) {
		return unsupported(fmt.Sprintf("a YAML facet owned by %s %q", kind, owner.GetName()))
	}
	fragment := written.fragment
	switch fragment.GetYamlKey().GetScope() {
	case protocol.YAMLSourceKey_DOCUMENT:
		if kind != "table" {
			return unsupported("YAML facet attachment of a document key to non-table objects")
		}
	case protocol.YAMLSourceKey_COLUMN, protocol.YAMLSourceKey_INDEX:
		if !providersource.SameSubject(owner, fragment.GetSubject()) {
			return fmt.Errorf("%w: decoded YAML facet is owned by %s %q of %q, which is not the %s %q its key belongs to", ptaherr.ErrInvalidAttributeValue,
				kind, owner.GetName(), owner.GetPath(), fragment.GetSubject().GetKind(), fragment.GetSubject().GetName())
		}
	case protocol.YAMLSourceKey_TABLE:
		table := &protocol.Reference{Kind: "table", Schema: owner.GetSchema(), Name: owner.GetName()}
		if kind != "table" {
			table.Name = owner.GetPath()[0]
		}
		if kind == "index" || !providersource.SameSubject(table, &protocol.Reference{Kind: "table", Schema: written.table.GetSchema(), Name: written.table.GetName()}) {
			return fmt.Errorf("%w: decoded YAML facet is owned by %s %q, which is neither the table %q of its key nor a column of it", ptaherr.ErrInvalidAttributeValue,
				kind, owner.GetName(), written.table.GetName())
		}
	}
	return nil
}

func sourceTable(tables yamlext.Tables, subject *protocol.Reference) (int, error) {
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
