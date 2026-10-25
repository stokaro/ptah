package provider

import (
	"slices"
	"strings"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/proto"
)

// EntryKeysFeature enables the COLUMN and INDEX scopes of YAML claims and the
// inherited lists of YAML and Go claims: the common keys of an entry the owner
// reads with its own. It is a two-way feature. A provider lists it, and so does
// each engine that uses it, beside DecodeSourceFeature. Contract 25.3.
const EntryKeysFeature = "source.entry-keys"

// validateInheritedNames refuses an invalid or repeated inherited name and one
// the claim owns itself.
func validateInheritedNames(inherited, owned []string) error {
	for i, name := range inherited {
		if !validGoSourceName(name) || slices.Contains(inherited[:i], name) || slices.Contains(owned, name) {
			return sourceError("invalid, repeated or owned inherited attribute")
		}
	}
	return nil
}

// validateInheritedAttributes requires a fragment of a claim with an
// inherited list to carry only those attributes. A claim without one keeps the
// rule of contract 25.2.
func validateInheritedAttributes(inherited []string, fragment *protocol.SourceFragment) error {
	if len(inherited) == 0 {
		return nil
	}
	for _, entry := range fragment.GetInheritedAttributes().GetEntries() {
		if !slices.Contains(inherited, entry.GetKey()) {
			return sourceError("inherited attribute is outside its claim")
		}
	}
	return nil
}

func validateYAMLSourceKeys(description *protocol.Description, engine *protocol.Engine) error {
	claimed := make(map[struct {
		scope protocol.YAMLSourceKey_Scope
		key   string
	}]bool)
	covered := make(map[string]bool)
	for _, claim := range engine.GetYamlSourceKeys() {
		if err := validateYAMLSourceKey(description, engine.GetId(), claim); err != nil {
			return err
		}
		if err := validateYAMLEntryKey(engine, claim); err != nil {
			return err
		}
		identity := struct {
			scope protocol.YAMLSourceKey_Scope
			key   string
		}{claim.GetScope(), claim.GetKey()}
		if claimed[identity] {
			return sourceError("duplicate YAML key claim")
		}
		claimed[identity] = true
		for _, kind := range claim.GetModels() {
			covered[kind] = true
		}
	}
	for _, model := range engine.GetValueModels() {
		if slices.Contains(model.GetSourceFormats(), protocol.SourceDocument_YAML) && !covered[model.GetKind()] {
			return sourceError("YAML model has no declared source keys")
		}
	}
	return nil
}

func validateYAMLSourceKey(description *protocol.Description, engine string, claim *protocol.YAMLSourceKey) error {
	if !slices.Contains(description.GetFeatures(), DecodeSourceFeature) {
		return sourceError("YAML grammar requires source decoding")
	}
	if claim == nil || strings.TrimSpace(claim.GetKey()) == "" || strings.TrimSpace(claim.GetSelector()) == "" || len(claim.GetModels()) == 0 {
		return sourceError("incomplete YAML key claim")
	}
	switch claim.GetScope() {
	case protocol.YAMLSourceKey_DOCUMENT, protocol.YAMLSourceKey_TABLE, protocol.YAMLSourceKey_COLUMN, protocol.YAMLSourceKey_INDEX:
	default:
		return sourceError("unknown YAML key scope")
	}
	seen := make(map[string]bool)
	for _, kind := range claim.GetModels() {
		model := findValueModel(description, engine, kind, protocol.ValueInput_DESIRED)
		if model == nil || !slices.Contains(model.GetSourceFormats(), protocol.SourceDocument_YAML) || seen[kind] {
			return sourceError("YAML key names an unknown, duplicate, or unsupported desired model")
		}
		seen[kind] = true
	}
	return nil
}

// validateYAMLEntryKey requires the feature for an entry scope and for an
// inherited list, which only an entry scope takes.
func validateYAMLEntryKey(engine *protocol.Engine, claim *protocol.YAMLSourceKey) error {
	entry := claim.GetScope() == protocol.YAMLSourceKey_COLUMN || claim.GetScope() == protocol.YAMLSourceKey_INDEX
	if (entry || len(claim.GetInherited()) != 0) && !slices.Contains(engine.GetFeatures(), EntryKeysFeature) {
		return sourceError("a YAML entry key needs " + EntryKeysFeature)
	}
	if len(claim.GetInherited()) != 0 && !entry {
		return sourceError("only a column or index key inherits")
	}
	return validateInheritedNames(claim.GetInherited(), []string{claim.GetKey()})
}

func validateYAMLSourceFragments(description *protocol.Description, engine string, document *protocol.SourceDocument, models map[string]bool) error {
	if document.GetFormat() != protocol.SourceDocument_YAML {
		return nil
	}
	var claims []*protocol.YAMLSourceKey
	for _, declared := range description.GetEngines() {
		if declared.GetId() == engine {
			claims = declared.GetYamlSourceKeys()
		}
	}
	for _, fragment := range document.GetFragments() {
		claim := fragment.GetYamlKey()
		if claim == nil || !slices.ContainsFunc(claims, func(declared *protocol.YAMLSourceKey) bool { return proto.Equal(claim, declared) }) || fragment.GetSelector() != claim.GetSelector() {
			return sourceError("YAML fragment changed its negotiated grammar claim")
		}
		for _, kind := range claim.GetModels() {
			if !models[kind] {
				return sourceError("YAML fragment model is outside document scope")
			}
		}
		if err := validateYAMLSourceBinding(document, fragment); err != nil {
			return err
		}
		if err := validateInheritedAttributes(claim.GetInherited(), fragment); err != nil {
			return err
		}
	}
	return nil
}

func validateYAMLSourceBinding(document *protocol.SourceDocument, fragment *protocol.SourceFragment) error {
	switch fragment.GetYamlKey().GetScope() {
	case protocol.YAMLSourceKey_DOCUMENT:
		if fragment.GetBindingId() != "" || fragment.GetSubject() != nil {
			return sourceError("document YAML key has a table attachment")
		}
	case protocol.YAMLSourceKey_TABLE:
		if fragment.GetBindingId() == "" || fragment.GetSubject() != nil {
			return sourceError("table YAML key requires one frontend binding")
		}
		for _, binding := range document.GetBindings() {
			if binding.GetId() == fragment.GetBindingId() && binding.GetObject().GetKind() == "table" && binding.GetEntryKey() != "" {
				return nil
			}
		}
		return sourceError("table YAML key has no common table binding")
	case protocol.YAMLSourceKey_COLUMN, protocol.YAMLSourceKey_INDEX:
		subject := map[protocol.YAMLSourceKey_Scope]string{protocol.YAMLSourceKey_COLUMN: "column", protocol.YAMLSourceKey_INDEX: "index"}[fragment.GetYamlKey().GetScope()]
		if fragment.GetSubject().GetKind() != subject || len(fragment.GetSubject().GetPath()) != 1 {
			return sourceError("entry YAML key subject does not match its scope")
		}
		for _, binding := range document.GetBindings() {
			if binding.GetId() == fragment.GetBindingId() && binding.GetObject().GetKind() == "table" && binding.GetEntryKey() != "" {
				return nil
			}
		}
		return sourceError("entry YAML key has no common table binding")
	default:
		return sourceError("unknown YAML key scope")
	}
	return nil
}
