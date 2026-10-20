package provider

import (
	"slices"
	"strings"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/proto"
)

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
	case protocol.YAMLSourceKey_DOCUMENT, protocol.YAMLSourceKey_TABLE:
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
	default:
		return sourceError("unknown YAML key scope")
	}
	return nil
}
