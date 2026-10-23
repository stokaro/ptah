package provider

import (
	"slices"
	"strings"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/proto"
)

func validateGoSourceDirectives(description *protocol.Description, engine *protocol.Engine) error {
	claimed := make(map[string]bool)
	for _, claim := range engine.GetGoSourceDirectives() {
		if err := validateGoSourceDirective(description, engine.GetId(), claim); err != nil {
			return err
		}
		if claimed[claim.GetDirective()] {
			return sourceError("duplicate Go directive claim")
		}
		claimed[claim.GetDirective()] = true
	}
	return nil
}

func validGoSourceName(name string) bool {
	return name != "" && !strings.ContainsFunc(name, func(r rune) bool { return r == '=' || r == ',' || r == '"' || strings.ContainsRune(" \t\r\n", r) })
}

func validateGoSourceDirective(description *protocol.Description, engine string, claim *protocol.GoSourceDirective) error {
	if !slices.Contains(description.GetFeatures(), DecodeSourceFeature) {
		return sourceError("Go grammar requires source decoding")
	}
	if claim == nil || !validGoSourceName(claim.GetDirective()) || strings.TrimSpace(claim.GetSelector()) == "" || len(claim.GetModels()) == 0 {
		return sourceError("incomplete Go directive claim")
	}
	switch claim.GetScope() {
	case protocol.GoSourceDirective_DOCUMENT, protocol.GoSourceDirective_TABLE, protocol.GoSourceDirective_COLUMN, protocol.GoSourceDirective_INDEX:
	default:
		return sourceError("unknown Go directive scope")
	}
	attributes := make(map[string]bool)
	for _, attribute := range claim.GetAttributes() {
		if !validGoSourceName(attribute) || attributes[attribute] {
			return sourceError("invalid or duplicate Go directive attribute")
		}
		attributes[attribute] = true
	}
	seen := make(map[string]bool)
	for _, kind := range claim.GetModels() {
		model := findValueModel(description, engine, kind, protocol.ValueInput_DESIRED)
		if model == nil || !slices.Contains(model.GetSourceFormats(), protocol.SourceDocument_GO_ANNOTATIONS) || seen[kind] {
			return sourceError("Go directive names an unknown, duplicate, or unsupported desired model")
		}
		seen[kind] = true
	}
	return nil
}

func validateGoSourceFragments(description *protocol.Description, engine string, document *protocol.SourceDocument, models map[string]bool) error {
	if document.GetFormat() != protocol.SourceDocument_GO_ANNOTATIONS {
		return nil
	}
	var claims []*protocol.GoSourceDirective
	for _, declared := range description.GetEngines() {
		if declared.GetId() == engine {
			claims = declared.GetGoSourceDirectives()
		}
	}
	for _, fragment := range document.GetFragments() {
		claim := fragment.GetGoDirective()
		if claim == nil && len(claims) == 0 {
			// An engine that declares no Go syntax keeps accepting captured Go
			// fragments without a claim, as it did before directives existed.
			continue
		}
		if claim == nil || !slices.ContainsFunc(claims, func(declared *protocol.GoSourceDirective) bool { return proto.Equal(claim, declared) }) || fragment.GetSelector() != claim.GetSelector() {
			return sourceError("Go fragment changed its negotiated directive claim")
		}
		for _, kind := range claim.GetModels() {
			if !models[kind] {
				return sourceError("Go fragment model is outside document scope")
			}
		}
		if err := validateGoSourceAttributes(claim, fragment); err != nil {
			return err
		}
		if err := validateGoSourceBinding(document, claim, fragment); err != nil {
			return err
		}
	}
	return nil
}

// validateGoSourceAttributes requires the parsed attributes to be a mapping
// whose keys the claim lists. The host parses the comment; the owner never
// sees an attribute it did not declare.
func validateGoSourceAttributes(claim *protocol.GoSourceDirective, fragment *protocol.SourceFragment) error {
	value, ok := fragment.GetContent().(*protocol.SourceFragment_Value)
	if !ok || value.Value.GetMapping() == nil {
		return sourceError("Go directive attributes must be a mapping")
	}
	for _, entry := range value.Value.GetMapping().GetEntries() {
		if !slices.Contains(claim.GetAttributes(), entry.GetKey()) {
			return sourceError("Go directive attribute is outside its claim")
		}
	}
	return nil
}

func validateGoSourceBinding(document *protocol.SourceDocument, claim *protocol.GoSourceDirective, fragment *protocol.SourceFragment) error {
	subject := map[protocol.GoSourceDirective_Scope]string{
		protocol.GoSourceDirective_COLUMN: "column",
		protocol.GoSourceDirective_INDEX:  "index",
	}[claim.GetScope()]
	if claim.GetScope() == protocol.GoSourceDirective_DOCUMENT {
		if fragment.GetBindingId() != "" || fragment.GetSubject() != nil {
			return sourceError("document Go directive has a table attachment")
		}
		return nil
	}
	if (subject == "") != (fragment.GetSubject() == nil) || (subject != "" && fragment.GetSubject().GetKind() != subject) {
		return sourceError("Go directive subject does not match its scope")
	}
	for _, binding := range document.GetBindings() {
		if binding.GetId() == fragment.GetBindingId() && binding.GetObject().GetKind() == "table" {
			return nil
		}
	}
	return sourceError("Go directive has no common table binding")
}
