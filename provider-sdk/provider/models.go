package provider

import (
	"encoding/hex"
	"fmt"
	"strings"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

func validateValueModels(models []*protocol.ValueModel) error {
	type identity struct {
		kind           string
		representation protocol.ValueInput_Representation
	}
	seen := make(map[identity]bool)
	for _, model := range models {
		if model == nil || strings.TrimSpace(model.GetKind()) == "" || model.GetSchemaVersion() == 0 ||
			strings.TrimSpace(model.GetEncoding()) == "" || strings.TrimSpace(model.GetCanonicalFormat()) == "" {
			return fmt.Errorf("%w: incomplete value model definition", ErrProtocol)
		}
		switch model.GetRepresentation() {
		case protocol.ValueInput_DESIRED, protocol.ValueInput_OBSERVED, protocol.ValueInput_OPERATION, protocol.ValueInput_CHANGE:
		default:
			return fmt.Errorf("%w: unknown model representation", ErrProtocol)
		}
		if err := validateModelSourceFormats(model); err != nil {
			return err
		}
		digest, err := hex.DecodeString(model.GetDefinitionSha256())
		if err != nil || len(digest) != 32 || model.GetDefinitionSha256() != strings.ToLower(model.GetDefinitionSha256()) {
			return fmt.Errorf("%w: model definition requires a lowercase SHA-256 digest", ErrProtocol)
		}
		key := identity{kind: model.GetKind(), representation: model.GetRepresentation()}
		if seen[key] {
			return fmt.Errorf("%w: duplicate model representation", ErrProtocol)
		}
		seen[key] = true
	}
	return nil
}

func findValueModel(description *protocol.Description, target, kind string, representation protocol.ValueInput_Representation) *protocol.ValueModel {
	for _, engine := range description.GetEngines() {
		if engine.GetId() != target {
			continue
		}
		for _, model := range engine.GetValueModels() {
			if model.GetKind() == kind && model.GetRepresentation() == representation {
				return model
			}
		}
	}
	return nil
}

func envelopeMatchesModel(value *protocol.Envelope, model *protocol.ValueModel) bool {
	return value != nil && model != nil && value.GetKind() == model.GetKind() && value.GetSchemaVersion() == model.GetSchemaVersion() &&
		value.GetEncoding() == model.GetEncoding() && value.GetDefinitionSha256() == model.GetDefinitionSha256()
}

func validateModelSourceFormats(model *protocol.ValueModel) error {
	if model.GetRepresentation() != protocol.ValueInput_DESIRED && len(model.GetSourceFormats()) != 0 {
		return fmt.Errorf("%w: only desired models declare source formats", ErrProtocol)
	}
	seen := make(map[protocol.SourceDocument_Format]bool)
	for _, format := range model.GetSourceFormats() {
		switch format {
		case protocol.SourceDocument_SQL, protocol.SourceDocument_YAML, protocol.SourceDocument_HCL, protocol.SourceDocument_GO_ANNOTATIONS:
		default:
			return fmt.Errorf("%w: unknown model source format", ErrProtocol)
		}
		if seen[format] {
			return fmt.Errorf("%w: duplicate model source format", ErrProtocol)
		}
		seen[format] = true
	}
	return nil
}
