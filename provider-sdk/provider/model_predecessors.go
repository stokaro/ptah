package provider

import (
	"encoding/hex"
	"fmt"
	"slices"
	"strings"
	"unicode"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

// ModelPredecessorsFeature enables Engine.model_predecessors: the identities
// an engine's models had before the engine owned them, such as those of a
// host's former built-in engine. A document that captured a claim under one
// of them, such as the ptah:feature-coverage header of an HCL export, makes
// the same claim of the engine's model. It is a two-way feature. A provider
// lists it, and so does each engine that declares a predecessor. Contract
// 25.25.
const ModelPredecessorsFeature = "models.predecessors"

// validateModelPredecessors checks an engine's model predecessors: present
// exactly when the engine lists the feature, which needs the provider's; each
// names a desired or observed model the engine declares and a complete former
// identity that is not one of the provider's own models; and no former
// identity is named twice in the engine.
func validateModelPredecessors(description *protocol.Description, engine *protocol.Engine) error {
	listed := slices.Contains(engine.GetFeatures(), ModelPredecessorsFeature)
	if listed && !slices.Contains(description.GetFeatures(), ModelPredecessorsFeature) {
		return predecessorError("an engine's model predecessors need the provider's " + ModelPredecessorsFeature)
	}
	if listed != (len(engine.GetModelPredecessors()) != 0) {
		return predecessorError("an engine declares model predecessors exactly when it lists " + ModelPredecessorsFeature)
	}
	type former struct {
		owner, kind    string
		representation protocol.ValueInput_Representation
		version        uint32
		digest         string
	}
	seen := make(map[former]bool)
	for _, predecessor := range engine.GetModelPredecessors() {
		representation := predecessor.GetRepresentation()
		if representation != protocol.ValueInput_DESIRED && representation != protocol.ValueInput_OBSERVED ||
			findValueModel(description, engine.GetId(), predecessor.GetKind(), representation) == nil {
			return predecessorError("a model predecessor names a desired or observed model the engine does not declare")
		}
		digest, err := hex.DecodeString(predecessor.GetDefinitionSha256())
		if !formerName(predecessor.GetFormerOwner()) || !formerName(predecessor.GetFormerKind()) || predecessor.GetSchemaVersion() == 0 ||
			err != nil || len(digest) != 32 || predecessor.GetDefinitionSha256() != strings.ToLower(predecessor.GetDefinitionSha256()) {
			return predecessorError("a model predecessor needs an owner, a kind, a schema version and a lowercase SHA-256 definition digest")
		}
		if predecessor.GetFormerOwner() == description.GetProviderId() &&
			findValueModel(description, engine.GetId(), predecessor.GetFormerKind(), representation) != nil {
			return predecessorError("a model predecessor is a model the provider declares")
		}
		key := former{owner: predecessor.GetFormerOwner(), kind: predecessor.GetFormerKind(), representation: representation,
			version: predecessor.GetSchemaVersion(), digest: predecessor.GetDefinitionSha256()}
		if seen[key] {
			return predecessorError("a former model identity is named twice")
		}
		seen[key] = true
	}
	return nil
}

// formerName reports a former owner or kind a captured claim can name:
// nonempty, with no white space and no control character.
func formerName(name string) bool {
	return name != "" && !strings.ContainsFunc(name, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) })
}

func predecessorError(reason string) error {
	return fmt.Errorf("%w: model predecessors: %s", ErrProtocol, reason)
}

// negotiateModelPredecessors removes the model predecessors unless the host
// lists ModelPredecessorsFeature.
func negotiateModelPredecessors(engine *protocol.Engine, hostFeatures []string) {
	if !slices.Contains(hostFeatures, ModelPredecessorsFeature) {
		engine.ModelPredecessors = nil
	}
}
