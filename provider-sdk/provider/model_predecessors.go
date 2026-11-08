package provider

import (
	"encoding/hex"
	"fmt"
	"slices"
	"strings"
	"unicode"

	protocol "ptah.run/provider-sdk/protocol/experimental"
)

// ModelPredecessorsFeature enables Engine.model_predecessors: the identities
// an engine's models had before the engine owned them, such as those of a
// host's former built-in engine. A document that captured a claim under one
// of them, such as the ptah:feature-coverage header of an HCL export, makes
// the same claim of the engine's model. It is a two-way feature. A provider
// lists it, and so does each engine that declares a predecessor. Contract
// 25.25.
const ModelPredecessorsFeature = "models.predecessors"

// OwnPredecessorsFeature lets a model predecessor name an earlier identity of
// the same model: the provider as the former owner and the model's own kind
// as the former kind, with another schema version or definition digest. A
// host names a provider's model in a captured claim by the identity the
// provider owns, so a definition text that changes moves the identity every
// file exported before names; the provider names the identity it replaced,
// and such a file keeps reading. It is a two-way feature beside
// ModelPredecessorsFeature. The provider lists it, and so does each engine
// that declares such a predecessor; a host that does not list it receives
// none of them. Contract 25.27.
const OwnPredecessorsFeature = "models.own-predecessors"

// validateModelPredecessors checks an engine's model predecessors: present
// exactly when the engine lists the feature, which needs the provider's; each
// names a desired or observed model the engine declares and a complete former
// identity that is none of the provider's current models, and that names the
// provider and another of its kinds only as an earlier identity of the same
// model, under OwnPredecessorsFeature; and no former identity is named twice
// in the engine.
func validateModelPredecessors(description *protocol.Description, engine *protocol.Engine) error {
	listed := slices.Contains(engine.GetFeatures(), ModelPredecessorsFeature)
	if listed && !slices.Contains(description.GetFeatures(), ModelPredecessorsFeature) {
		return predecessorError("an engine's model predecessors need the provider's " + ModelPredecessorsFeature)
	}
	own := slices.Contains(engine.GetFeatures(), OwnPredecessorsFeature)
	if own && !slices.Contains(description.GetFeatures(), OwnPredecessorsFeature) {
		return predecessorError("an engine's own model predecessors need the provider's " + OwnPredecessorsFeature)
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
		if err := validateOwnPredecessor(description, engine, predecessor, own); err != nil {
			return err
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

// validateOwnPredecessor refuses a predecessor that names a model the
// provider declares now: another model of the engine, the model's current
// identity, or an earlier identity of the model without
// OwnPredecessorsFeature. A former kind of the provider that the engine no
// longer declares is a former identity like any other owner's.
func validateOwnPredecessor(description *protocol.Description, engine *protocol.Engine, predecessor *protocol.ModelPredecessor, own bool) error {
	if predecessor.GetFormerOwner() != description.GetProviderId() {
		return nil
	}
	current := findValueModel(description, engine.GetId(), predecessor.GetFormerKind(), predecessor.GetRepresentation())
	switch {
	case current == nil:
		return nil
	case predecessor.GetFormerKind() != predecessor.GetKind():
		return predecessorError("a model predecessor is another model the provider declares")
	case !own:
		return predecessorError("an earlier identity of the model itself needs " + OwnPredecessorsFeature)
	case predecessor.GetSchemaVersion() == current.GetSchemaVersion() && predecessor.GetDefinitionSha256() == current.GetDefinitionSha256():
		return predecessorError("a model predecessor is the model's current identity")
	}
	return nil
}

// ownPredecessor reports whether predecessor names an earlier identity of its
// own model: the provider as the former owner and the model's kind.
func ownPredecessor(providerID string, predecessor *protocol.ModelPredecessor) bool {
	return predecessor.GetFormerOwner() == providerID && predecessor.GetFormerKind() == predecessor.GetKind()
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
// lists ModelPredecessorsFeature, and those that name an earlier identity of
// their own model unless it lists OwnPredecessorsFeature too. An engine left
// with no predecessor no longer lists ModelPredecessorsFeature, since it
// declares predecessors exactly when it lists the feature.
func negotiateModelPredecessors(providerID string, engine *protocol.Engine, hostFeatures []string) {
	if !slices.Contains(hostFeatures, ModelPredecessorsFeature) {
		engine.ModelPredecessors = nil
		return
	}
	if slices.Contains(hostFeatures, OwnPredecessorsFeature) {
		return
	}
	engine.ModelPredecessors = slices.DeleteFunc(engine.ModelPredecessors, func(predecessor *protocol.ModelPredecessor) bool {
		return ownPredecessor(providerID, predecessor)
	})
	if len(engine.ModelPredecessors) == 0 {
		engine.ModelPredecessors = nil
		engine.Features = slices.DeleteFunc(engine.Features, func(feature string) bool { return feature == ModelPredecessorsFeature })
	}
}
