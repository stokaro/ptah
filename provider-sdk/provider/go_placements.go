package provider

import (
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

// GoPlacementsFeature enables the GO_ values of EncodedFragment.Placement: an
// encoded Go DOCUMENT directive says where it stands among the Go writer's own
// directives in the comment of the struct that holds a schema's standalone
// objects. A directive that names no placement stands after the views. It is a
// two-way feature. A provider lists it, and so does each engine that places
// its directives, beside EncodeSourceFeature. Contract 25.26.
const GoPlacementsFeature = "source.go-placements"

// validateGoPlacementsFeature requires an engine that lists the feature to
// have the provider's feature and source encoding.
func validateGoPlacementsFeature(description *protocol.Description, engine *protocol.Engine) error {
	if slices.Contains(engine.GetFeatures(), GoPlacementsFeature) &&
		(!slices.Contains(description.GetFeatures(), GoPlacementsFeature) || !slices.Contains(engine.GetFeatures(), EncodeSourceFeature)) {
		return encodeError("an engine lists Go placements without the provider's " + GoPlacementsFeature + " and source encoding")
	}
	return nil
}

// goPlacement reports a placement of a Go DOCUMENT directive.
func goPlacement(placement protocol.EncodedFragment_Placement) bool {
	switch placement {
	case protocol.EncodedFragment_GO_AFTER_SCHEMAS, protocol.EncodedFragment_GO_AFTER_VIEWS, protocol.EncodedFragment_GO_AFTER_PRIVILEGES:
		return true
	default:
		return false
	}
}

// validateGoPlacements refuses a Go placement in a reply to a host the engine
// did not negotiate the feature with.
func validateGoPlacements(engine *protocol.Engine, result *protocol.EncodedSource) error {
	if slices.Contains(engine.GetFeatures(), GoPlacementsFeature) {
		return nil
	}
	for _, part := range EncodedSourceParts(result) {
		if goPlacement(part.GetPlacement()) {
			return encodeError("a Go placement needs " + GoPlacementsFeature)
		}
	}
	return nil
}
