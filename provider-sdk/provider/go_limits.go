package provider

import (
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

// GoLimitsFeature enables claimed ptah:not-described headers in Go sources.
// Both peers negotiate the kinds the owner reads and writes as coverage limits.
const GoLimitsFeature = "source.go-limits"

func validateGoLimitsEngine(description *protocol.Description, engine *protocol.Engine) error {
	listed := slices.Contains(engine.GetFeatures(), GoLimitsFeature)
	if listed != (len(engine.GetGoHeaderKinds()) != 0) || listed &&
		(!slices.Contains(description.GetFeatures(), GoLimitsFeature) || !slices.Contains(description.GetFeatures(), DecodeSourceFeature)) {
		return sourceError("Go header kinds require negotiated Go limits and source decoding")
	}
	for i, kind := range engine.GetGoHeaderKinds() {
		if !validName(kind) || slices.Contains(engine.GetGoHeaderKinds()[:i], kind) {
			return sourceError("a Go header kind must be a unique lower-case name")
		}
	}
	return nil
}

func validateGoSourceLimits(description *protocol.Description, engine string, document *protocol.SourceDocument) error {
	if document.GetFormat() != protocol.SourceDocument_GO_ANNOTATIONS {
		return nil
	}
	var kinds []string
	for _, target := range description.GetEngines() {
		if target.GetId() == engine {
			kinds = target.GetGoHeaderKinds()
		}
	}
	return validateGoLimits(kinds, document.GetLimits())
}

func validateGoLimits(kinds []string, limits []*protocol.SchemaCoverageLimit) error {
	seen := make(map[[2]string]bool)
	for _, limit := range limits {
		key := [2]string{limit.GetKind(), limit.GetName()}
		if !slices.Contains(kinds, key[0]) || seen[key] {
			return sourceError("a Go limit names a declared header kind and unique subject")
		}
		seen[key] = true
	}
	return nil
}
