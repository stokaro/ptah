package provider

import (
	"fmt"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

// DefaultCapabilitiesFeature enables Engine.default_capabilities and
// ReleaseLine.capabilities: the capability facts the host plans an engine
// with when no session states the server's, such as for an offline render,
// and those of each release line, for a --server-version that names one. The
// provider and each engine that states them list it, and so does the host.
// Contract 25.22.
const DefaultCapabilitiesFeature = "engine.default-capabilities"

// validateDefaultCapabilities checks an engine's offline capability facts: an
// engine states them, and the facts of each of its release lines, exactly when
// it lists DefaultCapabilitiesFeature, the provider lists the feature too, and
// each name is one a session may state.
func validateDefaultCapabilities(description *protocol.Description, engine *protocol.Engine) error {
	listed := slices.Contains(engine.GetFeatures(), DefaultCapabilitiesFeature)
	switch {
	case listed && !slices.Contains(description.GetFeatures(), DefaultCapabilitiesFeature):
		return fmt.Errorf("%w: engine %s requires the provider's", ErrProtocol, DefaultCapabilitiesFeature)
	case listed && len(engine.GetDefaultCapabilities()) == 0:
		return fmt.Errorf("%w: engine %s states no capability", ErrProtocol, DefaultCapabilitiesFeature)
	case !listed && len(engine.GetDefaultCapabilities()) != 0:
		return fmt.Errorf("%w: default capabilities require engine %s", ErrProtocol, DefaultCapabilitiesFeature)
	}
	for name := range engine.GetDefaultCapabilities() {
		if !comparisonReason(name) {
			return fmt.Errorf("%w: invalid default capability name", ErrProtocol)
		}
	}
	for _, line := range engine.GetReleaseLines() {
		switch {
		case listed && len(line.GetCapabilities()) == 0:
			return fmt.Errorf("%w: release line %s states no capability", ErrProtocol, line.GetLine())
		case !listed && len(line.GetCapabilities()) != 0:
			return fmt.Errorf("%w: release line %s capabilities require engine %s", ErrProtocol, line.GetLine(), DefaultCapabilitiesFeature)
		}
		for name := range line.GetCapabilities() {
			if !comparisonReason(name) {
				return fmt.Errorf("%w: invalid capability name on release line %s", ErrProtocol, line.GetLine())
			}
		}
	}
	return nil
}
