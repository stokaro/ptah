package provider

import (
	"fmt"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

// ConnectionCapabilitySourceFeature has an engine state, in
// Engine.connection_capability_source, where its connection reads capabilities
// that differ from its release line's: the one line a host prints beneath
// them, so an operator knows what to change to change them. A host that does
// not list it prints its own sentence for a provider's connection, as a host
// of contract 25.34 does. It is a two-way feature: the provider lists it, and
// so does each engine that states the line. Contract 25.35.
const ConnectionCapabilitySourceFeature = "engine.connection-capability-source"

// validateConnectionCapabilitySource checks an engine's capability source: the
// engine states it exactly when it lists ConnectionCapabilitySourceFeature, the
// provider lists the feature too, and the line is one line a host can print.
func validateConnectionCapabilitySource(description *protocol.Description, engine *protocol.Engine) error {
	listed := slices.Contains(engine.GetFeatures(), ConnectionCapabilitySourceFeature)
	source := engine.GetConnectionCapabilitySource()
	switch {
	case listed && !slices.Contains(description.GetFeatures(), ConnectionCapabilitySourceFeature):
		return fmt.Errorf("%w: engine %s requires the provider's", ErrProtocol, ConnectionCapabilitySourceFeature)
	case listed != (source != ""):
		return fmt.Errorf("%w: engine %s states a connection capability source exactly when it lists %s", ErrProtocol, engine.GetId(),
			ConnectionCapabilitySourceFeature)
	case source != "" && (!comparisonReason(source) || len(source) > maxQuerySpelling):
		return fmt.Errorf("%w: the connection capability source of engine %s is not one line of text", ErrProtocol, engine.GetId())
	}
	return nil
}
