package providerhost

import (
	"fmt"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
)

// registerDefaultCapabilities registers the capabilities each engine's
// provider states it is planned with offline (Engine.default_capabilities),
// and those of each of its release lines (ReleaseLine.capabilities), under
// the engine's ID and aliases, so capability.ForDialect answers the defaults
// and a --server-version resolves to a line while the provider is selected:
// an offline render, generation or export of the engine plans as a built-in
// dialect plans with its presets. An engine the process registry describes
// keeps the presets its descriptor names. Facts the host's vocabulary does
// not hold refuse the provider.
func registerDefaultCapabilities(engines []*protocol.Engine) error {
	for _, engine := range engines {
		declared := engine.GetDefaultCapabilities()
		if len(declared) == 0 {
			continue
		}
		if _, described := platform.Lookup(engine.GetId()); described {
			continue
		}
		names := append([]string{engine.GetId()}, engine.GetAliases()...)
		if err := capability.RegisterDefaults(stated(declared), names...); err != nil {
			return fmt.Errorf("%w: engine %s states default capabilities the host cannot register: %w", provider.ErrProtocol, engine.GetId(), err)
		}
		lines := make(map[string]capability.Capabilities, len(engine.GetReleaseLines()))
		for _, line := range engine.GetReleaseLines() {
			lines[line.GetLine()] = stated(line.GetCapabilities())
		}
		if err := capability.RegisterReleaseLines(lines, names...); err != nil {
			return fmt.Errorf("%w: engine %s states release-line capabilities the host cannot register: %w", provider.ErrProtocol, engine.GetId(), err)
		}
	}
	return nil
}

// stated is the capability set a provider stated as a map of names.
func stated(declared map[string]bool) capability.Capabilities {
	flags := make(map[capability.Capability]bool, len(declared))
	for name, value := range declared {
		flags[capability.Capability(name)] = value
	}
	return capability.Of(flags)
}
