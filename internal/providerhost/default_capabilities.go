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
// under the engine's ID and aliases, so capability.ForDialect answers them
// while the provider is selected: an offline render, generation or export of
// the engine plans as a built-in dialect plans with its default preset. An
// engine the process registry describes keeps the preset its descriptor
// names. Facts the host's vocabulary does not hold refuse the provider.
func registerDefaultCapabilities(engines []*protocol.Engine) error {
	for _, engine := range engines {
		declared := engine.GetDefaultCapabilities()
		if len(declared) == 0 {
			continue
		}
		if _, described := platform.Lookup(engine.GetId()); described {
			continue
		}
		flags := make(map[capability.Capability]bool, len(declared))
		for name, value := range declared {
			flags[capability.Capability(name)] = value
		}
		if err := capability.RegisterDefaults(capability.Of(flags), append([]string{engine.GetId()}, engine.GetAliases()...)...); err != nil {
			return fmt.Errorf("%w: engine %s states default capabilities the host cannot register: %w", provider.ErrProtocol, engine.GetId(), err)
		}
	}
	return nil
}
