package providerhost

import (
	"fmt"
	"strings"
	"sync"

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

// sessionPlanned holds, by engine ID in lower case, the engines this process
// plans with the capabilities of their first session.
var sessionPlanned sync.Map

// planWithSessionCapabilities registers caps, the capabilities a session of
// t's engine states, as the facts capability.ForDialect answers for the
// engine's ID and aliases, when its provider states none to plan it with
// (Engine.default_capabilities) and the process registry does not describe
// it. A provider that does not negotiate engine.default-capabilities then
// plans a command that connects as it planned with the engine's built-in
// preset: the conversion of a source, which reads capability.ForDialect,
// sees the server's facts rather than none, and the provider does not refuse
// a Serial column as unknown. The first session of an engine that states
// any capability decides for the life of the process, as a built-in
// dialect's preset does whatever server a command connects to. An offline
// command opens no session and plans against unknown facts.
func (t *target) planWithSessionCapabilities(caps capability.Capabilities) error {
	if len(t.engine.GetDefaultCapabilities()) != 0 || caps.Len() == 0 {
		return nil
	}
	if _, described := platform.Lookup(t.engine.GetId()); described {
		return nil
	}
	if capability.ForDialect(t.engine.GetId()).Len() != 0 {
		return nil
	}
	if _, decided := sessionPlanned.LoadOrStore(strings.ToLower(t.engine.GetId()), struct{}{}); decided {
		return nil
	}
	names := append([]string{t.engine.GetId()}, t.engine.GetAliases()...)
	if err := capability.RegisterDefaults(caps, names...); err != nil {
		return fmt.Errorf("engine %s: plan with the session's capabilities: %w", t.engine.GetId(), err)
	}
	return nil
}
