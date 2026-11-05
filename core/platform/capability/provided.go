package capability

import (
	"fmt"
	"strings"
	"sync"

	"ptah.run/core/platform"
)

// provided holds the offline capability facts an executable provider stated
// for an engine the process registry does not describe, keyed by the engine's
// name in lower case.
var provided sync.Map

// RegisterDefaults records caps as the capabilities an engine Ptah does not
// describe is planned with offline, under each of names: the facts its
// executable provider states for the release line a command assumes without a
// server. [ForDialect] then answers them for those names, so an offline
// render, generation or export plans with them, as it plans a built-in
// dialect with its default preset. A command that connects reads the
// session's capabilities instead.
//
// A name the process registry describes is refused, and so is a name already
// registered with other facts. Registering the same facts again changes
// nothing. A registration lasts for the life of the process.
func RegisterDefaults(caps Capabilities, names ...string) error {
	if err := caps.Validate(); err != nil {
		return fmt.Errorf("capability: default capabilities: %w", err)
	}
	for _, name := range names {
		key := strings.ToLower(strings.TrimSpace(name))
		if key == "" {
			return fmt.Errorf("capability: default capabilities need an engine name")
		}
		if _, described := platform.Lookup(key); described {
			return fmt.Errorf("capability: %s is a dialect Ptah describes, whose preset its descriptor names", key)
		}
		if existing, loaded := provided.LoadOrStore(key, caps); loaded {
			if registered, ok := existing.(Capabilities); !ok || !registered.Equal(caps) {
				return fmt.Errorf("capability: %s was registered with other default capabilities", key)
			}
		}
	}
	return nil
}

// providedDefaults returns the facts [RegisterDefaults] recorded for dialect,
// compared without regard to case or surrounding space. A dialect the process
// registry describes has none: its descriptor names its preset.
func providedDefaults(dialect string) (Capabilities, bool) {
	entry, found := provided.Load(strings.ToLower(strings.TrimSpace(dialect)))
	if !found {
		return Capabilities{}, false
	}
	if _, described := platform.Lookup(dialect); described {
		return Capabilities{}, false
	}
	registered, ok := entry.(Capabilities)
	return registered, ok
}
