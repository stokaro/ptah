// Package capabilitysource holds the line an executable provider states for
// each engine it serves about where the engine's connection reads capabilities
// that differ from its release line (contract 25.35,
// engine.connection-capability-source). internal/providerhost declares it while
// the provider is selected, and ptah db capabilities prints it beneath a
// refined capability, so the host names no engine to word it.
package capabilitysource

import (
	"fmt"
	"strings"
	"sync"

	"ptah.run/core/platform"
)

// declared holds each engine's line, keyed by the engine's name in lower case.
// A value is a *declaration.
var declared sync.Map

type declaration struct{ source string }

// Declare registers source as where the connection of the engine named by
// each of names reads refined capabilities, and returns the function that ends
// the registration. A name the process registry describes is refused, since a
// built-in dialect's sentence is the host's, and so is an empty line. A name
// another registration holds with the same line keeps that registration, which
// its own release ends; one it holds with another line refuses the whole
// declaration. The release is safe to call more than once.
func Declare(source string, names ...string) (func(), error) {
	if strings.TrimSpace(source) == "" {
		return nil, fmt.Errorf("capabilitysource: an engine states no source line")
	}
	keys := make([]string, 0, len(names))
	for _, name := range names {
		key := strings.ToLower(strings.TrimSpace(name))
		if key == "" {
			return nil, fmt.Errorf("capabilitysource: a source line needs an engine name")
		}
		if _, described := platform.Lookup(key); described {
			return nil, fmt.Errorf("capabilitysource: %s is a dialect Ptah describes, whose capability source the host words", key)
		}
		keys = append(keys, key)
	}
	entry := &declaration{source: source}
	var stored []string
	release := func() {
		for _, key := range stored {
			declared.CompareAndDelete(key, entry)
		}
	}
	for _, key := range keys {
		existing, loaded := declared.LoadOrStore(key, entry)
		if !loaded {
			stored = append(stored, key)
			continue
		}
		if existing.(*declaration).source != source {
			release()
			return nil, fmt.Errorf("capabilitysource: %s is registered with another source line", key)
		}
	}
	return release, nil
}

// Lookup returns the line a provider declared for dialect, compared without
// regard to case or surrounding space, and whether one is declared.
func Lookup(dialect string) (string, bool) {
	entry, found := declared.Load(strings.ToLower(strings.TrimSpace(dialect)))
	if !found {
		return "", false
	}
	return entry.(*declaration).source, true
}
