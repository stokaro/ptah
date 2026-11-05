package capability

import (
	"fmt"
	"maps"
	"slices"
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

// providedLines holds the capability facts of each release line an executable
// provider declared for an engine the process registry does not describe,
// keyed by the engine's name in lower case. A value is a map from line to
// facts.
var providedLines sync.Map

// RegisterReleaseLines records the capability facts of each release line of
// an engine Ptah does not describe, under each of names: the facts the
// engine's executable provider states for a server of the line with its
// default settings. [ProvidedReleaseLine] then resolves a server version of
// the engine to one of them.
//
// A name the process registry describes is refused, and so is a name already
// registered with other lines. Registering the same lines again changes
// nothing. A registration lasts for the life of the process.
func RegisterReleaseLines(lines map[string]Capabilities, names ...string) error {
	for line, caps := range lines {
		if strings.TrimSpace(line) == "" {
			return fmt.Errorf("capability: a release line needs a name")
		}
		if err := caps.Validate(); err != nil {
			return fmt.Errorf("capability: release line %s: %w", line, err)
		}
	}
	for _, name := range names {
		key := strings.ToLower(strings.TrimSpace(name))
		if key == "" {
			return fmt.Errorf("capability: release lines need an engine name")
		}
		if _, described := platform.Lookup(key); described {
			return fmt.Errorf("capability: %s is a dialect Ptah describes, whose ladder its descriptor names", key)
		}
		if existing, loaded := providedLines.LoadOrStore(key, maps.Clone(lines)); loaded {
			registered, ok := existing.(map[string]Capabilities)
			if !ok || !maps.EqualFunc(registered, lines, Capabilities.Equal) {
				return fmt.Errorf("capability: %s was registered with other release lines", key)
			}
		}
	}
	return nil
}

// ReleaseLineResolution is the release line a server version names among the
// lines an executable provider declared for its engine.
type ReleaseLineResolution struct {
	// Line is the line the version names, empty when it names none.
	Line string
	// Capabilities are the facts of Line.
	Capabilities Capabilities
	// Declared are the lines the provider declared, sorted.
	Declared []string
}

// Found reports whether the version named a declared line.
func (r ReleaseLineResolution) Found() bool { return r.Line != "" }

// ProvidedReleaseLine resolves version among the release lines
// [RegisterReleaseLines] recorded for dialect: the line equal to version, or
// the longest line version starts with followed by a dot, as 26.2.1.14 names
// 26.2. registered is false when dialect has no registered lines.
func ProvidedReleaseLine(dialect, version string) (resolution ReleaseLineResolution, registered bool) {
	entry, loaded := providedLines.Load(strings.ToLower(strings.TrimSpace(dialect)))
	if !loaded {
		return ReleaseLineResolution{}, false
	}
	lines, ok := entry.(map[string]Capabilities)
	if !ok {
		return ReleaseLineResolution{}, false
	}
	resolution.Declared = slices.Sorted(maps.Keys(lines))
	version = strings.TrimSpace(version)
	for _, candidate := range resolution.Declared {
		if (version == candidate || strings.HasPrefix(version, candidate+".")) && len(candidate) > len(resolution.Line) {
			resolution.Line = candidate
		}
	}
	resolution.Capabilities = lines[resolution.Line]
	return resolution, true
}
