package capability

import (
	"cmp"
	"fmt"
	"maps"
	"slices"
	"strconv"
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
// lines an executable provider declared for its engine, and the line it is
// planned with.
type ReleaseLineResolution struct {
	// Line is the line the version names, empty when it names none.
	Line string
	// Planned is the line whose facts the version is planned with: Line, or,
	// for a version that names no line, the line the declared lines place it
	// on, as a built-in dialect's version ladder places a version between or
	// beyond its measured lines. Empty when the version is not a release, or
	// when the declared lines are not all releases and so form no ladder.
	Planned string
	// Saturated reports a version newer than every declared line, planned
	// with the newest one.
	Saturated bool
	// Newest is the newest declared line, empty when the declared lines form
	// no ladder.
	Newest string
	// Capabilities are the facts of Planned.
	Capabilities Capabilities
	// Declared are the lines the provider declared, sorted.
	Declared []string
}

// Found reports whether the version named a declared line.
func (r ReleaseLineResolution) Found() bool { return r.Line != "" }

// ProvidedReleaseLine resolves version among the release lines
// [RegisterReleaseLines] recorded for dialect. registered is false when
// dialect has no registered lines.
//
// The version names the line equal to it, or the longest line it starts with
// followed by a dot, as 26.2.1.14 names 26.2. When every declared line is a
// release, written <major> or <major>.<minor>, the lines also form a ladder,
// and a version that is a release names the line of its major and minor and
// is planned with the line it falls on: the newest line at or below it, the
// newest line when it is newer than all of them (Saturated), and the oldest
// when it is older. A version is a release when it opens with a dotted number,
// "26.3.1.18", or is a build branch, stable-<major>-<minor>[-<patch>], as
// "stable-25-4-1" names 25.4.
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
	resolution.Planned = resolution.Line
	if steps, ok := releaseLadder(resolution.Declared); ok {
		resolution.Newest = steps[len(steps)-1].line
		if parsed, ok := parseRelease(version); ok && resolution.Line == "" {
			resolution.Line, resolution.Planned, resolution.Saturated = steps.place(parsed)
		}
	}
	resolution.Capabilities = lines[resolution.Planned]
	return resolution, true
}

// providedResolution answers [ResolveServerVersion] for an engine whose
// executable provider declared release lines, as a built-in dialect's version
// ladder answers it. registered is false for an engine with none.
func providedResolution(dialect, version string) (resolution VersionResolution, registered bool) {
	line, registered := ProvidedReleaseLine(dialect, version)
	if !registered {
		return VersionResolution{}, false
	}
	name := strings.ToLower(strings.TrimSpace(dialect))
	if line.Planned == "" {
		// Recognized stays false: the defaults are picked without reading
		// anything out of the string.
		return VersionResolution{Capabilities: ForDialect(dialect), NewestMeasured: line.Newest, ResolvedDialect: name}, true
	}
	return VersionResolution{
		Capabilities:    line.Capabilities,
		VersionSpecific: line.Found(),
		Saturated:       line.Saturated,
		Recognized:      true,
		NewestMeasured:  line.Newest,
		ResolvedDialect: name,
	}, true
}

// release is a major and minor release, and the declared line that spells it.
type release struct {
	major, minor int
	line         string
}

// compare orders two releases by major, then minor.
func (r release) compare(other release) int {
	return cmp.Or(cmp.Compare(r.major, other.major), cmp.Compare(r.minor, other.minor))
}

// ladder is a set of declared lines that are all releases, oldest first.
type ladder []release

// releaseLadder reads every declared line as a release and orders them, or
// reports that one is not a release, in which case the lines form no ladder.
func releaseLadder(declared []string) (ladder, bool) {
	result := make(ladder, 0, len(declared))
	for _, line := range declared {
		major, minor, ok := releaseNumbers(line, ".")
		if !ok || line != strings.TrimSpace(line) {
			return nil, false
		}
		result = append(result, release{major: major, minor: minor, line: line})
	}
	if len(result) == 0 {
		return nil, false
	}
	slices.SortFunc(result, release.compare)
	return result, true
}

// place returns the line version names, the line it is planned with, and
// whether it is newer than every line.
func (l ladder) place(version release) (named, planned string, saturated bool) {
	newest := l[len(l)-1]
	if version.compare(newest) > 0 {
		return "", newest.line, true
	}
	planned = l[0].line
	for _, step := range l {
		if step.compare(version) > 0 {
			break
		}
		planned = step.line
		if step.compare(version) == 0 {
			named = step.line
		}
	}
	return named, planned, false
}

// parseRelease reads the major and minor release out of a server version
// that is a release: one that opens with a dotted number, or a build branch,
// stable-<major>-<minor>[-<patch>]. A floating build such as "trunk" is
// neither.
func parseRelease(version string) (release, bool) {
	if branch, found := strings.CutPrefix(strings.ToLower(version), "stable-"); found {
		major, minor, ok := releaseNumbers(branch, "-")
		return release{major: major, minor: minor}, ok && strings.Count(branch, "-") >= 1
	}
	end := 0
	for end < len(version) && (version[end] >= '0' && version[end] <= '9' || version[end] == '.') {
		end++
	}
	major, minor, ok := releaseNumbers(strings.TrimSuffix(version[:end], "."), ".")
	return release{major: major, minor: minor}, ok
}

// releaseNumbers reads the first two numbers of text, separated by separator.
// A missing minor reads as 0 when text holds only a major; any other part
// that is not a number refuses the whole text.
func releaseNumbers(text, separator string) (major, minor int, ok bool) {
	parts := strings.Split(text, separator)
	if len(parts) == 0 || parts[0] == "" {
		return 0, 0, false
	}
	numbers := make([]int, 0, 2)
	for _, part := range parts[:min(len(parts), 2)] {
		number, err := strconv.Atoi(part)
		if err != nil || number < 0 || part != strconv.Itoa(number) {
			return 0, 0, false
		}
		numbers = append(numbers, number)
	}
	for _, part := range parts[min(len(parts), 2):] {
		if _, err := strconv.Atoi(part); err != nil {
			return 0, 0, false
		}
	}
	if len(numbers) == 1 {
		return numbers[0], 0, true
	}
	return numbers[0], numbers[1], true
}
