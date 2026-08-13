package sqllint

import (
	"fmt"

	"go.5x5.cz/ptah/core/platform/capability"
)

// RecognizedVersionShapes describes, in one sentence a person can act on, the
// version strings the capability resolver can identify. It is a constant
// because two callers report it — the linter, when a library caller hands it
// an unusable Options.Version, and the command, when an operator types one —
// and a wording that drifts between those two is a wording nobody can trust.
const RecognizedVersionShapes = `a dotted server version such as "17" or "8.0.42", ` +
	`or a server banner such as "PostgreSQL 16.3 (Debian)", "10.11.6-MariaDB", or "CockroachDB CCL v25.4.0"`

// UnrecognizedVersionError reports a target version string the capability
// resolver could not identify as any server at all.
//
// It is an error rather than a quiet fallback because the string came from a
// person. capability.ForServerVersion answers an unreadable string with the
// dialect's default preset and says nothing, which is the right behavior for
// a banner read from a live SELECT version() — a server does not typo its own
// name — and the wrong behavior for a value typed on a command line, where
// the silent answer is a lint result attributed to a version that never
// resolved.
type UnrecognizedVersionError struct {
	// Dialect is the normalized dialect the version was resolved against.
	Dialect string
	// Version is the string that named no server.
	Version string
}

func (e *UnrecognizedVersionError) Error() string {
	return fmt.Sprintf("%q is not a recognized %s server version: expected %s",
		e.Version, e.Dialect, RecognizedVersionShapes)
}

// TargetVersion is the outcome of applying an operator-supplied server
// version to a dialect.
type TargetVersion struct {
	// Capabilities is the preset the linter plans with.
	Capabilities capability.Capabilities
	// Note is empty when the version selected an exact measured release line,
	// and otherwise says what was planned instead. Silence is reserved for
	// that clean match: every other outcome resolved to something the
	// operator did not name, and saying nothing there is how a run against an
	// unmodeled server reads as a run against the server they asked for.
	Note string
}

// ResolveTargetVersion maps a dialect and an operator-supplied version onto
// the capability preset the linter should plan with.
//
// An empty version is not a mistake — it means the caller wants the dialect
// default — so it resolves silently to ForDialect. A non-empty version that
// names no server is refused with an *UnrecognizedVersionError.
func ResolveTargetVersion(dialect, version string) (TargetVersion, error) {
	if version == "" {
		return TargetVersion{Capabilities: capability.ForDialect(dialect)}, nil
	}

	resolution := capability.ResolveServerVersion(dialect, version)
	if !resolution.Recognized {
		return TargetVersion{}, &UnrecognizedVersionError{Dialect: dialect, Version: version}
	}
	return TargetVersion{
		Capabilities: resolution.Capabilities,
		Note:         versionNote(dialect, version, resolution),
	}, nil
}

// versionNote renders the one line that says what a non-version-specific
// resolution actually planned. The three arms are the three ways a recognized
// version can fail to select a measured release line, and they are separate
// sentences because the remedies differ: wait for the newer line to be
// measured, read the preset a gap falls back to, or stop passing a version to
// a dialect that has no ladder to spend it on.
func versionNote(dialect, version string, resolution capability.VersionResolution) string {
	switch {
	case resolution.VersionSpecific:
		return ""
	case resolution.Saturated:
		return fmt.Sprintf(
			"%s %s is newer than the newest measured release line %s; capabilities were planned as %s",
			dialect, version, resolution.NewestMeasured, resolution.NewestMeasured)
	case resolution.NewestMeasured != "":
		return fmt.Sprintf(
			"%s %s is not a measured release line; capabilities fall back to the preset its ladder assigns "+
				"(newest measured line: %s)",
			dialect, version, resolution.NewestMeasured)
	default:
		return fmt.Sprintf(
			"the %s dialect has no measured version ladder; the version did not refine capabilities",
			dialect)
	}
}
