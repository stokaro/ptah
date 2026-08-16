// Package servertarget maps an operator-supplied server version onto the
// capability preset an offline command should plan with. It sits below the CLI
// layer because more than one command takes that string — `ptah sql lint` reads
// it as --version and `ptah schema render` as --server-version — and a second
// derivation of the same contract is how two surfaces come to disagree about
// which versions exist.
package servertarget

import (
	"fmt"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
)

// RecognizedVersionShapes describes, in one sentence a person can act on, the
// version strings the capability resolver can identify. It is a constant
// because several callers report it — the SQL linter, when a library caller
// hands it an unusable version; and each command that lets an operator type
// one — and a wording that drifts between those is a wording nobody can trust.
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
// the silent answer is a result attributed to a version that never resolved.
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

// DialectMismatchError reports a version string that names a different server
// product than the dialect it was supplied with.
//
// A live connection resolves such a contradiction in favor of the string:
// MariaDB announces itself over the MySQL protocol and CockroachDB over the
// PostgreSQL one, so the banner is better evidence than the driver name.
// Applied to two values a person typed it is a silent contradiction instead.
// Measured before this refusal existed, `ptah schema render --dialect mysql
// --server-version 10.11.6-MariaDB` exited 0 rendering MySQL DDL against
// MariaDB capabilities, where the same command without a version exited 2 — so
// the flag could relax a refusal by naming a server the render was never going
// to target.
//
// The question asked here is capability.BannerPlatform: which product does the
// string announce? It is deliberately not
// capability.VersionResolution.ResolvedDialect, which answers which ladder the
// capabilities came from. The two differ for a PostgreSQL banner on a
// PostgreSQL-family dialect: `--dialect cockroachdb --server-version
// 'PostgreSQL 16.3'` resolves on the CockroachDB ladder, because a live
// CockroachDB may report a banner carrying no product token of its own and
// must keep its own preset — while the same pair typed on a command line is
// still a contradiction and is refused here.
type DialectMismatchError struct {
	// Dialect is the normalized dialect the caller asked for.
	Dialect string
	// ResolvedDialect is the platform the version string named instead.
	ResolvedDialect string
	// Version is the string that named it.
	Version string
}

func (e *DialectMismatchError) Error() string {
	return fmt.Sprintf("%q names a %s server, but the target dialect is %s",
		e.Version, e.ResolvedDialect, e.Dialect)
}

// Target is the outcome of applying an operator-supplied server version to a
// dialect.
type Target struct {
	// Capabilities is the preset the caller plans with.
	Capabilities capability.Capabilities
	// Note is empty when the version selected an exact measured release line,
	// and otherwise says what was planned instead. Silence is reserved for
	// that clean match: every other outcome resolved to something the
	// operator did not name, and saying nothing there is how a run against an
	// unmodeled server reads as a run against the server they asked for.
	Note string
}

// Resolve maps a dialect and an operator-supplied version onto the capability
// preset the caller should plan with.
//
// An empty version is not a mistake — it means the caller wants the dialect
// default — so it resolves silently to ForDialect. A non-empty version that
// names no server is refused with an *UnrecognizedVersionError, and one that
// names a different server product than the dialect with a
// *DialectMismatchError.
func Resolve(dialect, version string) (Target, error) {
	if version == "" {
		return Target{Capabilities: capability.ForDialect(dialect)}, nil
	}

	normalized := platform.NormalizeDialect(dialect)
	resolution := capability.ResolveServerVersion(dialect, version)
	if !resolution.Recognized {
		return Target{}, &UnrecognizedVersionError{Dialect: dialect, Version: version}
	}
	if named := capability.BannerPlatform(version); named != "" && named != normalized {
		return Target{}, &DialectMismatchError{
			Dialect:         normalized,
			ResolvedDialect: named,
			Version:         version,
		}
	}
	return Target{
		Capabilities: resolution.Capabilities,
		Note:         VersionNote(dialect, version, resolution),
	}, nil
}

// VersionNote renders the one line that says what a non-version-specific
// resolution actually planned. The three arms are the three ways a recognized
// version can fail to select a measured release line, and they are separate
// sentences because the remedies differ: wait for the newer line to be
// measured, read the preset a gap falls back to, or stop passing a version to
// a dialect that has no ladder to spend it on.
//
// It is exported because a live connection asks the same question a typed
// --server-version does. Only the refusal differs — a banner a server wrote is
// not a typo — so the resolution is interpreted here once and refused only in
// [Resolve]. A second wording for the live path is a second answer to "what
// was actually planned", and the two would drift on the first edit.
func VersionNote(dialect, version string, resolution capability.VersionResolution) string {
	switch {
	case resolution.VersionSpecific:
		return ""
	case !resolution.Recognized:
		// Reached only from the live path: Resolve refuses an unrecognized
		// value before it gets here. Without this arm the default below fires,
		// because an unreadable string on a laddered dialect reports an empty
		// NewestMeasured exactly as a dialect with no ladder does — so an
		// unreadable PostgreSQL banner was described as "the postgres dialect
		// has no measured version ladder", which is false about PostgreSQL and
		// silent about the banner.
		return fmt.Sprintf(
			"%q does not name a %s server version Ptah can read; capabilities fall back to the dialect default",
			version, dialect)
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
