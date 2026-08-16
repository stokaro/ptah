package capability

import "slices"

// SupportLevel names what Ptah promises about one database release line.
//
// It answers a different question from the capability set beside it, and
// keeping the two apart is the whole point (stokaro/ptah#1230):
//
//   - a support level answers "has Ptah regularly tested this release line",
//     which is a statement about this repository's continuous integration;
//   - a capability answers "can Ptah safely perform this operation against
//     this server", which is a statement about the server in front of it.
//
// Conflating them turns a vendor's end-of-life date into a runtime refusal.
// Ptah does not do that. Upstream end-of-life lowers the testing guarantee
// Ptah offers for a release line; it is not by itself a reason to refuse to
// work with a server, and no code in this repository consults a support level
// to decide whether an operation may proceed.
//
// A hard refusal is reserved for [KnownIncompatible], which requires a
// concrete technical incompatibility rather than a calendar date.
type SupportLevel string

const (
	// Certified is a release line Ptah regularly tests in continuous
	// integration, and for which Ptah makes a compatibility commitment over
	// the tested feature surface. The declaration lives with the release line
	// in internal/capabilityprobe.
	Certified SupportLevel = "certified"

	// LegacyTested is an upstream end-of-life release line Ptah deliberately
	// keeps measuring as a regression sentinel, because it remains common in
	// real deployments or marks a compatibility boundary worth defending. It
	// carries the same runtime behavior as [Certified] and a weaker promise:
	// the line is retained on purpose, not covered by the same guarantee.
	LegacyTested SupportLevel = "legacy-tested"

	// BestEffort is a release line Ptah does not test and does not reject.
	// Capabilities are resolved for it exactly as for any other server, and
	// the operations those capabilities allow are performed.
	//
	// It is the only level that is never declared. A declared level names a
	// line somebody chose to cover; this one is what remains when a live
	// server's version falls on no declared line at all, so it is resolved at
	// runtime and cannot be written down in advance.
	BestEffort SupportLevel = "best-effort"

	// KnownIncompatible is a release line with a concrete, named technical
	// incompatibility: required catalog metadata cannot be obtained, the
	// server lacks a primitive Ptah's schema model needs, or introspection
	// cannot be made correct there.
	//
	// A vendor ending support for a release is NOT such a reason. No release
	// line carries this level today, which is a fact about what has been
	// found rather than about what the level means.
	KnownIncompatible SupportLevel = "known-incompatible"
)

// supportLevels is the closed set, in the order documentation and command
// output present it: strongest promise first, then the two levels that are
// resolved rather than promised.
var supportLevels = []SupportLevel{Certified, LegacyTested, BestEffort, KnownIncompatible}

// SupportLevels returns every level, strongest promise first.
func SupportLevels() []SupportLevel {
	return slices.Clone(supportLevels)
}

// Valid reports whether the level is one this package defines. The zero value
// is not valid: a line whose level was never assigned must not read as one
// Ptah promises anything about.
func (l SupportLevel) Valid() bool {
	return slices.Contains(supportLevels, l)
}

// Doc returns the one-sentence meaning of the level, or the empty string for a
// level this package does not define. Command output and the generated support
// matrix both render it, so the wording lives here rather than in each of them.
func (l SupportLevel) Doc() string {
	switch l {
	case Certified:
		return "Ptah regularly tests this release line in CI and commits to the tested feature surface."
	case LegacyTested:
		return "Upstream end-of-life, retained on purpose as a regression sentinel; tested less exhaustively than a certified line."
	case BestEffort:
		return "Not regularly tested and not rejected: capabilities are resolved and the operations they allow are performed."
	case KnownIncompatible:
		return "A concrete technical incompatibility is known; the release is not usable, for a stated reason that is not a vendor EOL date."
	default:
		return ""
	}
}

// String makes SupportLevel printable without a conversion at every call site.
func (l SupportLevel) String() string {
	return string(l)
}
