// Package serverprofile assembles what Ptah knows about one concrete database
// server: which product and release it is, which capability preset answers for
// it and how that preset was reached, what this repository promises about the
// release line, and the capability values — boolean and otherwise — that
// follow. It sits below the CLI layer because the assembly is the general
// capability and `ptah db capabilities` is only its first reader.
//
// [For] is deliberately pure. Every input is a string the caller already read
// from the server, so a profile can be built, asserted and rendered without a
// database, and the two live reads stay in the command that owns the
// connection.
//
// The package answers three questions that are routinely conflated, and keeps
// them in separate fields because the answers genuinely differ:
//
//   - which product does the server SAY it is ([Server.Product], from the
//     banner);
//   - whose capability ladder ANSWERED ([Preset.Dialect]) — a MariaDB behind a
//     mysql:// URL is resolved as MariaDB, because a banner is better evidence
//     than a driver name;
//   - what does Ptah PROMISE about the release line ([Certification]) — which
//     is a statement about this repository's continuous integration and not
//     about the server at all.
package serverprofile

import (
	"fmt"
	"slices"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/internal/capabilityprobe"
	"go.5x5.cz/ptah/internal/servertarget"
)

// Profile is everything Ptah has established about one server.
type Profile struct {
	// Dialect is the normalized dialect the caller connected as. It is what
	// the URL scheme said, which is not always what answered — compare
	// Preset.Dialect.
	Dialect string `json:"dialect"`

	// Server is the identity the server reported about itself.
	Server Server `json:"server"`

	// Preset is the capability set Ptah plans with here, and how it was
	// chosen.
	Preset Preset `json:"capability_preset"`

	// Certification is what Ptah promises about this release line.
	Certification Certification `json:"certification"`

	// Traits are the non-boolean capability values for this target.
	Traits capability.Traits `json:"traits"`

	// Capabilities is every registered capability key with its value here,
	// sorted by key so that two runs against one server render byte-identical
	// output and a diff of that output means something changed.
	Capabilities []Capability `json:"capabilities"`
}

// Server is what the server said about itself.
type Server struct {
	// Banner is the version string the server reported, verbatim.
	Banner string `json:"banner,omitempty"`

	// Version is the product version parsed out of the banner, empty when no
	// version could be read from it.
	//
	// It is the CORRECTED parse: SQL Server's banner opens with a marketing
	// year and YugabyteDB's with a PostgreSQL compatibility version, and both
	// are read past. That correction is why a profile asks
	// capabilityprobe.ParseVersion rather than reading the leading digits.
	Version string `json:"version,omitempty"`

	// Product is the database product the banner names, empty when it names
	// none. A bare "8.0.42" names no product; so does an unreadable banner.
	Product string `json:"product,omitempty"`
}

// Preset is the capability set Ptah plans with, and the evidence for it.
type Preset struct {
	// Name is the preset's name in core/platform/capability, empty when the
	// server fell on no declared release line and no name can be attributed.
	Name string `json:"name,omitempty"`

	// Dialect is the platform whose ladder produced the set, which is not
	// always [Profile.Dialect].
	Dialect string `json:"dialect,omitempty"`

	// Source names how the set was reached. It is the observability the
	// capability model needs to be debuggable: a wrong capability is a
	// question about which rule fired, and without this the answer is
	// unavailable to anyone not reading the resolver.
	Source Source `json:"source"`

	// Note is empty when the version selected an exact measured release line,
	// and otherwise says in one sentence what was planned instead. The
	// wording is servertarget's, so the live path and the typed
	// --server-version path cannot describe the same outcome differently.
	Note string `json:"note,omitempty"`
}

// Source names how a capability preset was reached.
type Source string

const (
	// SourceVersionLadder: the parsed version selected an exact measured
	// release line on its dialect's ladder. This is the only source that is a
	// version-specific answer.
	SourceVersionLadder Source = "version-ladder"

	// SourceSaturated: the version parsed and is newer than the newest line
	// the ladder was measured against, so the top preset is a stand-in and any
	// capability the newer server gained or lost is unmodeled.
	SourceSaturated Source = "newer-than-measured"

	// SourceDialectDefault: the version was read and did not change the
	// answer, either because it fell between measured lines or because the
	// dialect has no ladder.
	SourceDialectDefault Source = "dialect-default"

	// SourceUnrecognized: the string named no server at all, so the preset was
	// chosen without consulting it. On a live connection this is a surprise
	// worth printing — a server does not typo its own name — and it is
	// reported rather than refused, because refusing would take away a
	// working connection over a banner Ptah cannot read.
	SourceUnrecognized Source = "unrecognized-banner"
)

// Certification is what Ptah promises about the release line a server falls on.
type Certification struct {
	// Level is the promise. It is [capability.BestEffort] whenever no declared
	// line matched, which is the honest answer for a server this repository
	// has never run anything against.
	Level capability.SupportLevel `json:"level"`

	// Line is the declared release line the server fell on, empty when none
	// did.
	Line string `json:"line,omitempty"`

	// Label is the line's human name where it differs from Line, for example
	// "SQL Server 2022".
	Label string `json:"label,omitempty"`

	// Reason says why the level is what it is. It is always populated: for a
	// matched line it is the line's own note where one exists, and for an
	// unmatched one it says that no declared line covers this release.
	Reason string `json:"reason"`
}

// Capability is one registered capability key as it stands on this server.
type Capability struct {
	Key       string `json:"key"`
	Supported bool   `json:"supported"`
	Doc       string `json:"doc"`
}

// For builds the profile for a server, from strings the caller has already
// read from it.
//
// banner is the server's own version string — SELECT version(), @@VERSION, or
// the dialect's equivalent. productVersion is the cleaner version surface a
// dialect may offer instead; today only SQL Server has one, and
// [capabilityprobe.ProductVersion] is what reads it. Both may be empty: a
// profile for a server that answered nothing about itself is still a profile,
// and it reports exactly that.
func For(dialect, banner, productVersion string) Profile {
	normalized := platform.NormalizeDialect(dialect)
	resolution := capability.ResolveServerVersion(normalized, banner)

	// The ladder that answered decides how the banner is read, not the dialect
	// the caller connected as. A MariaDB reached over mysql:// prefixes its
	// banner with the 5.5.5- replication marker, and only the MariaDB rule
	// trims it.
	effective := resolution.ResolvedDialect
	if effective == "" {
		effective = normalized
	}

	profile := Profile{
		Dialect: normalized,
		Server: Server{
			Banner:  banner,
			Product: capability.BannerPlatform(banner),
		},
		Preset: Preset{
			Dialect: effective,
			Source:  sourceOf(resolution),
			Note:    servertarget.VersionNote(effective, banner, resolution),
		},
		Traits:       capability.TraitsFor(effective, resolution.Capabilities),
		Capabilities: capabilitiesOf(resolution.Capabilities),
	}

	version, err := capabilityprobe.ParseVersion(effective, banner, productVersion)
	if err != nil {
		profile.Certification = unmatched(effective, "")
		return profile
	}
	profile.Server.Version = version.String()

	cell, matched := capabilityprobe.CellFor(effective, version)
	if !matched {
		profile.Certification = unmatched(effective, version.String())
		return profile
	}
	profile.Preset.Name = cell.PresetName
	profile.Certification = Certification{
		Level:  cell.Support,
		Line:   cell.Line,
		Label:  cell.Label,
		Reason: certificationReason(cell),
	}
	return profile
}

// unmatched is the certification for a server on no declared release line.
//
// It is best-effort by construction and never an error. A release Ptah has not
// covered is one Ptah still connects to, resolves capabilities for, and
// performs the operations those capabilities allow — refusing it because a
// matrix cell is missing would make the matrix a gate on the user's database
// rather than a record of what this repository tests.
func unmatched(dialect, version string) Certification {
	if version == "" {
		return Certification{
			Level: capability.BestEffort,
			Reason: fmt.Sprintf(
				"no product version could be read from the %s server's banner, so no declared release "+
					"line could be matched; capabilities were still resolved and are reported below",
				dialect),
		}
	}
	return Certification{
		Level: capability.BestEffort,
		Reason: fmt.Sprintf(
			"%s %s falls on no release line Ptah declares, so nothing in this repository has been run "+
				"against it; capabilities were still resolved and the operations they allow are performed",
			dialect, version),
	}
}

// certificationReason explains a matched line's level. The cell's own note is
// preferred where it has one, because that note is where the reasoning was
// written down next to the declaration; the fallback states the level's
// meaning so the field is never empty.
func certificationReason(cell capabilityprobe.Cell) string {
	if cell.Note != "" {
		return cell.Note
	}
	return cell.Support.Doc()
}

// sourceOf reads the resolution's three flags as the one mechanism that fired.
// The flags are not independent — VersionSpecific and Saturated are mutually
// exclusive, and Recognized false implies VersionSpecific false — so a single
// name is the honest rendering, and the order below is the implication order.
func sourceOf(resolution capability.VersionResolution) Source {
	switch {
	case !resolution.Recognized:
		return SourceUnrecognized
	case resolution.VersionSpecific:
		return SourceVersionLadder
	case resolution.Saturated:
		return SourceSaturated
	default:
		return SourceDialectDefault
	}
}

// capabilitiesOf renders every registered key, present or absent.
//
// Absent keys are included on purpose. A report that listed only what a server
// supports could not answer "why did Ptah refuse this", which is the question
// the command exists for, and a reader cannot tell a capability that is absent
// from one this build does not know about.
//
// The sort is load-bearing rather than cosmetic. capability.All ranges over the
// registry map and says so — "the order is unspecified; sort before rendering
// user-facing output" — so without this, two runs against the same server
// produced two different JSON documents, and anything diffing that output
// would report a change on every invocation.
func capabilitiesOf(caps capability.Capabilities) []Capability {
	keys := slices.Sorted(slices.Values(capability.All()))
	out := make([]Capability, 0, len(keys))
	for _, key := range keys {
		out = append(out, Capability{
			Key:       string(key),
			Supported: caps.Has(key),
			Doc:       capability.Doc(key),
		})
	}
	return out
}
