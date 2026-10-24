// Package providerrelease reads what an official provider release publishes
// beside its archives: ptah-provider.json, which names the release's artifacts
// and the provider contract it needs, and checksums.txt, which holds the
// digests. It also holds the contract version comparison that installation and
// selection share. It does no I/O: callers pass the bytes they downloaded.
package providerrelease

import (
	"fmt"
	"runtime"

	"github.com/stokaro/ptah/provider-sdk/provider"
)

// Contract is a provider contract version, MAJOR.MINOR. The major version
// changes only on an incompatible change. The minor version grows with each
// compatible addition and goes back to 0 when the major version changes.
type Contract struct {
	Major uint32 `json:"major"`
	Minor uint32 `json:"minor"`
}

// String returns the version as "MAJOR.MINOR".
func (c Contract) String() string { return fmt.Sprintf("%d.%d", c.Major, c.Minor) }

// Requirement describes what a minimum contract asks of a host, such as
// "25.1 or a later 25.x".
func (c Contract) Requirement() string {
	return fmt.Sprintf("%s or a later %d.x", c, c.Major)
}

// HostContract returns the contract version this Ptah speaks. The major version
// is the Revision of the provider SDK the host is built with. That SDK declares
// no minor version, so the host speaks minor 0.
func HostContract() Contract { return Contract{Major: provider.Revision} }

// Meets reports whether a host that speaks host can start a provider whose
// minimum contract is minimum: the major versions are equal and the host's
// minor version is at least the minimum's. Everything a provider can use beyond
// its minimum is negotiated as features at startup, never through this check.
//
// This is the one comparison of contract versions. Installation and selection
// both call it, so neither can grow a rule of its own.
func Meets(host, minimum Contract) bool {
	return host.Major == minimum.Major && host.Minor >= minimum.Minor
}

// Platform names one operating system and architecture, spelled the way Go
// spells GOOS and GOARCH.
type Platform struct {
	OS   string
	Arch string
}

// String returns the platform as "<os>/<arch>".
func (p Platform) String() string { return p.OS + "/" + p.Arch }

// Running returns the platform of this process.
func Running() Platform { return Platform{OS: runtime.GOOS, Arch: runtime.GOARCH} }
