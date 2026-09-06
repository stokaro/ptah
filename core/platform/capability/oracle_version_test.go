package capability_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
)

// TestOracleResolution pins the ladder against both spellings a live Oracle
// answers with, and against the trap the SQL Server arm of ResolveServerVersion
// describes.
//
// The 23 banner carries two numbers -- "Oracle AI Database 26ai Free Release
// 23.26.2.0.0" -- and both of them land on the Oracle23 preset: 26 because it
// runs off the top of the ladder, 23.26 because it is the measured line. So the
// preset alone cannot tell a resolver that reads the marketing number from one
// that reads the version. VersionSpecific and Saturated can, and that is why
// they are asserted here: without them this test passes against a resolver that
// is wrong about every 23 server.
func TestOracleResolution(t *testing.T) {
	tests := []struct {
		name    string
		version string
		// wantGuard is the preset half: 21 refuses every IF [NOT] EXISTS guard
		// and 23 accepts every one.
		wantGuard bool
		// wantVersionSpecific and wantSaturated are the half that catches a
		// resolver reading the wrong number out of the banner.
		wantVersionSpecific bool
		wantSaturated       bool
	}{
		{
			name:                "23 banner",
			version:             "Oracle AI Database 26ai Free Release 23.26.2.0.0 - Develop, Learn, and Run for Free",
			wantGuard:           true,
			wantVersionSpecific: true,
		},
		{
			// The 21 banner says "Release 21.0.0.0.0" while
			// product_component_version.version_full on the same server says
			// "21.3.0.0.0". Only the second is a measured line, which is why
			// the live path reads that column rather than the banner.
			name:      "21 banner",
			version:   "Oracle Database 21c Express Edition Release 21.0.0.0.0 - Production",
			wantGuard: false,
		},
		{
			name:                "23 version_full",
			version:             "23.26.2.0.0",
			wantGuard:           true,
			wantVersionSpecific: true,
		},
		{
			name:                "21 version_full",
			version:             "21.3.0.0.0",
			wantGuard:           false,
			wantVersionSpecific: true,
		},
		{
			name:          "past the newest measured line",
			version:       "24.1.0.0.0",
			wantGuard:     true,
			wantSaturated: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := capability.ResolveServerVersion(platform.Oracle, test.version)

			c.Assert(got.Capabilities.Has(capability.ObjectExistenceGuards), qt.Equals, test.wantGuard)
			c.Assert(got.Capabilities.Has(capability.DropIndexIfExists), qt.Equals, test.wantGuard)
			c.Assert(got.VersionSpecific, qt.Equals, test.wantVersionSpecific)
			c.Assert(got.Saturated, qt.Equals, test.wantSaturated)
			c.Assert(got.ResolvedDialect, qt.Equals, platform.Oracle)
		})
	}
}

// TestOraclePresets_DifferOnlyInTheGuardsAndDomains states the whole distance
// between the two measured lines, so a later edit that changes another key has
// to say so here.
//
// domain_types joined the list in stokaro/ptah#1920, and it is the first key
// here that is not a guard: 23 has a real CREATE DOMAIN and 21 answers
// ORA-00901, so the difference is the object rather than the spelling of an
// IF EXISTS.
func TestOraclePresets_DifferOnlyInTheGuardsAndDomains(t *testing.T) {
	c := qt.New(t)

	newer := capability.Oracle23()
	older := capability.Oracle21()

	c.Assert(newer.Validate(), qt.IsNil)
	c.Assert(older.Validate(), qt.IsNil)

	differing := slices.DeleteFunc(capability.All(), func(key capability.Capability) bool {
		return newer.Has(key) == older.Has(key)
	})
	names := make([]string, 0, len(differing))
	for _, key := range differing {
		names = append(names, string(key))
	}
	c.Assert(names, qt.ContentEquals, []string{
		string(capability.DomainTypes),
		string(capability.DropIndexIfExists),
		string(capability.ObjectExistenceGuards),
	})
}
