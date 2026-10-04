package capability_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
)

// TestYDBResolution_HappyPath pins the ladder against the version strings the
// six measured lines report for `SELECT Version()`, each read off a live
// local-ydb container: five dotted releases and the branch name 25.4.1.15
// answers with. The branch name is the row that matters most. The shared parse
// reads `stable-25-4-1` as 25.0, which would put a 25.4 server on the 25.1
// preset and refuse every 64-bit date type it has.
//
// The preset is compared by equality with the constructor the line declares,
// so a row passes only when the ladder picked that line and nothing else.
func TestYDBResolution_HappyPath(t *testing.T) {
	tests := []struct {
		name                string
		version             string
		want                capability.Capabilities
		wantVersionSpecific bool
		wantSaturated       bool
	}{
		{name: "25.1.4.7", version: "25.1.4.7", want: capability.YDB251(), wantVersionSpecific: true},
		{name: "25.2.1.24", version: "25.2.1.24", want: capability.YDB252(), wantVersionSpecific: true},
		{name: "25.3.1.25", version: "25.3.1.25", want: capability.YDB253(), wantVersionSpecific: true},
		{name: "25.4.1.15 branch name", version: "stable-25-4-1", want: capability.YDB253(), wantVersionSpecific: true},
		{name: "26.1.1.22", version: "26.1.1.22", want: capability.YDB261(), wantVersionSpecific: true},
		{name: "26.2.1.14", version: "26.2.1.14", want: capability.YDB262(), wantVersionSpecific: true},
		{name: "branch name without a patch", version: "stable-26-1", want: capability.YDB261(), wantVersionSpecific: true},
		// Below the oldest measured line: the oldest preset, and no line
		// selected it.
		{name: "24.4.4.12", version: "24.4.4.12", want: capability.YDB251()},
		// Past the newest measured line: the newest preset, saturated.
		{name: "26.3.1.18", version: "26.3.1.18", want: capability.YDB262(), wantSaturated: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got := capability.ResolveServerVersion(platform.YDB, test.version)
			c.Assert(got.Capabilities, qt.DeepEquals, test.want)
			c.Assert(got.VersionSpecific, qt.Equals, test.wantVersionSpecific)
			c.Assert(got.Saturated, qt.Equals, test.wantSaturated)
			c.Assert(got.Recognized, qt.IsTrue)
			c.Assert(got.NewestMeasured, qt.Equals, "26.2")
			c.Assert(got.ResolvedDialect, qt.Equals, platform.YDB)
		})
	}
}

// TestYDBResolution_FailurePath covers strings that name no YDB release: a
// floating build, a branch name with no line in it, and nothing at all. Each
// takes the dialect default with Recognized false, which is what a caller
// holding operator input refuses on.
func TestYDBResolution_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		version string
	}{
		{name: "floating build", version: "trunk"},
		// A number behind a word is not a release: the general parser would
		// skip the word and read 26.2 out of it.
		{name: "floating build carrying a number", version: "trunk-26.2"},
		{name: "branch name with one number", version: "stable-26"},
		{name: "branch name with a word in it", version: "stable-26-x"},
		{name: "empty", version: ""},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got := capability.ResolveServerVersion(platform.YDB, test.version)
			c.Assert(got.Recognized, qt.IsFalse)
			c.Assert(got.VersionSpecific, qt.IsFalse)
			c.Assert(got.Capabilities, qt.DeepEquals, capability.ForDialect(platform.YDB))
			c.Assert(got.ResolvedDialect, qt.Equals, platform.YDB)
		})
	}
}

// TestYDBPresets_DifferOnlyWhereTheLinesDid holds each derived YDB preset to
// the keys its line was measured to answer differently from the line above
// it. A key changed in the base preset and forgotten in a derivation, or a
// derivation that drifts by a key nobody measured, turns a row red.
func TestYDBPresets_DifferOnlyWhereTheLinesDid(t *testing.T) {
	tests := []struct {
		name  string
		lower capability.Capabilities
		upper capability.Capabilities
		want  []capability.Capability
	}{
		{name: "26.1 below 26.2", lower: capability.YDB261(), upper: capability.YDB262(),
			want: []capability.Capability{capability.AlterColumnDefault}},
		{name: "25.3 below 26.1", lower: capability.YDB253(), upper: capability.YDB261(),
			want: []capability.Capability{capability.AddColumnWithDefault}},
		{name: "25.2 below 25.3", lower: capability.YDB252(), upper: capability.YDB253(),
			want: []capability.Capability{capability.DocumentTypeDefaults, capability.ReturningClause}},
		{name: "25.1 below 25.2", lower: capability.YDB251(), upper: capability.YDB252(),
			want: []capability.Capability{capability.ParameterizedDecimal, capability.SmallIntegerDefaults, capability.WideDateTimeTypes}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.lower.Validate(), qt.IsNil)
			c.Assert(differingKeys(test.lower, test.upper), qt.DeepEquals, test.want)
		})
	}
}

// differingKeys lists, sorted, the registered keys on which two sets answer
// differently.
func differingKeys(a, b capability.Capabilities) []capability.Capability {
	keys := slices.DeleteFunc(capability.All(), func(key capability.Capability) bool {
		return a.Has(key) == b.Has(key)
	})
	slices.Sort(keys)
	return keys
}
