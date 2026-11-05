package serverprofile_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
	"ptah.run/internal/serverprofile"
)

// declaredLines are the release lines a provider declares for the engine
// "profiled-engine", as the profile reads them, and declaredFacts their facts.
var (
	declaredLines = []serverprofile.DeclaredLine{
		{Line: "26.2", Label: "Profiled 26.2", Support: capability.Certified, Note: "The current release."},
		{Line: "25.1", Label: "Profiled 25.1", Support: capability.BestEffort},
	}
	declaredFacts = map[string]capability.Capabilities{
		"26.2": capability.Of(map[capability.Capability]bool{capability.TransactionalDDL: false, capability.ReturningClause: true}),
		"25.1": capability.Of(map[capability.Capability]bool{capability.TransactionalDDL: false, capability.ReturningClause: false}),
	}
)

// supported is what the profile says about one key.
func supported(profile serverprofile.Profile, key capability.Capability) bool {
	at := slices.IndexFunc(profile.Capabilities, func(row serverprofile.Capability) bool { return row.Key == string(key) })
	return at >= 0 && profile.Capabilities[at].Supported
}

// A server of an engine a provider serves is profiled on the lines the
// provider declares: the line its version names supplies the facts, the
// preset's name and the certification, which repeats no label, as a built-in
// line's preset name is not its certification's; a version newer than every
// line is planned with the newest and stays best-effort.
func TestWithDeclaredLines_HappyPath(t *testing.T) {
	c := qt.New(t)
	c.Assert(capability.RegisterReleaseLines(declaredFacts, "profiled-engine"), qt.IsNil)
	for _, test := range []struct {
		name, banner string
		preset       serverprofile.Preset
		want         serverprofile.Certification
		returning    bool
	}{
		{name: "a certified line", banner: "26.2.1.14", returning: true,
			preset: serverprofile.Preset{Name: "Profiled 26.2", Dialect: "profiled-engine", Source: serverprofile.SourceVersionLadder},
			want:   serverprofile.Certification{Level: capability.Certified, Line: "26.2", Reason: "The current release."}},
		{name: "a line without a note", banner: "25.1.4.7",
			preset: serverprofile.Preset{Name: "Profiled 25.1", Dialect: "profiled-engine", Source: serverprofile.SourceVersionLadder},
			want:   serverprofile.Certification{Level: capability.BestEffort, Line: "25.1", Reason: capability.BestEffort.Doc()}},
		{name: "a newer release", banner: "26.3.1.18", returning: true,
			preset: serverprofile.Preset{Dialect: "profiled-engine", Source: serverprofile.SourceSaturated,
				Note: "profiled-engine 26.3.1.18 is newer than the newest measured release line 26.2; capabilities were planned as 26.2"},
			want: serverprofile.Certification{Level: capability.BestEffort,
				Reason: "profiled-engine 26.3.1.18 falls on no release line Ptah declares, so nothing in this repository has been run " +
					"against it; capabilities were still resolved and the operations they allow are performed"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			profile := serverprofile.For("profiled-engine", test.banner, "").WithDeclaredLines(declaredLines)

			c.Assert(profile.Preset, qt.DeepEquals, test.preset)
			c.Assert(profile.Certification, qt.DeepEquals, test.want)
			c.Assert(supported(profile, capability.ReturningClause), qt.Equals, test.returning)
		})
	}
}

// A built-in dialect, and an engine whose provider registered no lines, keep
// the profile For gave them.
func TestWithDeclaredLines_FailurePath(t *testing.T) {
	for _, test := range []struct{ name, dialect, banner string }{
		{"a built-in dialect", "postgres", "PostgreSQL 18.1"},
		{"an engine with no registered lines", "unprofiled-engine", "26.2.1.14"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			profile := serverprofile.For(test.dialect, test.banner, "")

			c.Assert(profile.WithDeclaredLines(declaredLines), qt.DeepEquals, profile)
		})
	}
}
