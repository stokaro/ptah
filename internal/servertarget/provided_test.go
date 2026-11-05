package servertarget_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
	"ptah.run/internal/servertarget"
)

// providedLines are the release lines a provider declares for the engine
// "versioned-engine".
var providedLines = map[string]capability.Capabilities{
	"26.2": capability.Of(map[capability.Capability]bool{capability.TransactionalDDL: false, capability.ReturningClause: true}),
	"25.1": capability.Of(map[capability.Capability]bool{capability.TransactionalDDL: false, capability.ReturningClause: false}),
}

// A version of an engine a provider serves resolves to the facts of the
// release line it names, with no note: the line is the one asked for.
func TestResolve_AProvidersReleaseLine_HappyPath(t *testing.T) {
	c := qt.New(t)
	c.Assert(capability.RegisterReleaseLines(providedLines, "versioned-engine"), qt.IsNil)
	for _, test := range []struct {
		version   string
		returning bool
	}{
		{version: "26.2", returning: true},
		{version: "26.2.1.14", returning: true},
		{version: "25.1.4.7", returning: false},
	} {
		t.Run(test.version, func(t *testing.T) {
			c := qt.New(t)
			target, err := servertarget.Resolve("versioned-engine", test.version)
			c.Assert(err, qt.IsNil)
			c.Assert(target.Capabilities.Has(capability.ReturningClause), qt.Equals, test.returning)
			c.Assert(target.Capabilities.Established(capability.TransactionalDDL), qt.IsTrue)
			c.Assert(target.Note, qt.Equals, "")
		})
	}
}

// A release that names no declared line is planned with the line the declared
// lines place it on, as a built-in dialect's ladder places it, and the note
// says what was planned instead, in the words a built-in dialect's note uses.
func TestResolve_AProvidersReleaseLine_HappyPath_PlacesAReleaseOnTheLines(t *testing.T) {
	c := qt.New(t)
	c.Assert(capability.RegisterReleaseLines(providedLines, "versioned-engine"), qt.IsNil)
	for _, test := range []struct {
		version, note string
		returning     bool
	}{
		{version: "26.3.1.18", returning: true,
			note: "versioned-engine 26.3.1.18 is newer than the newest measured release line 26.2; capabilities were planned as 26.2"},
		{version: "25.4.1.15", returning: false,
			note: "versioned-engine 25.4.1.15 is not a measured release line; capabilities fall back to the preset its ladder assigns (newest measured line: 26.2)"},
		{version: "24.4.4.12", returning: false,
			note: "versioned-engine 24.4.4.12 is not a measured release line; capabilities fall back to the preset its ladder assigns (newest measured line: 26.2)"},
		{version: "stable-25-1-4", returning: false},
	} {
		t.Run(test.version, func(t *testing.T) {
			c := qt.New(t)
			target, err := servertarget.Resolve("versioned-engine", test.version)
			c.Assert(err, qt.IsNil)
			c.Assert(target.Capabilities.Has(capability.ReturningClause), qt.Equals, test.returning)
			c.Assert(target.Capabilities.Established(capability.TransactionalDDL), qt.IsTrue)
			c.Assert(target.Note, qt.Equals, test.note)
		})
	}
}

// A version that is no release is refused with the lines to choose from, and
// so is any version of an engine whose provider declares no line: either
// would otherwise select nothing and be dropped in silence.
func TestResolve_AProvidersReleaseLine_FailurePath(t *testing.T) {
	c := qt.New(t)
	c.Assert(capability.RegisterReleaseLines(providedLines, "versioned-engine"), qt.IsNil)
	for _, test := range []struct{ name, dialect, version, want string }{
		{"a floating build", "versioned-engine", "trunk",
			`"trunk" names no release line the versioned-engine provider declares: expected 25.1, 26.2, or a version that starts with one followed by a dot`},
		{"a branch with no minor", "versioned-engine", "stable-26",
			`"stable-26" names no release line the versioned-engine provider declares: .*`},
		{"an engine whose provider declares none", "unversioned-engine", "26.2",
			`"26.2" names no release line of unversioned-engine: Ptah holds no release-line capabilities for an engine its provider serves, and the provider declares none`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target, err := servertarget.Resolve(test.dialect, test.version)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(err, qt.ErrorAs, new(*servertarget.UndeclaredLineError))
			c.Assert(target, qt.DeepEquals, servertarget.Target{})
		})
	}
}
