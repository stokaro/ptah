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

// A version that names no declared line is refused with the lines to choose
// from, and so is any version of an engine whose provider declares no line:
// either would otherwise select nothing and be dropped in silence.
func TestResolve_AProvidersReleaseLine_FailurePath(t *testing.T) {
	c := qt.New(t)
	c.Assert(capability.RegisterReleaseLines(providedLines, "versioned-engine"), qt.IsNil)
	for _, test := range []struct{ name, dialect, version, want string }{
		{"an undeclared line", "versioned-engine", "25.4.1.15",
			`"25.4.1.15" names no release line the versioned-engine provider declares: expected 25.1, 26.2, or a version that starts with one followed by a dot`},
		{"a spelling the rule does not read", "versioned-engine", "stable-25-1-4",
			`"stable-25-1-4" names no release line the versioned-engine provider declares: .*`},
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
