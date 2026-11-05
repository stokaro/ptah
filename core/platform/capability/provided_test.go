package capability_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
)

// stated is what a provider states for an engine Ptah does not describe.
var stated = capability.Of(map[capability.Capability]bool{capability.TransactionalDDL: false, capability.ReturningClause: true})

// The registered facts answer for every name of the engine, without regard to
// case or surrounding space, and registering them again changes nothing. A
// built-in dialect keeps its descriptor's preset.
func TestRegisterDefaults_HappyPath(t *testing.T) {
	c := qt.New(t)
	c.Assert(capability.ForDialect("stated-engine"), qt.DeepEquals, capability.Capabilities{})

	c.Assert(capability.RegisterDefaults(stated, "stated-engine", "stated-alias"), qt.IsNil)
	c.Assert(capability.RegisterDefaults(stated, "stated-engine"), qt.IsNil)

	c.Assert(capability.ForDialect("stated-engine").Equal(stated), qt.IsTrue)
	c.Assert(capability.ForDialect(" Stated-Alias ").Equal(stated), qt.IsTrue)
	c.Assert(capability.ForDialect("sqlite").Equal(capability.SQLite3()), qt.IsTrue)
}

func TestRegisterDefaults_FailurePath(t *testing.T) {
	c := qt.New(t)
	c.Assert(capability.RegisterDefaults(stated, "stated-taken"), qt.IsNil)
	other := capability.Of(map[capability.Capability]bool{capability.TransactionalDDL: true})
	for _, test := range []struct {
		name  string
		caps  capability.Capabilities
		names []string
		want  string
	}{
		{"other facts for a registered name", other, []string{"stated-taken"}, `capability: stated-taken was registered with other default capabilities`},
		{"a dialect Ptah describes", stated, []string{"PostgreSQL"}, `capability: postgresql is a dialect Ptah describes, .*`},
		{"an empty name", stated, []string{" "}, `capability: default capabilities need an engine name`},
		{"a key Ptah does not know", capability.Of(map[capability.Capability]bool{"no_such_capability": true}), []string{"stated-unknown"}, `capability: default capabilities: .*`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(capability.RegisterDefaults(test.caps, test.names...), qt.ErrorMatches, test.want)
		})
	}
	c.Assert(capability.ForDialect("stated-taken").Equal(stated), qt.IsTrue)
	c.Assert(capability.ForDialect("stated-unknown"), qt.DeepEquals, capability.Capabilities{})
	c.Assert(capability.ForDialect("postgresql").Equal(capability.ForDialect("postgres")), qt.IsTrue)
}

// linesFor is two release lines of an engine, the newer one with a fact the
// older lacks.
func linesFor() map[string]capability.Capabilities {
	return map[string]capability.Capabilities{
		"2.1":  capability.Of(map[capability.Capability]bool{capability.TransactionalDDL: false, capability.ReturningClause: true}),
		"2.10": capability.Of(map[capability.Capability]bool{capability.TransactionalDDL: false, capability.ReturningClause: true}),
		"1.4":  capability.Of(map[capability.Capability]bool{capability.TransactionalDDL: false, capability.ReturningClause: false}),
	}
}

// A version names the line it equals, or the longest line it starts with
// followed by a dot, so 2.10.3 names 2.10 and not 2.1.
func TestProvidedReleaseLine_HappyPath(t *testing.T) {
	c := qt.New(t)
	c.Assert(capability.RegisterReleaseLines(linesFor(), "lined-engine", "lined-alias"), qt.IsNil)
	c.Assert(capability.RegisterReleaseLines(linesFor(), "lined-engine"), qt.IsNil)
	for _, test := range []struct {
		version, line string
		returning     bool
	}{
		{version: "1.4", line: "1.4", returning: false},
		{version: "1.4.7", line: "1.4", returning: false},
		{version: "2.1.0.5", line: "2.1", returning: true},
		{version: "2.10.3", line: "2.10", returning: true},
	} {
		t.Run(test.version, func(t *testing.T) {
			c := qt.New(t)
			resolution, registered := capability.ProvidedReleaseLine(" Lined-Alias ", test.version)
			c.Assert(registered, qt.IsTrue)
			c.Assert(resolution.Line, qt.Equals, test.line)
			c.Assert(resolution.Capabilities.Has(capability.ReturningClause), qt.Equals, test.returning)
			c.Assert(resolution.Declared, qt.DeepEquals, []string{"1.4", "2.1", "2.10"})
		})
	}
}

// A version no line names resolves to none of them, and an engine with no
// registered lines is not registered; a registration that conflicts is
// refused.
func TestProvidedReleaseLine_FailurePath(t *testing.T) {
	c := qt.New(t)
	c.Assert(capability.RegisterReleaseLines(linesFor(), "lined-taken"), qt.IsNil)
	for _, version := range []string{"3.0", "1", "1.40", "stable-1-4"} {
		t.Run(version, func(t *testing.T) {
			c := qt.New(t)
			resolution, registered := capability.ProvidedReleaseLine("lined-taken", version)
			c.Assert(registered, qt.IsTrue)
			c.Assert(resolution.Found(), qt.IsFalse)
			c.Assert(resolution.Capabilities, qt.DeepEquals, capability.Capabilities{})
		})
	}
	_, registered := capability.ProvidedReleaseLine("unlined-engine", "1.4")
	c.Assert(registered, qt.IsFalse)
	other := map[string]capability.Capabilities{"1.4": capability.Of(map[capability.Capability]bool{capability.TransactionalDDL: true})}
	c.Assert(capability.RegisterReleaseLines(other, "lined-taken"), qt.ErrorMatches, `capability: lined-taken was registered with other release lines`)
	c.Assert(capability.RegisterReleaseLines(linesFor(), "MySQL"), qt.ErrorMatches, `capability: mysql is a dialect Ptah describes, .*`)
	c.Assert(capability.RegisterReleaseLines(map[string]capability.Capabilities{" ": {}}, "lined-blank"), qt.ErrorMatches, `capability: a release line needs a name`)
}
