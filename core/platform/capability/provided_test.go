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
