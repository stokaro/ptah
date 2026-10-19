package builtin_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/doblockext/builtin"
)

// Every language a descriptor may name has a reader, so a dialect that names
// one reads its DO blocks.
func TestLookup_ServesEveryDeclaredLanguage(t *testing.T) {
	for _, language := range descriptor.DoBlockLanguages() {
		t.Run(language, func(t *testing.T) {
			c := qt.New(t)
			services := builtin.Lookup(language)
			c.Assert(services.Languages, qt.DeepEquals, []string{language})
			c.Assert(services.Label, qt.Not(qt.Equals), "")
			c.Assert(services.Roles, qt.IsNotNil)
		})
	}
}

// The empty name, which a descriptor of an engine without DO blocks states,
// and a language no service serves have no reader.
func TestLookup_NoLanguage(t *testing.T) {
	for _, language := range []string{"", "plv8", "PLPGSQL"} {
		t.Run(language, func(t *testing.T) {
			c := qt.New(t)
			services := builtin.Lookup(language)
			c.Assert(services.Languages, qt.IsNil)
			c.Assert(services.Roles, qt.IsNil)
		})
	}
}
