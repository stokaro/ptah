package yamlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/yamlschema"
	"ptah.run/internal/builtintest"
)

// Two entries may name one role. Where they agree they are one declaration,
// and the role is read once.
func TestParse_DuplicateRoles_HappyPath(t *testing.T) {
	c := qt.New(t)
	document := `roles:
  reader:
    name: app
    login: true
    settings: "statement_timeout = '30s'"
  reader_again:
    name: app
    login: true
    settings: "statement_timeout = '30s'"
`

	db, err := yamlschema.Parse(t.Context(), builtintest.Runtime().YAML(), []byte(document))

	c.Assert(err, qt.IsNil)
	c.Assert(db.Roles, qt.HasLen, 1)
	c.Assert([]any{db.Roles[0].Name, db.Roles[0].Login}, qt.DeepEquals, []any{"app", true})
}

// Two entries that name one role and disagree are refused, naming both keys.
// Without the refusal the first entry wins and the second one's attributes
// and settings are dropped with nothing said.
func TestParse_DuplicateRoles_FailurePath(t *testing.T) {
	for _, test := range []struct{ name, second string }{
		{name: "an attribute", second: "    login: false\n    settings: \"statement_timeout = '30s'\"\n"},
		{name: "a setting", second: "    login: true\n    settings: \"statement_timeout = '45s'\"\n"},
		{name: "settings on one entry only", second: "    login: true\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			document := "roles:\n  reader:\n    name: app\n    login: true\n    settings: \"statement_timeout = '30s'\"\n" +
				"  writer:\n    name: app\n" + test.second

			db, err := yamlschema.Parse(t.Context(), builtintest.Runtime().YAML(), []byte(document))

			c.Assert(err, qt.ErrorMatches, `.*roles "reader" and "writer": conflicting role "app" definitions`)
			c.Assert(db, qt.IsNil)
		})
	}
}
