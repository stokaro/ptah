package yamlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/core/yamlschema"
	"ptah.run/dialect/postgres/pgrole"
	"ptah.run/internal/builtintest"
)

// A YAML role entry spells its settings as a Go role directive does, and the
// PostgreSQL owner reads both with one decoder.
func TestParse_RoleSettings_HappyPath(t *testing.T) {
	c := qt.New(t)
	document := `roles:
  app_reader:
    login: true
    settings: "statement_timeout = '30s'; search_path = '$user', public"
    reset_settings: work_mem, app.tenant
`

	db, err := yamlschema.Parse(t.Context(), builtintest.Runtime().YAML(), []byte(document))

	c.Assert(err, qt.IsNil)
	c.Assert(db.Roles, qt.HasLen, 1)
	settings, found, err := pgrole.SettingsOf(db.Roles[0].Facets)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(settings, qt.DeepEquals, []pgrole.Setting{
		{Name: "app.tenant", Reset: true},
		{Name: "search_path", Value: "'$user', public"},
		{Name: "statement_timeout", Value: "'30s'"},
		{Name: "work_mem", Reset: true},
	})
}

// A role that names no setting manages none: the entry carries no facet, so
// a comparison leaves every setting the server holds alone.
func TestParse_RoleSettings_OmittedIsUnmanaged(t *testing.T) {
	c := qt.New(t)
	db, err := yamlschema.Parse(t.Context(), builtintest.Runtime().YAML(), []byte("roles:\n  app_reader:\n    login: true\n"))
	c.Assert(err, qt.IsNil)
	c.Assert(db.Roles, qt.HasLen, 1)
	c.Assert(db.Roles[0].Facets.IsZero(), qt.IsTrue)
}

func TestParse_RoleSettings_FailurePath(t *testing.T) {
	for _, test := range []struct{ name, entry, want string }{
		{name: "unknown key", entry: "    setting: x\n", want: `.*unknown key "setting" of role "app_reader"`},
		{name: "unit without quotes", entry: "    settings: statement_timeout = 30s\n", want: `role "app_reader": .*a value with a unit is a string literal: '30s'`},
		{name: "default", entry: "    settings: work_mem = DEFAULT\n", want: `role "app_reader": .*DEFAULT, which resets it; name it in reset_settings instead`},
		{name: "from current", entry: "    settings: work_mem FROM CURRENT\n", want: `role "app_reader": .*FROM CURRENT stores the value of the session.*`},
		{name: "reset all", entry: "    reset_settings: ALL\n", want: `role "app_reader": .*reset_settings names ALL.*`},
		{name: "set and reset", entry: "    settings: work_mem = '1MB'\n    reset_settings: work_mem\n", want: `role "app_reader": .*named twice`},
		{name: "empty list", entry: "    settings: ' ; '\n", want: `role "app_reader": .*settings lists no setting`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := yamlschema.Parse(t.Context(), builtintest.Runtime().YAML(), []byte("roles:\n  app_reader:\n"+test.entry))
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(db, qt.IsNil)
		})
	}
}

// The facet is a declaration of the PostgreSQL owner whatever target the
// document is rendered for; a target it does not serve refuses it there.
func TestParse_RoleSettings_IsTheOwnersFacet(t *testing.T) {
	c := qt.New(t)
	db, err := yamlschema.Parse(t.Context(), builtintest.Runtime().YAML(), []byte("roles:\n  app:\n    reset_settings: work_mem\n"))
	c.Assert(err, qt.IsNil)
	c.Assert(db.Roles[0].Facets.Kinds(), qt.DeepEquals, []schemaext.Kind{pgrole.SettingsKind})
}
