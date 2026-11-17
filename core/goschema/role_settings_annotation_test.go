package goschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/goschema"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/postgres/pgrole"
	"ptah.run/internal/builtintest"
)

func roleSource(attributes string) string {
	return "package p\n//ptah:schema:role name=\"app_reader\" login=\"true\" " + attributes + "\ntype AppReader struct{}\n"
}

// The role directive carries the PostgreSQL owner's settings, a semicolon in a
// quoted value included, and its explicit resets.
func TestParseRoleAnnotation_Settings_HappyPath(t *testing.T) {
	c := qt.New(t)
	source := roleSource(`settings="statement_timeout = '30s'; app.tenant = 'a;b'; search_path = '$user', public" reset_settings="work_mem"`)

	database, err := goschema.ParseSource(builtintest.Annotations(), "role.go", source)

	c.Assert(err, qt.IsNil)
	c.Assert(database.Roles, qt.HasLen, 1)
	settings, found, err := pgrole.SettingsOf(database.Roles[0].Facets)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(settings, qt.DeepEquals, []pgrole.Setting{
		{Name: "app.tenant", Value: "'a;b'"},
		{Name: "search_path", Value: "'$user', public"},
		{Name: "statement_timeout", Value: "'30s'"},
		{Name: "work_mem", Reset: true},
	})
}

func TestParseRoleAnnotation_Settings_OmittedIsUnmanaged(t *testing.T) {
	c := qt.New(t)
	database, err := goschema.ParseSource(builtintest.Annotations(), "role.go", roleSource(""))
	c.Assert(err, qt.IsNil)
	c.Assert(database.Roles, qt.HasLen, 1)
	c.Assert(database.Roles[0].Facets.IsZero(), qt.IsTrue)
}

func TestParseRoleAnnotation_Settings_FailurePath(t *testing.T) {
	for _, test := range []struct{ name, attributes, want string }{
		{name: "unit without quotes", attributes: `settings="statement_timeout = 30s"`, want: `(?s).*a value with a unit is a string literal: '30s'.*`},
		{name: "not an assignment", attributes: `settings="statement_timeout"`, want: `(?s).*role setting "statement_timeout" is not name = value.*`},
		{name: "reset all", attributes: `reset_settings="all"`, want: `(?s).*reset_settings names ALL.*`},
		{name: "unclosed quote", attributes: `settings="app.tenant = 'a"`, want: `(?s).*an unclosed quote.*`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database, err := goschema.ParseSource(builtintest.Annotations(), "role.go", roleSource(test.attributes))
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(database, qt.DeepEquals, schemamodel.Database{})
		})
	}
}
