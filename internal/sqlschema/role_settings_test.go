package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/core/sqlext"
	"ptah.run/dialect/postgres/pgrole"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/sqlschema"
)

// readRoleSettings reads text, which declares one role, and returns the
// settings the role declares.
func readRoleSettings(c *qt.C, text string) []pgrole.Setting {
	database, _, err := sqlschema.ReadWith(c.Context(), []byte(text), "postgres", nil, sqlschema.Options{Owners: builtintest.Runtime().SQL()})
	c.Assert(err, qt.IsNil)
	c.Assert(database.Roles, qt.HasLen, 1)
	settings, _, err := pgrole.SettingsOf(database.Roles[0].Facets)
	c.Assert(err, qt.IsNil)
	return settings
}

// The statements of one role fold in the order the server runs them: a later
// statement about a setting replaces an earlier one, SET ... TO DEFAULT is a
// RESET, ALTER USER is ALTER ROLE, and a role named in quotes and bare by
// its folded name is one role.
func TestReadRoleSettings_HappyPath(t *testing.T) {
	c := qt.New(t)
	settings := readRoleSettings(c, `CREATE ROLE app_reader LOGIN;
ALTER ROLE app_reader SET statement_timeout = '1min';
ALTER ROLE "app_reader" SET statement_timeout TO '30s';
ALTER USER APP_READER SET search_path TO "$user", public;
ALTER ROLE app_reader SET app.tenant = 'acme';
ALTER ROLE app_reader SET app.tenant TO DEFAULT;
ALTER ROLE app_reader RESET "Work_Mem";
`)
	c.Assert(settings, qt.DeepEquals, []pgrole.Setting{
		{Name: "app.tenant", Reset: true},
		{Name: "search_path", Value: `"$user", public`},
		{Name: "statement_timeout", Value: "'30s'"},
		{Name: "Work_Mem", Reset: true},
	})
}

func TestReadRoleSettings_QuotedRole(t *testing.T) {
	c := qt.New(t)
	database, _, err := sqlschema.ReadWith(c.Context(), []byte(`CREATE ROLE "App ""Reader""";
ALTER ROLE "App ""Reader""" SET work_mem = '64MB';`), "postgres", nil, sqlschema.Options{Owners: builtintest.Runtime().SQL()})
	c.Assert(err, qt.IsNil)
	c.Assert(database.Roles[0].Name, qt.Equals, `App "Reader"`)
	settings, _, err := pgrole.SettingsOf(database.Roles[0].Facets)
	c.Assert(err, qt.IsNil)
	c.Assert(settings, qt.DeepEquals, []pgrole.Setting{{Name: "work_mem", Value: "'64MB'"}})
}

// Each refused form is named; none is read as another statement.
func TestReadRoleSettings_FailurePath(t *testing.T) {
	for _, test := range []struct{ name, statement, want string }{
		{name: "in database", statement: "ALTER ROLE app IN DATABASE app SET work_mem = '1MB'", want: `.*ALTER ROLE \.\.\. IN DATABASE stores a default for one database, which Ptah does not model.*`},
		{name: "every role", statement: "ALTER ROLE ALL SET work_mem = '1MB'", want: `.*ALTER ROLE ALL stores a default for every role, which Ptah does not model.*`},
		{name: "every role in a database", statement: "ALTER ROLE ALL IN DATABASE app SET work_mem = '1MB'", want: `.*ALTER ROLE ALL stores a default for every role.*`},
		{name: "from current", statement: "ALTER ROLE app SET work_mem FROM CURRENT", want: `.*SET work_mem FROM CURRENT stores the value of the session that runs the statement.*`},
		{name: "reset all", statement: "ALTER ROLE app RESET ALL", want: `.*RESET ALL takes away settings the document does not name.*`},
		{name: "session role", statement: "ALTER ROLE CURRENT_USER SET work_mem = '1MB'", want: `.*ALTER ROLE CURRENT_USER names the role of the session that runs it.*`},
		{name: "user in database", statement: "ALTER USER app IN DATABASE app RESET work_mem", want: `.*ALTER USER \.\.\. IN DATABASE stores a default for one database.*`},
		{name: "unit without quotes", statement: "ALTER ROLE app SET statement_timeout = 30s", want: `.*a value with a unit is a string literal: '30s'`},
		{name: "no value", statement: "ALTER ROLE app SET work_mem =", want: `.*setting work_mem has no value`},
		{name: "role not declared", statement: "ALTER ROLE ghost SET work_mem = '1MB'", want: `.*role "ghost" is not declared in this schema`},
		{name: "case of a quoted role", statement: `ALTER ROLE "App" SET work_mem = '1MB'`, want: `.*role "App" is not declared in this schema`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database, _, err := sqlschema.ReadWith(c.Context(), []byte("CREATE ROLE app;\n"+test.statement+";\n"), "postgres", nil, sqlschema.Options{Owners: builtintest.Runtime().SQL()})
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(database, qt.DeepEquals, schemamodel.Database{})
		})
	}
}

// Every statement requires its role where it stands, as the server does, even
// though the last statement of the role carries the settings of all of them.
func TestReadRoleSettings_RoleMustExistAtEachStatement(t *testing.T) {
	c := qt.New(t)
	database, _, err := sqlschema.ReadWith(c.Context(), []byte(`ALTER ROLE app SET work_mem = '1MB';
CREATE ROLE app;
ALTER ROLE app SET statement_timeout = '30s';`), "postgres", nil, sqlschema.Options{Owners: builtintest.Runtime().SQL()})
	c.Assert(err, qt.ErrorIs, sqlext.ErrUndeclared)
	c.Assert(err, qt.ErrorMatches, `line 1, column 1: ALTER ROLE app SET work_mem = '1MB': role "app" is not declared in this schema`)
	c.Assert(database, qt.DeepEquals, schemamodel.Database{})
}
