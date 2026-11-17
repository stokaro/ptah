package postgres

// White-box testing required: readRoleSettings, parseRoleConfig and
// unmodeledRoleSettings are unexported, and the exported ReadSchema path
// reaches them only through a live server. What is under test includes rows
// a live server rarely holds -- a setconfig entry this reader cannot parse,
// for a role the read did not find -- and the statement's scope, neither of
// which a live read can be made to show on demand.

import (
	"database/sql/driver"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/postgres/pgrole"
	"ptah.run/internal/dbschema/dbtest"
)

var roleSettingColumns = []string{"rolname", "in_database", "datname", "setconfig"}

// answerRoleSettings plays PostgreSQL for the role-setting read, answering
// rows and recording each statement it was asked.
func answerRoleSettings(c *qt.C, rows [][]driver.Value, queries *[]string) *Reader {
	c.Helper()
	db := dbtest.Open(c, func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
		*queries = append(*queries, query)
		return dbtest.QueryResult{Columns: roleSettingColumns, Rows: rows}, nil
	})
	return NewPostgreSQLReaderWithCapabilities(db.SQL, "public", capability.Postgres16())
}

// settingsOf reads the observed settings a role carries.
func settingsOf(c *qt.C, role catalog.Role) []pgrole.Setting {
	c.Helper()
	settings, _, err := pgrole.SettingsOf(role.Facets)
	c.Assert(err, qt.IsNil)
	return settings
}

func TestReadRoleSettings_HappyPath(t *testing.T) {
	c := qt.New(t)
	var queries []string
	reader := answerRoleSettings(c, [][]driver.Value{
		{"app", false, "", `["statement_timeout=30s", "search_path=\"$user\", public"]`},
		{"app", true, "appdb", `["lock_timeout=5s"]`},
		{"other", false, "", `["work_mem=8MB"]`},
		{"stranger", false, "", `["not a setting"]`},
		{"", true, "appdb", `["app.region=eu"]`},
		{"", false, "", `["app.tenant=acme"]`},
	}, &queries)
	database := &catalog.Database{Roles: []catalog.Role{{Name: "app"}, {Name: "bare"}}, RolesOutOfScope: []catalog.Role{{Name: "other"}}}

	err := reader.readRoleSettings(t.Context(), database)

	c.Assert(err, qt.IsNil)
	c.Assert(queries, qt.HasLen, 1)
	// Rows for another database do not apply to this one, so the statement
	// asks only for the defaults everywhere and those of this database.
	c.Assert(queries[0], qt.Contains, "WHERE s.setdatabase = 0 OR d.datname = current_database()")
	c.Assert(settingsOf(c, database.Roles[0]), qt.DeepEquals, []pgrole.Setting{
		{Name: "search_path", Value: `"$user", public`}, {Name: "statement_timeout", Value: "30s"},
	})
	c.Assert(database.Roles[1].Facets.IsZero(), qt.IsTrue)
	c.Assert(settingsOf(c, database.RolesOutOfScope[0]), qt.DeepEquals, []pgrole.Setting{{Name: "work_mem", Value: "8MB"}})
	c.Assert(database.UnmodeledObjects, qt.DeepEquals, []catalog.UnmodeledObject{
		{Kind: "role setting", Name: "lock_timeout for role app in database appdb",
			Reason: "ALTER ROLE IN DATABASE stores a default for one database; Ptah models the defaults a role holds in every database"},
		{Kind: "role setting", Name: "app.region for every role in database appdb",
			Reason: "ALTER DATABASE SET and ALTER ROLE ALL IN DATABASE store a default for every role in one database, which Ptah does not model"},
		{Kind: "role setting", Name: "app.tenant for every role",
			Reason: "ALTER ROLE ALL SET stores a default for every role, which Ptah does not model"},
	})
	known, err := pgrole.SettingsCoverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, nil)
	c.Assert(err, qt.IsNil)
	c.Assert(database.FeatureCoverage, qt.DeepEquals, known)
}

// Only PostgreSQL takes the query; another PostgreSQL-wire product reads
// nothing and claims no knowledge of settings.
func TestReadRoleSettings_ReadsNothingOffPostgreSQL(t *testing.T) {
	c := qt.New(t)
	var queries []string
	db := dbtest.Open(c, func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
		queries = append(queries, query)
		return dbtest.QueryResult{Columns: roleSettingColumns}, nil
	})
	reader := NewPostgreSQLWireReaderWithCapabilities(db.SQL, "public", platform.CockroachDB, capability.Postgres16())
	database := &catalog.Database{Roles: []catalog.Role{{Name: "app"}}}

	err := reader.readRoleSettings(t.Context(), database)

	c.Assert(err, qt.IsNil)
	c.Assert(queries, qt.HasLen, 0)
	c.Assert(database.FeatureCoverage, qt.DeepEquals, schemaext.Coverage{})
}

func TestReadRoleSettings_FailurePath(t *testing.T) {
	t.Run("a value of a role the read found", func(t *testing.T) {
		c := qt.New(t)
		var queries []string
		reader := answerRoleSettings(c, [][]driver.Value{{"app", false, "", `["not a setting"]`}}, &queries)
		database := &catalog.Database{Roles: []catalog.Role{{Name: "app"}}}

		err := reader.readRoleSettings(t.Context(), database)

		c.Assert(err, qt.ErrorMatches, `read role settings of "app": setconfig entry "not a setting" is not name=value`)
		c.Assert(database.Roles[0].Facets.IsZero(), qt.IsTrue)
	})
	t.Run("the query", func(t *testing.T) {
		c := qt.New(t)
		refused := errors.New("permission denied for table pg_db_role_setting")
		db := dbtest.Open(c, func(string, []driver.NamedValue) (dbtest.QueryResult, error) { return dbtest.QueryResult{}, refused })
		reader := NewPostgreSQLReaderWithCapabilities(db.SQL, "public", capability.Postgres16())
		database := &catalog.Database{Roles: []catalog.Role{{Name: "app"}}}

		err := reader.readRoleSettings(t.Context(), database)

		c.Assert(err, qt.ErrorMatches, `read role settings: .*permission denied for table pg_db_role_setting`)
		c.Assert(database.FeatureCoverage, qt.DeepEquals, schemaext.Coverage{})
	})
}

func TestParseRoleConfig_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name, config string
		want         []pgrole.Setting
	}{
		{name: "empty", config: `[]`, want: make([]pgrole.Setting, 0)},
		{name: "a value with an equals sign", config: `["app.filter=a=b"]`, want: []pgrole.Setting{{Name: "app.filter", Value: "a=b"}}},
		{name: "a list as the server keeps it", config: `["search_path=\"$user\", public", "TimeZone=UTC"]`,
			want: []pgrole.Setting{{Name: "search_path", Value: `"$user", public`}, {Name: "TimeZone", Value: "UTC"}}},
		{name: "an empty value", config: `["app.tenant="]`, want: []pgrole.Setting{{Name: "app.tenant", Value: ""}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := parseRoleConfig(test.config)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

func TestParseRoleConfig_FailurePath(t *testing.T) {
	for _, test := range []struct{ name, config, want string }{
		{name: "not JSON", config: `{x`, want: `setconfig "\{x": .*`},
		{name: "no equals sign", config: `["work_mem"]`, want: `setconfig entry "work_mem" is not name=value`},
		{name: "no name", config: `["=8MB"]`, want: `setconfig entry "=8MB" is not name=value`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := parseRoleConfig(test.config)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(got, qt.IsNil)
		})
	}
}

func TestUnmodeledRoleSettings(t *testing.T) {
	for _, test := range []struct {
		name, role, database string
		want                 []catalog.UnmodeledObject
	}{
		{name: "nothing", role: "app", database: "appdb", want: make([]catalog.UnmodeledObject, 0)},
		{name: "one role in one database", role: "app", database: "appdb", want: []catalog.UnmodeledObject{
			{Kind: "role setting", Name: "work_mem for role app in database appdb",
				Reason: "ALTER ROLE IN DATABASE stores a default for one database; Ptah models the defaults a role holds in every database"},
			{Kind: "role setting", Name: "lock_timeout for role app in database appdb",
				Reason: "ALTER ROLE IN DATABASE stores a default for one database; Ptah models the defaults a role holds in every database"},
		}},
		{name: "every role in one database", database: "appdb", want: []catalog.UnmodeledObject{
			{Kind: "role setting", Name: "work_mem for every role in database appdb",
				Reason: "ALTER DATABASE SET and ALTER ROLE ALL IN DATABASE store a default for every role in one database, which Ptah does not model"},
			{Kind: "role setting", Name: "lock_timeout for every role in database appdb",
				Reason: "ALTER DATABASE SET and ALTER ROLE ALL IN DATABASE store a default for every role in one database, which Ptah does not model"},
		}},
		{name: "every role in every database", want: []catalog.UnmodeledObject{
			{Kind: "role setting", Name: "work_mem for every role", Reason: "ALTER ROLE ALL SET stores a default for every role, which Ptah does not model"},
			{Kind: "role setting", Name: "lock_timeout for every role", Reason: "ALTER ROLE ALL SET stores a default for every role, which Ptah does not model"},
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			settings := []pgrole.Setting{{Name: "work_mem", Value: "8MB"}, {Name: "lock_timeout", Value: "5s"}}[:len(test.want)]
			c.Assert(unmodeledRoleSettings(test.role, test.database, settings), qt.DeepEquals, test.want)
		})
	}
}
