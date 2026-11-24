package planner_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/engine/builtin"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/sqlschema"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// declaredGrantOptionRevoke grants SELECT on table to a role the schema does
// not manage and takes the grant option the GRANT gives.
func declaredGrantOptionRevoke(table string) string {
	return "GRANT SELECT ON " + table + " TO u48 WITH GRANT OPTION;\n" +
		"REVOKE GRANT OPTION FOR SELECT ON " + table + " FROM u48;\n"
}

// A declared REVOKE GRANT OPTION FOR, read from a SQL schema file, takes the
// option a role the schema does not manage holds on every target that keeps
// one, in the statement that target takes it with, and keeps the privilege.
// MySQL and MariaDB keep one option per grantee and object, so their
// statement names no privilege; SQL Server takes the option with CASCADE. A
// ClickHouse grant names its database.
func TestGenerateSchemaDiffSQLStatements_TakesADeclaredGrantOption_HappyPath(t *testing.T) {
	tests := []struct {
		dialect string
		table   string
		schema  string
		want    []string
	}{
		{dialect: platform.Postgres, table: "t48", want: []string{`REVOKE GRANT OPTION FOR SELECT ON TABLE "t48" FROM "u48"`}},
		{dialect: platform.CockroachDB, table: "t48", want: []string{`REVOKE GRANT OPTION FOR SELECT ON TABLE "t48" FROM "u48"`}},
		{dialect: platform.YugabyteDB, table: "t48", want: []string{`REVOKE GRANT OPTION FOR SELECT ON TABLE "t48" FROM "u48"`}},
		{dialect: platform.ClickHouse, table: "app.t48", schema: "app", want: []string{"REVOKE GRANT OPTION FOR SELECT ON `app`.`t48` FROM `u48`"}},
		{dialect: platform.MySQL, table: "t48", want: []string{"REVOKE GRANT OPTION ON `t48` FROM `u48`"}},
		{dialect: platform.MariaDB, table: "t48", want: []string{"REVOKE GRANT OPTION ON `t48` FROM `u48`"}},
		{dialect: platform.SQLServer, table: "t48", want: []string{"REVOKE GRANT OPTION FOR SELECT ON [t48] FROM [u48] CASCADE"}},
	}
	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)
			desired, _, err := sqlschema.ReadWith(t.Context(), []byte(declaredGrantOptionRevoke(test.table)), test.dialect, nil,
				sqlschema.Options{Owners: builtintest.Runtime().SQL()})
			c.Assert(err, qt.IsNil)
			database := &catalog.Database{Grants: []catalog.Grant{
				{Role: "u48", Privilege: "SELECT", ObjectType: "TABLE", Schema: test.schema, ObjectName: strings.TrimPrefix(test.table, test.schema+"."), WithOption: true},
			}}
			diff := must.Must(schemadiff.CompareWithDialect(t.Context(), &desired, database, test.dialect, must.Must(builtin.New())))

			statements, err := planner.GenerateSchemaDiffSQLStatements(t.Context(), must.Must(builtin.New()), diff, test.dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.DeepEquals, test.want)
		})
	}
}

// SQLite has no roles, and the declaration is refused by name rather than
// read as a plan that grants nothing.
func TestGenerateSchemaDiffSQLStatements_TakesADeclaredGrantOption_FailurePath(t *testing.T) {
	c := qt.New(t)
	desired, _, err := sqlschema.ReadWith(t.Context(), []byte(declaredGrantOptionRevoke("t48")), platform.SQLite, nil,
		sqlschema.Options{Owners: builtintest.Runtime().SQL()})
	c.Assert(err, qt.IsNil)
	database := &catalog.Database{Grants: []catalog.Grant{
		{Role: "u48", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "t48", WithOption: true},
	}}
	diff := must.Must(schemadiff.CompareWithDialect(t.Context(), &desired, database, platform.SQLite, must.Must(builtin.New())))

	statements, err := planner.GenerateSchemaDiffSQLStatements(t.Context(), must.Must(builtin.New()), diff, platform.SQLite)

	c.Assert(err, qt.ErrorMatches, `sqlite: roles and grants are not supported: u48`)
	c.Assert(statements, qt.IsNil)
}
