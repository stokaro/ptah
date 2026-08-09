package postgres_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/migration/planner"
	"go.5x5.cz/ptah/migration/schemadiff/types"
)

// TestPlannerDoesNotWriteToARelationTheSchemaNeverDeclaredLive is blocker 1
// asserted where it is observable.
//
// The statement the third resolution tier produced is one perfectly valid
// `ALTER TABLE "app"."users" ADD COLUMN "note" TEXT NOT NULL`. It exits 0. No
// assertion on the SQL text can distinguish it from a correct migration, and no
// error surfaces: the only place that says the column landed on a relation the
// desired schema never declared is information_schema.columns.
//
// The database carries BOTH `app.users` and `reporting.users`, which is what
// makes the wrong answer apply cleanly rather than fail. The desired schema
// declares exactly one `users` table, in `reporting`.
func TestPlannerDoesNotWriteToARelationTheSchemaNeverDeclaredLive(t *testing.T) {
	c := qt.New(t)
	adminURL := livePostgresURLForRLSEnable(t)

	tests := []struct {
		name string
		// declaredSchema is the schema the desired `users` table declares.
		// Empty means the declaration leaves the schema to the search path.
		declaredSchema string
		// wantAppColumns is what app.users holds after the plan runs.
		wantAppColumns []string
	}{
		{
			// The measured defect. `reporting` is not `app`, both are stated,
			// and the planner must produce nothing rather than write the
			// reporting definition onto the app relation.
			name:           "a table declared in reporting gets no column DDL on app",
			declaredSchema: "reporting",
			wantAppColumns: []string{"id"},
		},
		{
			// The control the gate must not swallow: a declaration that states
			// no schema at all is the case tier 3 exists for (stokaro/ptah#1287),
			// and the column has to land.
			name:           "a table declared without a schema still gets its column",
			declaredSchema: "",
			wantAppColumns: []string{"id", "note"},
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			dbURL := createRLSEnableDatabase(c, adminURL)
			executeSQL(c, dbURL, []string{
				`CREATE SCHEMA app`,
				`CREATE SCHEMA reporting`,
				`CREATE TABLE app.users (id integer PRIMARY KEY)`,
				`CREATE TABLE reporting.users (id integer PRIMARY KEY, note text)`,
			})
			c.Assert(schemaTableColumns(c, dbURL, "app", "users"), qt.DeepEquals, []string{"id"})

			generated := &goschema.Database{
				Tables: []goschema.Table{{
					StructName: "User",
					Name:       "users",
					Schema:     test.declaredSchema,
				}},
				Fields: []goschema.Field{
					{StructName: "User", Name: "id", Type: "INTEGER", Primary: true},
					{StructName: "User", Name: "note", Type: "TEXT"},
				},
			}
			diff := &types.SchemaDiff{TablesModified: []types.TableDiff{{
				TableName:    "app.users",
				ColumnsAdded: []string{"note"},
			}}}

			statements, err := planner.GenerateSchemaDiffSQLStatements(diff, generated, "postgres")
			c.Assert(err, qt.IsNil)
			c.Logf("plan:\n%s", strings.Join(statements, "\n"))
			executeSQL(c, dbURL, statements)

			c.Assert(schemaTableColumns(c, dbURL, "app", "users"), qt.DeepEquals, test.wantAppColumns)
			// reporting.users is the table the desired schema actually declares
			// and nothing in this plan touches it.
			c.Assert(schemaTableColumns(c, dbURL, "reporting", "users"), qt.DeepEquals, []string{"id", "note"})
		})
	}
}

// schemaTableColumns reports the columns one relation holds, sorted by name.
func schemaTableColumns(c *qt.C, dbURL, schema, table string) []string {
	c.Helper()
	return queryStrings(c, dbURL,
		`SELECT column_name FROM information_schema.columns
		  WHERE table_schema = '`+schema+`' AND table_name = '`+table+`'
		  ORDER BY column_name`)
}
