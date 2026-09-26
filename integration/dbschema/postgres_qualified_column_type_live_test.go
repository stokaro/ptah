//go:build integration

package dbschema_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/dbschema"
	"ptah.run/internal/schemafile"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// planAcrossSchemas plans the document against both schemas it declares
// objects in, the table's and the one holding its types.
func planAcrossSchemas(c *qt.C, conn *dbschema.DatabaseConnection, schemaName, document string) []string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(fmt.Sprintf(document, schemaName)), 0o600), qt.IsNil)
	declared, err := schemafile.LoadSources([]schemafile.Source{{URL: path}}, schemafile.Options{Dialect: platform.Postgres})
	c.Assert(err, qt.IsNil)
	live, err := dbschema.ReadSchemaWithSchemasContext(c.Context(), conn, []string{schemaName, schemaName + "_types"})
	c.Assert(err, qt.IsNil)
	diff, err := schemadiff.CompareWithDatabase(c.Context(), conn, declared, live, nil)
	c.Assert(err, qt.IsNil)
	statements, err := planner.GenerateSchemaDiffSQLStatements(diff, conn.Info().Dialect)
	c.Assert(err, qt.IsNil)
	return statements
}

// A column whose type lives in a named schema is written with the schema, as
// pg_dump writes every type outside the search path. The document reads, the
// table is created with that type, and planning again finds nothing to do.
func TestPostgresLiveSQLDocumentQualifiedColumnTypesConverge(t *testing.T) {
	tests := []struct {
		name    string
		types   string
		columns string
	}{
		{
			name:    "an enum in the table's own schema",
			types:   `CREATE TYPE "%[1]s".mood AS ENUM ('calm', 'busy');`,
			columns: `m "%[1]s".mood NOT NULL DEFAULT 'calm'`,
		},
		{
			name:    "an enum and its array in another schema",
			types:   `CREATE SCHEMA "%[1]s_types"; CREATE TYPE "%[1]s_types".mood AS ENUM ('calm', 'busy');`,
			columns: `m "%[1]s_types".mood, ms "%[1]s_types".mood[]`,
		},
		{
			name:    "a domain in another schema",
			types:   `CREATE SCHEMA "%[1]s_types"; CREATE DOMAIN "%[1]s_types".positive AS integer CHECK (VALUE > 0);`,
			columns: `p "%[1]s_types".positive`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn, schemaName := newFormsSchema(c)
			c.Cleanup(func() {
				_, _ = conn.ExecContext(context.Background(), `DROP SCHEMA IF EXISTS "`+schemaName+`_types" CASCADE`)
			})

			document := test.types + "\n" + `CREATE TABLE "%[1]s".typed (id integer PRIMARY KEY, ` + test.columns + `);`
			statements := planAcrossSchemas(c, conn, schemaName, document)
			c.Assert(statements, qt.Not(qt.HasLen), 0)
			for _, statement := range statements {
				_, err := conn.ExecContext(c.Context(), statement)
				c.Assert(err, qt.IsNil, qt.Commentf("statement:\n%s", statement))
			}

			c.Assert(planAcrossSchemas(c, conn, schemaName, document), qt.HasLen, 0,
				qt.Commentf("the document plans again after its own statements ran"))
		})
	}
}

// The control: a column moved to a type of the same name in another schema is
// a different type, and the plan changes it.
func TestPostgresLiveSQLDocumentQualifiedColumnTypeChangePlans(t *testing.T) {
	c := qt.New(t)
	conn, schemaName := newFormsSchema(c)
	c.Cleanup(func() {
		_, _ = conn.ExecContext(context.Background(), `DROP SCHEMA IF EXISTS "`+schemaName+`_types" CASCADE`)
	})
	types := `CREATE SCHEMA "%[1]s_types"; CREATE TYPE "%[1]s_types".mood AS ENUM ('calm', 'busy');` +
		`CREATE TYPE "%[1]s".mood AS ENUM ('calm', 'busy');`
	before := types + `CREATE TABLE "%[1]s".typed (id integer PRIMARY KEY, m "%[1]s".mood);`
	for _, statement := range planAcrossSchemas(c, conn, schemaName, before) {
		_, err := conn.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("statement:\n%s", statement))
	}

	statements := planAcrossSchemas(c, conn, schemaName,
		types+`CREATE TABLE "%[1]s".typed (id integer PRIMARY KEY, m "%[1]s_types".mood);`)

	c.Assert(statements, qt.Not(qt.HasLen), 0)
	c.Assert(fmt.Sprint(statements), qt.Contains, `ALTER COLUMN "m" TYPE "`+schemaName+`_types".mood`)
}
