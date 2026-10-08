//go:build integration

package gonative_test

import (
	"database/sql"
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/sqlutil"
	"ptah.run/dbschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/convert/dbschematogo"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/schemadiff"
)

func TestPostgreSQLConstraintOwnersSurviveSQLExport(t *testing.T) {
	c := qt.New(t)
	dsn := dbtarget.URL(c, dbtarget.PostgreSQL)
	sourceURL := scratchPostgresDatabase(c, dsn)
	targetURL := scratchPostgresDatabase(c, dsn)
	source, err := dbschema.ConnectToDatabase(c.Context(), sourceURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(source.Close(), qt.IsNil) })
	target, err := dbschema.ConnectToDatabase(c.Context(), targetURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(target.Close(), qt.IsNil) })
	sourceDB, err := sql.Open("pgx", sourceURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(sourceDB.Close(), qt.IsNil) })
	targetDB, err := sql.Open("pgx", targetURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(targetDB.Close(), qt.IsNil) })
	_, err = sourceDB.ExecContext(c.Context(), "CREATE SCHEMA app")
	c.Assert(err, qt.IsNil)
	for _, schema := range []string{"app", "public"} {
		_, err = sourceDB.ExecContext(c.Context(), fmt.Sprintf(
			`CREATE TABLE %s.parents (tenant_id integer, id integer, PRIMARY KEY (tenant_id, id));
			 CREATE TABLE %s.children (tenant_id integer, id integer,
			 CONSTRAINT children_parent FOREIGN KEY (tenant_id, id) REFERENCES %s.parents (tenant_id, id));`,
			schema, schema, schema))
		c.Assert(err, qt.IsNil)
	}

	observed, err := dbschema.ReadSchemaWithSchemasContext(c.Context(), source, []string{"app", "public"})
	c.Assert(err, qt.IsNil)
	// Roles belong to the shared server; this round trip replays database objects.
	observed.Roles = nil
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	model, err := dbschematogo.ConvertDBSchemaToGoSchema(c.Context(), observed, platform.Postgres, runtime)
	c.Assert(err, qt.IsNil)
	statements, err := builtin.GetOrderedCreateStatementsWithCapabilities(model, platform.Postgres, source.Info().Capabilities)
	c.Assert(err, qt.IsNil)
	for _, statement := range statements {
		for _, sqlStatement := range sqlutil.SplitStatements(statement) {
			_, err = targetDB.ExecContext(c.Context(), sqlStatement)
			c.Assert(err, qt.IsNil, qt.Commentf("statement: %s", sqlStatement))
		}
	}
	readBack, err := dbschema.ReadSchemaWithSchemasContext(c.Context(), target, []string{"app", "public"})
	c.Assert(err, qt.IsNil)
	readBack.Roles = nil
	diff, err := schemadiff.CompareWithDialect(c.Context(), model, readBack, platform.Postgres, runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(diff.HasChanges(), qt.IsFalse)

	for _, test := range []struct {
		schema string
		id     int
	}{
		{schema: "app", id: 1},
		{schema: "public", id: 2},
	} {
		t.Run(test.schema, func(t *testing.T) {
			c := qt.New(t)
			var count int
			err := targetDB.QueryRowContext(c.Context(), `SELECT count(*) FROM pg_constraint
				WHERE conrelid = $1::regclass AND confrelid = $2::regclass AND conname = 'children_parent'`,
				test.schema+".children", test.schema+".parents").Scan(&count)
			c.Assert(err, qt.IsNil)
			c.Assert(count, qt.Equals, 1)
			_, err = targetDB.ExecContext(c.Context(), fmt.Sprintf("INSERT INTO %s.parents VALUES (1, %d)", test.schema, test.id))
			c.Assert(err, qt.IsNil)
			_, err = targetDB.ExecContext(c.Context(), fmt.Sprintf("INSERT INTO %s.children VALUES (1, %d)", test.schema, test.id))
			c.Assert(err, qt.IsNil)
			_, err = targetDB.ExecContext(c.Context(), fmt.Sprintf("INSERT INTO %s.children VALUES (1, %d)", test.schema, 3-test.id))
			c.Assert(err, qt.ErrorMatches, `(?s).*violates foreign key constraint "children_parent".*`)
		})
	}
}
