//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/dbschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/dbtarget"
	"go.5x5.cz/ptah/migration/planner"
	"go.5x5.cz/ptah/migration/schemadiff"
)

// TestPostgresViewDependencyOrderE2E proves the plan a view-on-view schema
// produces actually applies.
//
// The ordering is derived by scanning a view's body for references to the other
// declarations, and that scan needs the target's quoting rules to recognize a
// qualified name written the way PostgreSQL renders one. The PostgreSQL planner
// asked for the ordering WITHOUT its dialect, so `"analytics"."base"` matched no
// declaration, the dependent view gained no edge, and it was created first.
//
// That renders cleanly and fails when it runs, which is why this is measured by
// executing the plan rather than by reading it (stokaro/ptah#1664).
func TestPostgresViewDependencyOrderE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)

	tests := []struct {
		name       string
		summaryRef string
	}{
		{
			// The spelling that used to break: quoted and qualified, which is
			// what pg_get_viewdef renders and what an author copying it writes.
			name:       "a quoted qualified reference",
			summaryRef: `"analytics"."zbase"`,
		},
		{
			name:       "an unquoted qualified reference",
			summaryRef: "analytics.zbase",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()

			adminDB, err := sql.Open("pgx", dbURL)
			c.Assert(err, qt.IsNil)
			defer adminDB.Close()

			testDBName := fmt.Sprintf("ptah_view_order_e2e_%d", time.Now().UnixNano())
			createE2EDatabase(c, ctx, adminDB, testDBName)
			defer dropE2EDatabase(c, context.Background(), adminDB, testDBName)

			scopedURL := replaceDatabaseName(c, dbURL, testDBName)
			setupDB, err := sql.Open("pgx", scopedURL)
			c.Assert(err, qt.IsNil)
			defer setupDB.Close()
			_, err = setupDB.ExecContext(ctx, "CREATE SCHEMA analytics")
			c.Assert(err, qt.IsNil)
			_, err = setupDB.ExecContext(ctx, "CREATE TABLE analytics.events (id integer)")
			c.Assert(err, qt.IsNil)

			conn, err := dbschema.ConnectToDatabase(ctx, scopedURL)
			c.Assert(err, qt.IsNil)
			defer dbschema.CloseAndWarn(conn)
			read, err := conn.Reader().ReadSchema()
			c.Assert(err, qt.IsNil)

			// The names are chosen so ALPHABETICAL order is the WRONG order:
			// the dependent `asummary` sorts before the `zbase` it reads, and
			// the plan sorts its added views. So nothing but the dependency
			// edge can put the base view ahead of it, which is what makes this
			// test able to fail. With names that sorted correctly it passed
			// even when the ordering ignored the dialect entirely.
			declared := &goschema.Database{
				Tables: existingTablesOf(read),
				Views: []goschema.View{
					{Name: "analytics.asummary", Body: "SELECT id FROM " + test.summaryRef},
					{Name: "analytics.zbase", Body: "SELECT id FROM analytics.events"},
				},
			}

			diff := schemadiff.CompareWithDialect(declared, read, "postgres")
			statements, err := planner.GenerateSchemaDiffSQLStatements(diff, declared, "postgres")
			c.Assert(err, qt.IsNil)

			// The judge is the server: a plan that creates the dependent view
			// first cannot apply, whatever the statement list looks like.
			for _, statement := range statements {
				_, execErr := setupDB.ExecContext(ctx, statement)
				c.Assert(execErr, qt.IsNil, qt.Commentf("statement: %s", statement))
			}

			var count int
			c.Assert(setupDB.QueryRowContext(ctx,
				`SELECT count(*) FROM information_schema.views WHERE table_schema = 'analytics'`,
			).Scan(&count), qt.IsNil)
			c.Assert(count, qt.Equals, 2)
		})
	}
}

// existingTablesOf carries the tables already in the database into the desired
// schema, so the comparison plans views and nothing else.
func existingTablesOf(read *dbschematypes.DBSchema) []goschema.Table {
	tables := make([]goschema.Table, 0, len(read.Tables))
	for _, table := range read.Tables {
		tables = append(tables, goschema.Table{
			StructName: table.Name, Name: table.Name, Schema: table.Schema,
		})
	}
	return tables
}
