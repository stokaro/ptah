//go:build integration

package dbschema_test

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// spannerRemovalPlan plans the removal of table and view: the live read is
// narrowed to the two of them and the table's indexes, and nothing is
// declared.
func spannerRemovalPlan(c *qt.C, conn *dbschema.DatabaseConnection, table, view string) []string {
	c.Helper()
	live := spannerLiveTable(c, conn, table)
	read, err := dbschema.ReadSchemaWithSchemasContext(c.Context(), conn, []string{"public"})
	c.Assert(err, qt.IsNil)
	live.Views = slices.DeleteFunc(slices.Clone(read.Views), func(v catalog.View) bool { return v.Name != view })
	info := conn.Info()
	engine := must.Must(builtin.New())
	diff, err := schemadiff.CompareWithDatabaseInfo(c.Context(), &schemamodel.Database{}, live, info, nil, engine)
	c.Assert(err, qt.IsNil)
	statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(
		c.Context(), engine, diff, info.Dialect, planner.Options{Capabilities: info.Capabilities},
	)
	c.Assert(err, qt.IsNil)
	return statements
}

// TestSpannerLiveRemovesATableWithItsIndexAndView removes a table that has a
// secondary index and a view reading it, and plans nothing after.
//
// The plan dropped the table and the view with CASCADE, which Spanner refuses
// whatever the object. Without CASCADE Spanner also refuses to drop a table
// that still has an index, so the index has to go first (stokaro/ptah#4362).
func TestSpannerLiveRemovesATableWithItsIndexAndView(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.Spanner)
	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(c.Context(), dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	suffix := time.Now().UnixNano()
	table := fmt.Sprintf("ptah_sp_drop_%d", suffix)
	index := fmt.Sprintf("ptah_sp_drop_idx_%d", suffix)
	view := fmt.Sprintf("ptah_sp_drop_v_%d", suffix)
	defer spannerDropTable(conn, table)
	defer spannerDropIndex(conn, index)
	defer spannerDropView(conn, view)

	spannerApply(c, conn, []string{
		`CREATE TABLE "` + table + `" ("id" BIGINT PRIMARY KEY, "kind" TEXT)`,
		`CREATE INDEX "` + index + `" ON "` + table + `" ("kind")`,
		`CREATE VIEW "` + view + `" SQL SECURITY INVOKER AS SELECT t.id FROM "` + table + `" t`,
	})

	planned := spannerRemovalPlan(c, conn, table, view)
	c.Assert(planned, qt.DeepEquals, []string{
		`DROP INDEX IF EXISTS "` + index + `"`,
		`DROP VIEW IF EXISTS "` + view + `"`,
		"-- WARNING: This will delete all data!\n" + `DROP TABLE IF EXISTS "` + table + `"`,
	})
	spannerApply(c, conn, planned)

	c.Assert(spannerRemovalPlan(c, conn, table, view), qt.HasLen, 0)
	c.Assert(spannerLiveTable(c, conn, table).Tables, qt.HasLen, 0)
}

// spannerDropIndex drops index, for cleanup.
func spannerDropIndex(conn *dbschema.DatabaseConnection, index string) {
	_, _ = conn.ExecContext(context.Background(), `DROP INDEX IF EXISTS "`+index+`"`)
}

// spannerDropView drops view, for cleanup.
func spannerDropView(conn *dbschema.DatabaseConnection, view string) {
	_, _ = conn.ExecContext(context.Background(), `DROP VIEW IF EXISTS "`+view+`"`)
}
