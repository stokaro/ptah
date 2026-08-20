//go:build integration

package dbschema_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/dbschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/atlasschema"
	"go.5x5.cz/ptah/internal/dbtarget"
	"go.5x5.cz/ptah/migration/migrator"
	"go.5x5.cz/ptah/migration/schemadiff"
)

// Spanner had capability coverage on every pull request and no end-to-end
// suite, so nothing exercised render, plan, apply and read together the way
// every other engine has. Two defects lived in that gap, and both are asserted
// here (stokaro/ptah#1719).

// spannerLiveSchema declares a table and a secondary index.
//
// The varchar column is the point of the second assertion below: Spanner's
// catalog reports data_type "character varying" with udt_name EMPTY, where
// PostgreSQL answers "varchar" and never reaches that arm of the comparison.
func spannerLiveSchema(table string) *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{{StructName: "Author", Name: table}},
		Fields: []goschema.Field{
			{StructName: "Author", Name: "id", Type: "bigint", Primary: true},
			{StructName: "Author", Name: "name", Type: "varchar(200)", Nullable: false},
		},
		Indexes: []goschema.Index{{
			StructName: "Author",
			Name:       "idx_" + table + "_name",
			Fields:     []string{"name"},
		}},
	}
}

// TestSpannerLiveSchemaRoundTrip is the convergence assertion the engine had no
// suite to make: apply, read the catalog back, compare, and require nothing
// left to do.
func TestSpannerLiveSchemaRoundTrip(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.Spanner)
	c := qt.New(t)
	ctx := t.Context()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	table := fmt.Sprintf("ptah_sp_%d", time.Now().UnixNano())
	description := spannerLiveSchema(table)
	defer func() {
		_, _ = conn.ExecContext(context.Background(), `DROP INDEX IF EXISTS "idx_`+table+`_name"`)
		_, _ = conn.ExecContext(context.Background(), `DROP TABLE IF EXISTS "`+table+`"`)
	}()

	// 1. The renderer's statements are what the server is given. Spanner
	// refuses DDL inside an explicit transaction, so each runs on its own.
	statements, err := renderer.GetOrderedCreateStatements(description, platform.Spanner)
	c.Assert(err, qt.IsNil)
	for _, statement := range statements {
		_, execErr := conn.ExecContext(ctx, statement)
		c.Assert(execErr, qt.IsNil, qt.Commentf("statement:\n%s", statement))
	}

	// 2. The read returns the table, its primary key and the secondary index,
	// through the SQL-standard catalog rather than pg_index.
	live, err := dbschema.ReadSchemaWithSchemas(conn, []string{"public"})
	c.Assert(err, qt.IsNil)
	c.Assert(spannerLiveTableNames(live.Tables), qt.Contains, table)

	// 3. Convergence. Without the `character varying` fold this reports a type
	// change on every run, and applying it fails: Spanner refuses to alter a
	// column an index refers to.
	settled := schemadiff.CompareWithDialect(description, live, platform.Spanner)
	c.Assert(settled.TablesModified, qt.HasLen, 0,
		qt.Commentf("a second run must have nothing to do"))
	c.Assert(settled.TablesAdded, qt.HasLen, 0)
	c.Assert(settled.IndexesAdded, qt.HasLen, 0)
}

// TestSpannerLiveRefusesDDLInsideATransaction is the fact the apply path now
// reads off a capability instead of discovering at runtime.
//
// capability.DDLInsideTransaction was already false for this target; nothing
// consulted it, so `schema apply` opened a transaction and failed on its first
// statement. This asserts the server really does refuse, so the capability is
// a measurement rather than a belief.
func TestSpannerLiveRefusesDDLInsideATransaction(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.Spanner)
	c := qt.New(t)
	ctx := t.Context()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	table := fmt.Sprintf("ptah_sp_tx_%d", time.Now().UnixNano())
	defer func() {
		_, _ = conn.ExecContext(context.Background(), `DROP TABLE IF EXISTS "`+table+`"`)
	}()

	tx, err := conn.SchemaWriter().BeginTransaction(ctx)
	c.Assert(err, qt.IsNil)
	execErr := tx.ExecuteSQL(ctx, `CREATE TABLE "`+table+`" ("id" bigint PRIMARY KEY)`)
	_ = tx.Rollback()

	c.Assert(execErr, qt.IsNotNil,
		qt.Commentf("the capability says this target refuses DDL inside a transaction"))
	c.Assert(execErr.Error(), qt.Contains, "outside explicit transactions")
}

// spannerLiveTableNames lists the table names a catalog read reported.
func spannerLiveTableNames(tables []dbschematypes.DBTable) []string {
	names := make([]string, 0, len(tables))
	for _, table := range tables {
		names = append(names, table.Name)
	}
	return names
}

// TestSpannerLiveApplyPathRunsOutsideATransaction is the assertion the
// round trip above could not make: it renders and execs statements itself, so
// it never reaches the apply path that opened the transaction.
//
// This goes through atlasschema.ApplyStatements with the DEFAULT per-file
// transaction mode -- the mode an operator gets without asking -- which is
// exactly the call that failed on Spanner before the capability was consulted.
func TestSpannerLiveApplyPathRunsOutsideATransaction(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.Spanner)
	c := qt.New(t)
	ctx := t.Context()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	table := fmt.Sprintf("ptah_sp_apply_%d", time.Now().UnixNano())
	defer func() {
		_, _ = conn.ExecContext(context.Background(), `DROP TABLE IF EXISTS "`+table+`"`)
	}()

	err = atlasschema.ApplyStatements(ctx, conn, migrator.MigrationTxModeFile, []string{
		`CREATE TABLE "` + table + `" ("id" bigint PRIMARY KEY)`,
	})

	c.Assert(err, qt.IsNil, qt.Commentf("the default transaction mode must not reach this target"))
	live, readErr := dbschema.ReadSchemaWithSchemas(conn, []string{"public"})
	c.Assert(readErr, qt.IsNil)
	c.Assert(spannerLiveTableNames(live.Tables), qt.Contains, table)
}
