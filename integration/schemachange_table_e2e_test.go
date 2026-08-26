//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/dbtarget"
	"go.5x5.cz/ptah/internal/schemachange"
)

// TestSchemaChangeTablePipelinePostgresE2E applies the canonical path's table
// and column plan to a real PostgreSQL and reads the result back out of the
// catalog.
//
// Every offline test for this family asserts against a fixture somebody wrote,
// and the statement-level differential test asserts against another renderer's
// text. Both can agree on SQL no server accepts. This is the test that cannot:
// the statements are executed, and the catalog is asked what they produced
// rather than the plan being trusted (stokaro/ptah#1662).
func TestSchemaChangeTablePipelinePostgresE2E(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()

	adminURL := dbtarget.URL(c.TB, dbtarget.PostgreSQL)
	adminDB, err := sql.Open("pgx", adminURL)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()

	name := fmt.Sprintf("ptah_table_slice_e2e_%d", time.Now().UnixNano())
	createE2EDatabase(c, ctx, adminDB, name)
	defer dropE2EDatabase(c, context.Background(), adminDB, name)

	db, err := sql.Open("pgx", replaceDatabaseName(c, adminURL, name))
	c.Assert(err, qt.IsNil)
	defer db.Close()

	profile := livePostgresProfile()

	// The creation, columns and key included.
	creation := planFor(c, liveWidget(nil), &catalog.Database{}, profile)
	c.Assert(creation, qt.HasLen, 1)
	execute(c, ctx, db, creation)
	// The TYPES as well as the names: a CREATE TABLE that rendered every
	// column as text is valid SQL the server accepts, so a test reading only
	// the names would call it a pass.
	c.Assert(liveColumns(c, ctx, db, "widget"), qt.DeepEquals,
		[]string{"code text", "id integer"})
	c.Assert(livePrimaryKeyColumns(c, ctx, db, "widget"), qt.DeepEquals, []string{"id"})

	// A column the table does not have. It is nullable, so nothing about the
	// rows it will not have can stop it.
	addition := planFor(c, liveWidget([]schemamodel.Field{{
		StructName: "Widget", Name: "label", Type: "text", Nullable: true,
	}}), liveWidgetCatalog(nil), profile)
	c.Assert(addition, qt.HasLen, 1)
	execute(c, ctx, db, addition)
	c.Assert(liveColumns(c, ctx, db, "widget"), qt.DeepEquals, []string{"code text", "id integer", "label text"})

	// A column the desired schema no longer declares.
	removal := planFor(c, liveWidget(nil), liveWidgetCatalog([]catalog.Column{{
		Name: "label", DataType: "text", IsNullable: "YES",
	}}), profile)
	c.Assert(removal, qt.HasLen, 1)
	execute(c, ctx, db, removal)
	c.Assert(liveColumns(c, ctx, db, "widget"), qt.DeepEquals, []string{"code text", "id integer"})

	// The whole table.
	drop := planFor(c, &schemamodel.Database{}, liveWidgetCatalog(nil), profile)
	c.Assert(drop, qt.HasLen, 1)
	execute(c, ctx, db, drop)
	c.Assert(liveColumns(c, ctx, db, "widget"), qt.HasLen, 0)
}

// TestSchemaChangeNotNullColumnRefusalIsTheEnginesPostgresE2E pins that the
// blocked answer for a NOT NULL addition matches what the engine does.
//
// The offline rule reads the catalog's row estimate and blocks. Here the same
// statement is handed to PostgreSQL with a row in the table, which must refuse
// it -- otherwise the block is Ptah refusing something the target accepts.
func TestSchemaChangeNotNullColumnRefusalIsTheEnginesPostgresE2E(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()

	adminURL := dbtarget.URL(c.TB, dbtarget.PostgreSQL)
	adminDB, err := sql.Open("pgx", adminURL)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()

	name := fmt.Sprintf("ptah_table_notnull_e2e_%d", time.Now().UnixNano())
	createE2EDatabase(c, ctx, adminDB, name)
	defer dropE2EDatabase(c, context.Background(), adminDB, name)

	db, err := sql.Open("pgx", replaceDatabaseName(c, adminURL, name))
	c.Assert(err, qt.IsNil)
	defer db.Close()

	_, err = db.ExecContext(ctx, `CREATE TABLE widget (id integer PRIMARY KEY, code text)`)
	c.Assert(err, qt.IsNil)
	_, err = db.ExecContext(ctx, `INSERT INTO widget (id, code) VALUES (1, 'a')`)
	c.Assert(err, qt.IsNil)

	// The catalog as a read of THIS database reports it: one row, statistics
	// available.
	current := liveWidgetCatalog(nil)
	current.Tables[0].EstimatedRows = 1

	changes := changesFor(c, liveWidget([]schemamodel.Field{{
		StructName: "Widget", Name: "label", Type: "text",
	}}), current, livePostgresProfile())

	c.Assert(changes, qt.HasLen, 1)
	c.Assert(changes[0].Status, qt.Equals, schemachange.Blocked)

	// The engine's own answer to the statement the block prevented.
	_, engineErr := db.ExecContext(ctx, `ALTER TABLE "widget" ADD COLUMN "label" text NOT NULL`)

	c.Assert(engineErr, qt.IsNotNil)
	c.Assert(engineErr.Error(), qt.Contains, "contains null values")
}

// liveWidget is a table with a key, a column, and whatever else a row adds.
func liveWidget(extra []schemamodel.Field) *schemamodel.Database {
	fields := []schemamodel.Field{
		{StructName: "Widget", Name: "id", Type: "integer", Primary: true},
		{StructName: "Widget", Name: "code", Type: "text", Nullable: true},
	}
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Widget", Name: "widget"}},
		Fields: append(fields, extra...),
	}
}

// liveWidgetCatalog is that table as a catalog read reports it, plus whatever
// else a step has added.
func liveWidgetCatalog(extra []catalog.Column) *catalog.Database {
	columns := []catalog.Column{
		{Name: "id", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true},
		{Name: "code", DataType: "text", IsNullable: "YES"},
	}
	return &catalog.Database{
		Tables: []catalog.Table{{
			Name: "widget", Schema: "public", Columns: append(columns, extra...),
		}},
	}
}

// execute runs a plan's statements in the order the graph put them.
func execute(c *qt.C, ctx context.Context, db *sql.DB, operations []schemachange.PlannedOperation) {
	c.Helper()
	for _, operation := range operations {
		_, err := db.ExecContext(ctx, operation.SQL)
		c.Assert(err, qt.IsNil, qt.Commentf("statement: %s", operation.SQL))
	}
}

// liveColumns is what the catalog says the table has -- each column and its
// type -- sorted, so the assertion is about the set rather than about ordinal
// position.
func liveColumns(c *qt.C, ctx context.Context, db *sql.DB, table string) []string {
	c.Helper()
	rows, err := db.QueryContext(ctx, `
		SELECT column_name || ' ' || data_type FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = $1
		ORDER BY column_name`, table)
	c.Assert(err, qt.IsNil)
	defer rows.Close()
	names := make([]string, 0)
	for rows.Next() {
		var name string
		c.Assert(rows.Scan(&name), qt.IsNil)
		names = append(names, name)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return names
}

// livePrimaryKeyColumns is the key the server actually built, which is the half
// a CREATE TABLE can get wrong without failing.
func livePrimaryKeyColumns(c *qt.C, ctx context.Context, db *sql.DB, table string) []string {
	c.Helper()
	rows, err := db.QueryContext(ctx, `
		SELECT a.attname
		FROM pg_index i
		JOIN pg_class t ON t.oid = i.indrelid
		JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY (i.indkey)
		WHERE t.relname = $1 AND i.indisprimary
		ORDER BY a.attname`, table)
	c.Assert(err, qt.IsNil)
	defer rows.Close()
	names := make([]string, 0)
	for rows.Next() {
		var name string
		c.Assert(rows.Scan(&name), qt.IsNil)
		names = append(names, name)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return names
}
