//go:build integration

package integration_test

import (
	"context"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/internal/atlasschema"
	"ptah.run/internal/dbtarget"
)

// Declared rows against ClickHouse, whose data modification is not the standard
// SQL migration/datadiff renders.
//
// The renderer writes `DELETE FROM <table> WHERE <key> = <literal>` with no
// dialect arm, and only the engine can say whether that is a statement it
// executes: an offline test reads the statement it just rendered and agrees
// with itself. ClickHouse takes it -- lightweight delete has been on by default
// since 23.3 -- so the delete direction of a declared row set works there, and
// this is the control that keeps it working.
//
// The update direction is absent on purpose. The same renderer writes a plain
// `UPDATE <table> SET ...`, which ClickHouse answers with
// `code: 48 ... Lightweight updates are not supported`; core/query refuses that
// spelling for the same engine with the same reason, and migration/datadiff
// neither refuses it nor spells it `ALTER TABLE <table> UPDATE`. A test of the
// update direction would be red against the product, not against the engine.

const clickHouseDeclaredRowsTable = "ptah_declared_rows_settings"

// TestDeclaredRowsRemoveOneRowOnClickHouseLive applies a declared row set into
// an empty ClickHouse database, then drops one row from the declaration and
// applies again.
//
// The second apply plans exactly the DELETE that reaches the withdrawn row, and
// the assertion is the table read back, not the statement list.
func TestDeclaredRowsRemoveOneRowOnClickHouseLive(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()

	conn := openClickHouseDeclaredRowsTarget(c, ctx)
	defer dbschema.CloseAndWarn(conn)
	dropClickHouseDeclaredRowsTable(c, ctx, conn)
	defer dropClickHouseDeclaredRowsTable(c, ctx, conn)

	applyClickHouseDeclaredRows(c, ctx, conn, clickHouseDeclaredRowsSchema(
		clickHouseSettingRow("one", "Retention window"),
		clickHouseSettingRow("two", "Batch size"),
	))
	c.Assert(clickHouseDeclaredRows(c, ctx, conn), qt.DeepEquals, []string{
		"one=Retention window", "two=Batch size",
	})

	applyClickHouseDeclaredRows(c, ctx, conn, clickHouseDeclaredRowsSchema(
		clickHouseSettingRow("one", "Retention window"),
	))
	c.Assert(clickHouseDeclaredRows(c, ctx, conn), qt.DeepEquals, []string{
		"one=Retention window",
	})
}

// TestDeclaredRowsConvergeOnClickHouseLive is the control for the test above: a
// second apply of an unchanged declaration plans nothing.
//
// Convergence is decided by comparing a declared value with what the driver read
// back, so the engine is the only thing that can answer it. Without it a
// comparison that never pairs anything would satisfy the delete assertion by
// planning a DELETE and an INSERT for every row on every apply.
func TestDeclaredRowsConvergeOnClickHouseLive(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()

	conn := openClickHouseDeclaredRowsTarget(c, ctx)
	defer dbschema.CloseAndWarn(conn)
	dropClickHouseDeclaredRowsTable(c, ctx, conn)
	defer dropClickHouseDeclaredRowsTable(c, ctx, conn)

	desired := clickHouseDeclaredRowsSchema(
		clickHouseSettingRow("one", "Retention window"),
		clickHouseSettingRow("two", "Batch size"),
	)
	applyClickHouseDeclaredRows(c, ctx, conn, desired)

	plan, err := atlasschema.PlanApply(ctx, conn, atlasschema.ApplyOptions{Desired: desired})
	c.Assert(err, qt.IsNil)
	c.Assert(plan.Statements(), qt.HasLen, 0)
}

func openClickHouseDeclaredRowsTarget(c *qt.C, ctx context.Context) *dbschema.DatabaseConnection {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(ctx, dbtarget.URL(c, dbtarget.ClickHouse))
	c.Assert(err, qt.IsNil)
	return conn
}

// dropClickHouseDeclaredRowsTable removes the table synchronously, so a run
// leaves the shared database as it found it and the next run creates the table
// rather than reading the previous run's rows.
func dropClickHouseDeclaredRowsTable(c *qt.C, ctx context.Context, conn *dbschema.DatabaseConnection) {
	c.Helper()
	_, err := conn.ExecContext(ctx, "DROP TABLE IF EXISTS "+clickHouseDeclaredRowsTable+" SYNC")
	c.Assert(err, qt.IsNil)
}

// applyClickHouseDeclaredRows runs the plan the way `ptah schema apply` does:
// statement by statement, in the order the plan carries them. A statement the
// engine refuses fails the test at that statement, which is the measurement.
func applyClickHouseDeclaredRows(
	c *qt.C,
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	desired *schemamodel.Database,
) {
	c.Helper()
	plan, err := atlasschema.PlanApply(ctx, conn, atlasschema.ApplyOptions{Desired: desired})
	c.Assert(err, qt.IsNil)
	for _, statement := range plan.Statements() {
		_, err := conn.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil, qt.Commentf("statement: %s", statement))
	}
}

// clickHouseDeclaredRows reads the table back as "code=label" strings, so one
// assertion covers the row set and the value each row holds.
func clickHouseDeclaredRows(c *qt.C, ctx context.Context, conn *dbschema.DatabaseConnection) []string {
	c.Helper()
	rows, err := conn.QueryContext(ctx,
		"SELECT code, label FROM "+clickHouseDeclaredRowsTable+" ORDER BY code")
	c.Assert(err, qt.IsNil)
	defer rows.Close()

	var found []string
	for rows.Next() {
		code, label := "", ""
		c.Assert(rows.Scan(&code, &label), qt.IsNil)
		found = append(found, code+"="+label)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return found
}

// clickHouseDeclaredRowsSchema is one MergeTree reference table whose sorting
// key is the declared key column.
func clickHouseDeclaredRowsSchema(rows ...schemamodel.ManagedRow) *schemamodel.Database {
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Setting", Name: clickHouseDeclaredRowsTable}},
		Fields: []schemamodel.Field{
			{StructName: "Setting", FieldName: "Code", Name: "code", Type: "String", Primary: true},
			{StructName: "Setting", FieldName: "Label", Name: "label", Type: "String"},
		},
		ManagedData: []schemamodel.ManagedData{{
			StructName: "Setting",
			Table:      clickHouseDeclaredRowsTable,
			Keys:       []string{"code"},
			File:       "settings.yaml",
			Rows:       append(make([]schemamodel.ManagedRow, 0, len(rows)), rows...),
		}},
	}
	schemamodel.Finalize(db)
	return db
}

func clickHouseSettingRow(code, label string) schemamodel.ManagedRow {
	return schemamodel.ManagedRow{
		"code":  {Tag: "str", Text: code},
		"label": {Tag: "str", Text: label},
	}
}
