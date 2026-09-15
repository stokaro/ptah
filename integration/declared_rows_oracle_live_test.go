//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"slices"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/sijms/go-ora/v3" // registers the Oracle driver for database/sql

	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/internal/atlasschema"
	"ptah.run/internal/dbtarget"
)

// Declared rows against Oracle, where the spelling of a name decides which
// object it names and a plain string is not a moment.
//
// The schema renderer writes a plain name bare and Oracle folds it, so
// ptah_i3296_rows is created as PTAH_I3296_ROWS. A statement that quotes the
// name addresses "ptah_i3296_rows", a table nobody created, and the server
// answers ORA-00942. The literals have the same property: a BOOLEAN column is
// NUMBER(1) here, and a TIMESTAMP column refuses '2024-03-01 12:30:45' with
// ORA-01843. An offline test reads each of those statements as correct, so
// these drive the plan `ptah schema apply` prepares into the server and read
// the rows back (stokaro/ptah#3296).
//
// The Oracle account can hold other tables, and the plan compares the whole
// schema, so it carries a DROP for each of them. Only the statements that reach
// the table under test are run.

const (
	oracleDeclaredRowsTable    = "ptah_i3296_rows"
	oracleDeclaredMomentsTable = "ptah_i3296_moments"
	oracleDeclaredFlagsTable   = "ptah_i3296_flags"
)

// TestDeclaredRowsNameTheOracleTableLive applies one declared row into a table
// the same plan creates, and reads it back.
//
// `comment` is a word Oracle refuses bare, so the renderer quotes that column
// and the row has to quote it too; `code` and `label` are plain and go bare in
// both. A row writer that quotes every name fails at the INSERT with ORA-00942.
func TestDeclaredRowsNameTheOracleTableLive(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()

	conn := openOracleDeclaredRows(c, ctx, oracleDeclaredRowsTable)
	applyOracleDeclaredRows(c, ctx, conn, oracleDeclaredRowsSchema(), oracleDeclaredRowsTable)

	c.Assert(oracleDeclaredRowValues(c, ctx, conn,
		`SELECT code, label, "comment" FROM ptah_i3296_rows ORDER BY code`,
	), qt.DeepEquals, []string{"one|Retention window|kept"})
}

// TestDeclaredRowsReadBackFromOracleLive reads the applied row through
// dbschema.ReadTableRows, the read every later reconciliation compares against.
//
// The declaration names the table and its columns in lower case. A reader that
// quotes them selects from "ptah_i3296_rows", and the ORA-00942 it gets stops
// every plan after the first.
func TestDeclaredRowsReadBackFromOracleLive(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()

	conn := openOracleDeclaredRows(c, ctx, oracleDeclaredRowsTable)
	applyOracleDeclaredRows(c, ctx, conn, oracleDeclaredRowsSchema(), oracleDeclaredRowsTable)

	rows, err := dbschema.ReadTableRows(ctx, conn, "", oracleDeclaredRowsTable, []string{"code", "label", "comment"})
	c.Assert(err, qt.IsNil)
	c.Assert(rows, qt.DeepEquals, []map[string]any{
		{"code": "one", "label": "Retention window", "comment": "kept"},
	})
}

// TestDeclaredMomentsReachOracleLive declares a moment as text for each Oracle
// datetime column and reads back what the server stored.
//
// Oracle reads a plain string through NLS_TIMESTAMP_FORMAT, DD-MON-RR by
// default, so every one of these INSERTs answers ORA-01843 unless the value is
// written as a typed TIMESTAMP literal. The expected values are the server's
// own rendering of the stored moment.
func TestDeclaredMomentsReachOracleLive(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()

	conn := openOracleDeclaredRows(c, ctx, oracleDeclaredMomentsTable)
	applyOracleDeclaredRows(c, ctx, conn, oracleDeclaredMomentsSchema(), oracleDeclaredMomentsTable)

	c.Assert(oracleDeclaredRowValues(c, ctx, conn,
		`SELECT code,
		        TO_CHAR(seen, 'YYYY-MM-DD HH24:MI:SS'),
		        TO_CHAR(day, 'YYYY-MM-DD HH24:MI:SS'),
		        TO_CHAR(stamped, 'YYYY-MM-DD HH24:MI:SS TZH:TZM')
		   FROM ptah_i3296_moments ORDER BY code`,
	), qt.DeepEquals, []string{"one|2024-03-01 12:30:45|2024-03-01 00:00:00|2026-01-02 03:04:05 +02:00"})
}

// TestDeclaredRowsConvergeOnOracleLive applies a declared row and plans again:
// an unchanged declaration plans nothing.
//
// Two readings have to agree for that. The catalog reports the table as
// PTAH_I3296_FLAGS, and a lookup that compares ptah_i3296_flags exactly never
// finds it, never reads its rows, and plans the INSERT again into a table that
// already holds the row. And the driver scans NUMBER as text, so a declared
// true has to meet "1" and a declared 30 has to meet "30"; compared by type,
// both plan an UPDATE on every apply.
func TestDeclaredRowsConvergeOnOracleLive(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()

	conn := openOracleDeclaredRows(c, ctx, oracleDeclaredFlagsTable)
	desired := oracleDeclaredFlagsSchema("30")
	applyOracleDeclaredRows(c, ctx, conn, desired, oracleDeclaredFlagsTable)

	c.Assert(oracleDeclaredRowsPlan(c, ctx, conn, desired, oracleDeclaredFlagsTable), qt.HasLen, 0)
}

// TestDeclaredRowsUpdateOneRowOnOracleLive is the control for the test above:
// with one declared value changed, the plan carries one statement, the server
// applies it, and the next plan is empty again.
//
// Without it, a comparison that pairs everything would satisfy the convergence
// assertion by planning nothing for a change as well.
func TestDeclaredRowsUpdateOneRowOnOracleLive(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()

	conn := openOracleDeclaredRows(c, ctx, oracleDeclaredFlagsTable)
	applyOracleDeclaredRows(c, ctx, conn, oracleDeclaredFlagsSchema("30"), oracleDeclaredFlagsTable)

	changed := oracleDeclaredFlagsSchema("31")
	c.Assert(oracleDeclaredRowsPlan(c, ctx, conn, changed, oracleDeclaredFlagsTable), qt.HasLen, 1)

	applyOracleDeclaredRows(c, ctx, conn, changed, oracleDeclaredFlagsTable)
	c.Assert(oracleDeclaredRowValues(c, ctx, conn,
		`SELECT code, enabled, weight FROM ptah_i3296_flags ORDER BY code`,
	), qt.DeepEquals, []string{"one|1|31"})
	c.Assert(oracleDeclaredRowsPlan(c, ctx, conn, changed, oracleDeclaredFlagsTable), qt.HasLen, 0)
}

// openOracleDeclaredRows connects to Oracle and drops table before the test and
// again after it. Only that table is dropped: the account is shared, and
// DropAllTables would take the tables of every other test with it.
func openOracleDeclaredRows(c *qt.C, ctx context.Context, table string) *dbschema.DatabaseConnection {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(ctx, dbtarget.URL(c, dbtarget.Oracle))
	c.Assert(err, qt.IsNil)
	dropOracleDeclaredRowsTable(c, ctx, conn, table)
	c.Cleanup(func() {
		dropOracleDeclaredRowsTable(c, context.WithoutCancel(ctx), conn, table)
		dbschema.CloseAndWarn(conn)
	})
	return conn
}

func dropOracleDeclaredRowsTable(c *qt.C, ctx context.Context, conn *dbschema.DatabaseConnection, table string) {
	c.Helper()
	_, err := conn.ExecContext(ctx, "DROP TABLE IF EXISTS "+table+" PURGE")
	c.Check(err, qt.IsNil)
}

// oracleDeclaredRowsPlan is the plan `ptah schema apply` prepares, narrowed to
// the statements that reach table.
func oracleDeclaredRowsPlan(
	c *qt.C,
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	desired *schemamodel.Database,
	table string,
) []string {
	c.Helper()
	plan, err := atlasschema.PlanApply(ctx, conn, atlasschema.ApplyOptions{Desired: desired})
	c.Assert(err, qt.IsNil)
	name := strings.ToUpper(table)
	return slices.DeleteFunc(plan.Statements(), func(statement string) bool {
		return !strings.Contains(strings.ToUpper(statement), name)
	})
}

// applyOracleDeclaredRows runs the plan statement by statement, in the order the
// plan carries them. A statement the server refuses fails the test at that
// statement, which is the measurement.
func applyOracleDeclaredRows(
	c *qt.C,
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	desired *schemamodel.Database,
	table string,
) {
	c.Helper()
	for _, statement := range oracleDeclaredRowsPlan(c, ctx, conn, desired, table) {
		_, err := conn.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil, qt.Commentf("statement: %s", statement))
	}
}

// oracleDeclaredRowValues runs query and joins the columns of each row with
// "|", so one assertion covers the row set and the value each row holds.
func oracleDeclaredRowValues(c *qt.C, ctx context.Context, conn *dbschema.DatabaseConnection, query string) []string {
	c.Helper()
	rows, err := conn.QueryContext(ctx, query)
	c.Assert(err, qt.IsNil)
	defer rows.Close()

	columns, err := rows.Columns()
	c.Assert(err, qt.IsNil)

	var found []string
	for rows.Next() {
		values := make([]sql.NullString, len(columns))
		targets := make([]any, len(columns))
		for i := range values {
			targets[i] = &values[i]
		}
		c.Assert(rows.Scan(targets...), qt.IsNil)
		texts := make([]string, len(values))
		for i, value := range values {
			texts[i] = value.String
		}
		found = append(found, strings.Join(texts, "|"))
	}
	c.Assert(rows.Err(), qt.IsNil)
	return found
}

// oracleDeclaredRowsSchema is one reference table with a plain column and a
// column named by a word Oracle refuses bare.
func oracleDeclaredRowsSchema() *schemamodel.Database {
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Setting", Name: oracleDeclaredRowsTable}},
		Fields: []schemamodel.Field{
			{StructName: "Setting", FieldName: "Code", Name: "code", Type: "VARCHAR(32)", Primary: true},
			{StructName: "Setting", FieldName: "Label", Name: "label", Type: "VARCHAR(64)", Nullable: true},
			{StructName: "Setting", FieldName: "Comment", Name: "comment", Type: "VARCHAR(64)", Nullable: true},
		},
		ManagedData: []schemamodel.ManagedData{{
			StructName: "Setting",
			Table:      oracleDeclaredRowsTable,
			Keys:       []string{"code"},
			File:       "settings.yaml",
			Rows: []schemamodel.ManagedRow{{
				"code":    {Tag: "str", Text: "one"},
				"label":   {Tag: "str", Text: "Retention window"},
				"comment": {Tag: "str", Text: "kept"},
			}},
		}},
	}
	schemamodel.Finalize(db)
	return db
}

// oracleDeclaredMomentsSchema is one row carrying a moment in each Oracle
// datetime column, each written in a spelling a declaration uses.
func oracleDeclaredMomentsSchema() *schemamodel.Database {
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Window", Name: oracleDeclaredMomentsTable}},
		Fields: []schemamodel.Field{
			{StructName: "Window", FieldName: "Code", Name: "code", Type: "VARCHAR(32)", Primary: true},
			{StructName: "Window", FieldName: "Seen", Name: "seen", Type: "TIMESTAMP", Nullable: true},
			{StructName: "Window", FieldName: "Day", Name: "day", Type: "DATE", Nullable: true},
			{StructName: "Window", FieldName: "Stamped", Name: "stamped", Type: "TIMESTAMPTZ", Nullable: true},
		},
		ManagedData: []schemamodel.ManagedData{{
			StructName: "Window",
			Table:      oracleDeclaredMomentsTable,
			Keys:       []string{"code"},
			File:       "windows.yaml",
			Rows: []schemamodel.ManagedRow{{
				"code":    {Tag: "str", Text: "one"},
				"seen":    {Tag: "str", Text: "2024-03-01 12:30:45"},
				"day":     {Tag: "str", Text: "2024-03-01"},
				"stamped": {Tag: "str", Text: "2026-01-02T03:04:05+02:00"},
			}},
		}},
	}
	schemamodel.Finalize(db)
	return db
}

// oracleDeclaredFlagsSchema is one row whose values the driver hands back in a
// different Go type than the declaration carries: a boolean and an integer
// read as text, and a timestamp read as a time.Time.
func oracleDeclaredFlagsSchema(weight string) *schemamodel.Database {
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Flag", Name: oracleDeclaredFlagsTable}},
		Fields: []schemamodel.Field{
			{StructName: "Flag", FieldName: "Code", Name: "code", Type: "VARCHAR(32)", Primary: true},
			{StructName: "Flag", FieldName: "Enabled", Name: "enabled", Type: "BOOLEAN", Nullable: true},
			{StructName: "Flag", FieldName: "Weight", Name: "weight", Type: "INTEGER", Nullable: true},
			{StructName: "Flag", FieldName: "Seen", Name: "seen", Type: "TIMESTAMP", Nullable: true},
		},
		ManagedData: []schemamodel.ManagedData{{
			StructName: "Flag",
			Table:      oracleDeclaredFlagsTable,
			Keys:       []string{"code"},
			File:       "flags.yaml",
			Rows: []schemamodel.ManagedRow{{
				"code":    {Tag: "str", Text: "one"},
				"enabled": {Tag: "bool", Text: "true"},
				"weight":  {Tag: "int", Text: weight},
				"seen":    {Tag: "str", Text: "2024-03-01 12:30:45"},
			}},
		}},
	}
	schemamodel.Finalize(db)
	return db
}
