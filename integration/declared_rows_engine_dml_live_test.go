//go:build integration

package integration_test

import (
	"context"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
)

// How a row change reaches the engine, where the engine decides the spelling.
//
// The renderer writes one statement per operation. Whether that statement is
// the one the server accepts is the server's answer: ClickHouse refuses a plain
// UPDATE, and a key column holding NULL matches nothing when it is addressed
// with `=`. Both read as correct SQL offline.

// TestDeclaredRowsUpdateOnClickHouseLive changes one declared value on
// ClickHouse, which answers a plain UPDATE with "Lightweight updates are not
// supported" (stokaro/ptah#3281).
func TestDeclaredRowsUpdateOnClickHouseLive(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()

	conn, err := dbschema.ConnectToDatabase(ctx, dbtarget.URL(c, dbtarget.ClickHouse))
	c.Assert(err, qt.IsNil)
	defer conn.Close()
	dropClickHouseProbeTable(c, ctx, conn)
	defer dropClickHouseProbeTable(c, ctx, conn)

	applyDeclaredRows(c, ctx, conn, clickHouseSettings("first"))
	applyDeclaredRows(c, ctx, conn, clickHouseSettings("second"))

	c.Assert(clickHouseLabel(c, ctx, conn), qt.Equals, "second")
}

// TestDeclaredRowsNullKeyLive removes a row whose key column holds NULL.
//
// Addressed with `= NULL` the predicate is UNKNOWN for every row, so the DELETE
// succeeds and removes nothing; the row stays, and every later run finds it
// again (stokaro/ptah#3279).
func TestDeclaredRowsNullKeyLive(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()

	conn, done := declaredRowsDatabase(c, ctx, "null_key")
	defer done()

	applyDeclaredRows(c, ctx, conn, nullKeySettings("one", "two"))
	applyDeclaredRows(c, ctx, conn, nullKeySettings("one"))

	c.Assert(nullKeyCodes(c, ctx, conn), qt.DeepEquals, []string{"one"})
}

func dropClickHouseProbeTable(c *qt.C, ctx context.Context, conn *dbschema.DatabaseConnection) {
	c.Helper()
	_, err := conn.ExecContext(ctx, "DROP TABLE IF EXISTS ptah_dml_settings")
	c.Assert(err, qt.IsNil)
}

func clickHouseLabel(c *qt.C, ctx context.Context, conn *dbschema.DatabaseConnection) string {
	c.Helper()
	// A ClickHouse mutation is applied in the background, so the read waits for
	// it rather than asserting on whatever the first SELECT happened to see.
	deadline := time.Now().Add(30 * time.Second)
	label := ""
	for time.Now().Before(deadline) && label != "second" {
		label = clickHouseLabelOnce(c, ctx, conn)
		time.Sleep(200 * time.Millisecond)
	}
	return label
}

func clickHouseLabelOnce(c *qt.C, ctx context.Context, conn *dbschema.DatabaseConnection) string {
	c.Helper()
	rows, err := conn.QueryContext(ctx, "SELECT label FROM ptah_dml_settings WHERE code = 'one'")
	c.Assert(err, qt.IsNil)
	defer rows.Close()

	label := ""
	for rows.Next() {
		c.Assert(rows.Scan(&label), qt.IsNil)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return label
}

func clickHouseSettings(label string) *schemamodel.Database {
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{{
			StructName: "Setting",
			Name:       "ptah_dml_settings",
			Engine:     "MergeTree",
		}},
		Fields: []schemamodel.Field{
			{StructName: "Setting", FieldName: "Code", Name: "code", Type: "String", Primary: true},
			{StructName: "Setting", FieldName: "Label", Name: "label", Type: "String"},
		},
		ManagedData: []schemamodel.ManagedData{{
			StructName: "Setting",
			Table:      "ptah_dml_settings",
			Keys:       []string{"code"},
			File:       "settings.yaml",
			Rows: []schemamodel.ManagedRow{{
				"code":  {Tag: "str", Text: "one"},
				"label": {Tag: "str", Text: label},
			}},
		}},
	}
	schemamodel.Finalize(db)
	return db
}

func nullKeyCodes(c *qt.C, ctx context.Context, conn *dbschema.DatabaseConnection) []string {
	c.Helper()
	rows, err := conn.QueryContext(ctx, `SELECT "code" FROM "scoped" ORDER BY "code"`)
	c.Assert(err, qt.IsNil)
	defer rows.Close()

	codes := make([]string, 0, 2)
	for rows.Next() {
		code := ""
		c.Assert(rows.Scan(&code), qt.IsNil)
		codes = append(codes, code)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return codes
}

// nullKeySettings declares rows of a table keyed on two columns, the second of
// which every row declares as NULL.
func nullKeySettings(codes ...string) *schemamodel.Database {
	rows := make([]schemamodel.ManagedRow, 0, len(codes))
	for _, code := range codes {
		rows = append(rows, schemamodel.ManagedRow{
			"code":   {Tag: "str", Text: code},
			"tenant": {Null: true},
		})
	}
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Scoped", Name: "scoped"}},
		Fields: []schemamodel.Field{
			{StructName: "Scoped", FieldName: "Code", Name: "code", Type: "TEXT"},
			{StructName: "Scoped", FieldName: "Tenant", Name: "tenant", Type: "TEXT", Nullable: true},
		},
		ManagedData: []schemamodel.ManagedData{{
			StructName: "Scoped",
			Table:      "scoped",
			Keys:       []string{"code", "tenant"},
			File:       "scoped.yaml",
			Rows:       rows,
		}},
	}
	schemamodel.Finalize(db)
	return db
}
