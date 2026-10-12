//go:build integration

package dbschema_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// ClickHouse and Spanner have no enum type to create. ClickHouse builds an
// enum column as Enum8 or Enum16, and Spanner as text with a CHECK, which is
// the model the comparison lowers the declaration to. These tests ask the
// servers whether the column the render builds is the column the comparison
// expects: apply it, read it back, and plan nothing.

// valuedEnumTable declares table with a nullable enum column s and an enum
// column n, each carrying its values. required makes n NOT NULL, which Spanner
// cannot add to an existing table.
func valuedEnumTable(table string, required bool) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Name: table}},
		Fields: []schemamodel.Field{
			{StructName: "T", FieldName: "ID", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "T", FieldName: "S", Name: "s", Type: "ENUM", Nullable: true, Enum: []string{"draft", "it's"}},
			{StructName: "T", FieldName: "N", Name: "n", Type: "ENUM", Nullable: !required, Enum: []string{"a", "b"}},
		},
	}
}

// declaredEnumTable is valuedEnumTable with s typed by a declared enum.
func declaredEnumTable(table string, required bool) *schemamodel.Database {
	db := valuedEnumTable(table, required)
	db.Enums = []schemamodel.Enum{{Name: table + "_status", Values: []string{"draft", "it's"}}}
	db.Fields[1].Type = table + "_status"
	db.Fields[1].Enum = nil
	return db
}

// inlineEnumConnection connects to engine and drops table when the test ends.
func inlineEnumConnection(c *qt.C, engine dbtarget.Engine, table string) *dbschema.DatabaseConnection {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), dbtarget.URL(c, engine))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, _ = conn.ExecContext(context.Background(), `DROP TABLE IF EXISTS `+table)
		dbschema.CloseAndWarn(conn)
	})
	return conn
}

// inlineEnumExec runs each statement on its own: Spanner refuses DDL inside a
// transaction.
func inlineEnumExec(c *qt.C, conn *dbschema.DatabaseConnection, statements []string) {
	c.Helper()
	for _, statement := range statements {
		c.Assert(conn.Writer().ExecuteSQL(c.Context(), statement), qt.IsNil, qt.Commentf("statement:\n%s", statement))
	}
}

// inlineEnumPlan compares desired with what conn holds and returns the plan.
func inlineEnumPlan(c *qt.C, conn *dbschema.DatabaseConnection, desired *schemamodel.Database) []string {
	c.Helper()
	runtime := must.Must(builtin.New())
	current, err := conn.Reader().ReadSchemaContext(c.Context())
	c.Assert(err, qt.IsNil)
	diff, err := schemadiff.CompareWithDatabase(c.Context(), conn, desired, current, nil, runtime)
	c.Assert(err, qt.IsNil)
	statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(
		c.Context(), runtime, diff, conn.Info().Dialect, planner.Options{Capabilities: conn.Info().Capabilities},
	)
	c.Assert(err, qt.IsNil)
	return statements
}

// inlineEnumTableName is unique per run, so a run that failed before its
// cleanup leaves nothing a later run trips over.
func inlineEnumTableName(prefix string) string {
	return fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
}

// TestInlineEnumCreateConvergesLive creates the table the render writes and
// requires a second plan to be empty. The server must also refuse a value the
// enum does not list, which is what proves the values reached it.
func TestInlineEnumCreateConvergesLive(t *testing.T) {
	tests := []struct {
		name   string
		engine dbtarget.Engine
		schema func(table string, required bool) *schemamodel.Database
	}{
		{name: "clickhouse declared enum", engine: dbtarget.ClickHouse, schema: declaredEnumTable},
		{name: "clickhouse values on the column", engine: dbtarget.ClickHouse, schema: valuedEnumTable},
		{name: "spanner declared enum", engine: dbtarget.Spanner, schema: declaredEnumTable},
		{name: "spanner values on the column", engine: dbtarget.Spanner, schema: valuedEnumTable},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			table := inlineEnumTableName("ptah_enum")
			conn := inlineEnumConnection(c, test.engine, table)
			statements, err := builtin.GetOrderedCreateStatements(test.schema(table, true), conn.Info().Dialect)
			c.Assert(err, qt.IsNil)
			inlineEnumExec(c, conn, statements)

			c.Assert(inlineEnumPlan(c, conn, test.schema(table, true)), qt.HasLen, 0)
			_, err = conn.ExecContext(c.Context(), "INSERT INTO "+table+" (id, s, n) VALUES (1, 'draft', 'c')")
			c.Assert(err, qt.IsNotNil)
		})
	}
}

// TestInlineEnumAddColumnConvergesLive adds the enum columns to a table that
// exists and requires a second plan to be empty. Spanner refuses a CHECK
// inside ADD COLUMN, so its plan adds the check as a constraint of its own.
func TestInlineEnumAddColumnConvergesLive(t *testing.T) {
	tests := []struct {
		name   string
		engine dbtarget.Engine
	}{
		{name: "clickhouse", engine: dbtarget.ClickHouse},
		{name: "spanner", engine: dbtarget.Spanner},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			table := inlineEnumTableName("ptah_enum_add")
			conn := inlineEnumConnection(c, test.engine, table)
			base := declaredEnumTable(table, false)
			base.Enums = nil
			base.Fields = base.Fields[:1]
			statements, err := builtin.GetOrderedCreateStatements(base, conn.Info().Dialect)
			c.Assert(err, qt.IsNil)
			inlineEnumExec(c, conn, statements)

			inlineEnumExec(c, conn, inlineEnumPlan(c, conn, declaredEnumTable(table, false)))

			c.Assert(inlineEnumPlan(c, conn, declaredEnumTable(table, false)), qt.HasLen, 0)
		})
	}
}
