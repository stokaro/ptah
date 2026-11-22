//go:build integration

package dbschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/dbtarget"
)

// checkProbeEngines are the servers that store a CHECK in a spelling of their
// own, so the comparison asks the server to spell the declaration first.
// CockroachDB answers only with temporary tables turned on for the probe's
// transaction; PostgreSQL is the control, where they need no setting.
var checkProbeEngines = []struct {
	name   string
	engine dbtarget.Engine
}{
	{name: "PostgreSQL", engine: dbtarget.PostgreSQL},
	{name: "CockroachDB", engine: dbtarget.CockroachDB},
}

// checkedTable declares table with the column-level CHECK check on amount.
func checkedTable(table, check string) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Name: table}},
		Fields: []schemamodel.Field{
			{StructName: "T", FieldName: "ID", Name: "id", Type: "INT", Primary: true},
			{StructName: "T", FieldName: "Code", Name: "code", Type: "TEXT", Nullable: true},
			{StructName: "T", FieldName: "Amount", Name: "amount", Type: "INT", Nullable: true, Check: check, CheckName: table + "_amount_check"},
		},
	}
}

// createCheckedTable is the statement that creates checkedTable's table with
// the CHECK spelled stored.
func createCheckedTable(table, stored string) string {
	return "CREATE TABLE " + table + " (id INT PRIMARY KEY, code TEXT, amount INT CONSTRAINT " +
		table + "_amount_check CHECK (" + stored + "))"
}

// TestCompareWithDatabase_LiveCheckConvergesOnTheServersSpelling creates a
// table holding a CHECK, then plans the same CHECK as declared. The server
// stores it in a spelling of its own -- CockroachDB v26.4.1 prints
// `code <> 'C:x'` as `(code != 'C:x'::text)` and `code <> 'C:\'` as
// `(code != e'C:\\'::text)` -- and the comparison sees one CHECK only where the
// server spelled the declaration too. Without the probe the plan drops the
// CHECK and adds it again on every run.
func TestCompareWithDatabase_LiveCheckConvergesOnTheServersSpelling(t *testing.T) {
	for _, engine := range checkProbeEngines {
		for _, test := range []struct {
			name  string
			check string
		}{
			{name: "a string", check: "amount >= 0 AND code <> 'C:x'"},
			{name: "a string that ends in a backslash", check: `amount >= 0 AND code <> 'C:\'`},
			{name: "a list of values", check: "code IN ('a', 'b') OR amount > 10"},
		} {
			t.Run(engine.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				table := inlineEnumTableName("ck_probe")
				conn := inlineEnumConnection(c, engine.engine, table)
				inlineEnumExec(c, conn, []string{createCheckedTable(table, test.check)})

				statements := inlineEnumPlan(c, conn, checkedTable(table, test.check))

				c.Assert(statements, qt.HasLen, 0)
			})
		}
	}
}

// TestCompareWithDatabase_LiveCheckPlansAChangedValue is the control for
// TestCompareWithDatabase_LiveCheckConvergesOnTheServersSpelling: a CHECK
// declared with another value than the table holds plans the CHECK's
// replacement on each server, so the convergence is the server's answer and
// not a comparison that reads every CHECK as equal.
func TestCompareWithDatabase_LiveCheckPlansAChangedValue(t *testing.T) {
	for _, engine := range checkProbeEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			table := inlineEnumTableName("ck_probe")
			conn := inlineEnumConnection(c, engine.engine, table)
			inlineEnumExec(c, conn, []string{createCheckedTable(table, "amount >= 0 AND code <> 'C:x'")})

			statements := inlineEnumPlan(c, conn, checkedTable(table, "amount >= 0 AND code <> 'C:y'"))

			c.Assert(statements, qt.HasLen, 2)
			c.Assert(statements[0], qt.Contains, "DROP CONSTRAINT")
			c.Assert(statements[1], qt.Contains, "'C:y'")
		})
	}
}
