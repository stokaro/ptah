package sqlite_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/engine/builtin"
)

// An expression default is written in the parentheses SQLite requires around
// one: `DEFAULT 1 + 2` and `DEFAULT datetime('now')` are syntax errors. What
// the grammar takes bare stays bare.
func TestRenderSQL_CreateTableEnclosesAnExpressionDefault(t *testing.T) {
	tests := []struct {
		name, columnType, expression string
		want                         string
	}{
		{name: "an operation", columnType: "INTEGER", expression: "1 + 2", want: `"c" INTEGER DEFAULT (1 + 2)`},
		{name: "a call", columnType: "TEXT", expression: "now()", want: `"c" TEXT DEFAULT (now())`},
		{name: "a call with an argument", columnType: "TEXT", expression: "datetime('now')", want: `"c" TEXT DEFAULT (datetime('now'))`},
		{name: "two groups", columnType: "INTEGER", expression: "(1) + (2)", want: `"c" INTEGER DEFAULT ((1) + (2))`},
		{name: "an enclosed expression", columnType: "INTEGER", expression: "(1 + 2)", want: `"c" INTEGER DEFAULT (1 + 2)`},
		{name: "a negative number", columnType: "INTEGER", expression: "-1", want: `"c" INTEGER DEFAULT -1`},
		{name: "a hexadecimal number", columnType: "INTEGER", expression: "0x1F", want: `"c" INTEGER DEFAULT 0x1F`},
		{name: "a quoted string", columnType: "TEXT", expression: "'x'", want: `"c" TEXT DEFAULT 'x'`},
		{name: "a blob", columnType: "BLOB", expression: "x'41'", want: `"c" BLOB DEFAULT x'41'`},
		{name: "NULL", columnType: "TEXT", expression: "NULL", want: `"c" TEXT DEFAULT NULL`},
		{name: "a current-time keyword", columnType: "TEXT", expression: "CURRENT_TIMESTAMP", want: `"c" TEXT DEFAULT CURRENT_TIMESTAMP`},
		{name: "a boolean", columnType: "INTEGER", expression: "TRUE", want: `"c" INTEGER DEFAULT TRUE`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			column := ast.NewColumn("c", test.columnType)
			column.SetDefaultExpression(test.expression)

			sql, err := builtin.RenderSQL("sqlite", ast.NewCreateTable("notes").AddColumn(column))

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, test.want)
		})
	}
}
