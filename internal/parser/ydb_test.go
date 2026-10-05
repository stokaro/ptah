package parser_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/internal/parser"
)

func TestParse_YDB(t *testing.T) {
	for _, dialect := range []string{"ydb", "ydbs"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			statements, err := parser.NewParser("CREATE TABLE t (id Int64 NOT NULL, v Utf8, PRIMARY KEY (id), INDEX i GLOBAL ON (v));", parser.WithDialect(dialect)).Parse()
			c.Assert(err, qt.IsNil)
			c.Assert(statements.Statements, qt.HasLen, 1)
			table, ok := statements.Statements[0].(*ast.CreateTableNode)
			c.Assert(ok, qt.IsTrue)
			c.Assert(table.Indexes, qt.HasLen, 1)
			c.Assert(table.Indexes[0].Columns, qt.DeepEquals, []string{"v"})
		})
	}
}
