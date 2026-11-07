package parser_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/internal/parser"
)

// The restrict lines pg_dump writes around a dump declare nothing, so the parse
// reads the statements between them alone.
func TestParse_DumpRestriction_HappyPath(t *testing.T) {
	c := qt.New(t)

	statements, err := parser.NewParser("\\restrict bOMhEX9z\n\nCREATE TABLE t (id int);\n\n\\unrestrict bOMhEX9z\n",
		parser.WithDialect("postgres")).Parse()

	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)
	table, ok := statements.Statements[0].(*ast.CreateTableNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(table.Name, qt.Equals, "t")
}

// Any other psql command reaches the statement grammar and is refused, and so is
// a restrict line inside a statement.
func TestParse_DumpRestriction_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name, sql, wantErr string
	}{
		{name: "another psql command", sql: "\\connect app\nCREATE TABLE t (id int);\n", wantErr: `line 1, column 1: expected SQL keyword, got Operator`},
		{name: "a restrict line inside a statement", sql: "CREATE TABLE t (id int)\n\\restrict k\n;\n", wantErr: `line 2, column 1: expected SQL keyword, got Operator`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parser.NewParser(test.sql, parser.WithDialect("postgres")).Parse()

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(statements, qt.IsNil)
		})
	}
}
