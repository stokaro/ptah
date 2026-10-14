package sqlsyntax_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/internal/sqlsyntax"
)

// TestParse_HappyPath reads a YDB document as YQL in every spelling of the
// dialect, and any other document with the shared grammar.
func TestParse_HappyPath(t *testing.T) {
	for _, dialect := range []string{"ydb", "ydbs"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			statements, err := sqlsyntax.Parse("CREATE TABLE t (id Int64 NOT NULL, v Utf8, PRIMARY KEY (id), INDEX i GLOBAL ON (v));", dialect)
			c.Assert(err, qt.IsNil)
			c.Assert(statements.Statements, qt.HasLen, 1)
			table, ok := statements.Statements[0].(*ast.CreateTableNode)
			c.Assert(ok, qt.IsTrue)
			c.Assert(table.Indexes, qt.HasLen, 1)
			c.Assert(table.Indexes[0].Columns, qt.DeepEquals, []string{"v"})
		})
	}
	t.Run("shared grammar", func(t *testing.T) {
		c := qt.New(t)
		statements, err := sqlsyntax.Parse(`CREATE TABLE "t" (id integer PRIMARY KEY);`, "postgres")
		c.Assert(err, qt.IsNil)
		c.Assert(statements.Statements, qt.HasLen, 1)
		table, ok := statements.Statements[0].(*ast.CreateTableNode)
		c.Assert(ok, qt.IsTrue)
		c.Assert(table.Name, qt.Equals, `"t"`)
	})
}

// TestParse_FailurePath refuses what the dialect's grammar cannot read: a
// YQL statement Ptah does not read, and a document the shared grammar
// refuses.
func TestParse_FailurePath(t *testing.T) {
	for _, test := range []struct{ name, text, dialect, wantErr string }{
		{name: "YQL", text: "SELECT 1;", dialect: "ydb", wantErr: `YQL schema at position .*`},
		{name: "shared grammar", text: "CREATE TABLE (;", dialect: "postgres", wantErr: `expected table name: .*`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statements, err := sqlsyntax.Parse(test.text, test.dialect)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(statements, qt.IsNil)
		})
	}
}
