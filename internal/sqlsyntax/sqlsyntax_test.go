package sqlsyntax_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/internal/sqlsyntax"
)

// TestParse_HappyPath reads a document with the shared grammar: no built-in
// dialect is read by a lexer of its own.
func TestParse_HappyPath(t *testing.T) {
	c := qt.New(t)
	statements, err := sqlsyntax.Parse(`CREATE TABLE "t" (id integer PRIMARY KEY);`, "postgres")
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)
	table, ok := statements.Statements[0].(*ast.CreateTableNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(table.Name, qt.Equals, `"t"`)
}

// TestParse_FailurePath refuses a document the shared grammar refuses.
func TestParse_FailurePath(t *testing.T) {
	for _, test := range []struct{ name, text, dialect, wantErr string }{
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
