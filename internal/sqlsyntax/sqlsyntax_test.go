package sqlsyntax_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform/descriptor"
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
		{name: "shared grammar", text: "CREATE TABLE (;", dialect: "postgres", wantErr: `line 1, column 14: expected table name: .*`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statements, err := sqlsyntax.Parse(test.text, test.dialect)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(statements, qt.IsNil)
		})
	}
}

// TestReads says which dialects a refusal from Parse means anything for: the shared
// grammar reads every built-in dialect and a run with none, and it holds nothing for
// an engine Ptah has no descriptor for.
func TestReads(t *testing.T) {
	for _, test := range []struct {
		dialect string
		want    bool
	}{
		{dialect: "", want: true},
		{dialect: "postgres", want: true},
		{dialect: "sqlite", want: true},
		{dialect: "no-such-engine", want: false},
	} {
		t.Run(test.dialect, func(t *testing.T) {
			qt.New(t).Assert(sqlsyntax.Reads(test.dialect), qt.Equals, test.want)
		})
	}
}

// TestReadsDescribed holds the case a process only meets once a provider registers
// its dialect: YQL names a lexer no built-in service serves, because its provider
// reads it.
func TestReadsDescribed(t *testing.T) {
	for _, test := range []struct {
		name  string
		lexer string
		want  bool
	}{
		{name: "the shared lexer", lexer: "", want: true},
		{name: "YQL", lexer: descriptor.LexerYQL, want: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			described := descriptor.Dialect{Lexical: descriptor.Lexical{Lexer: test.lexer}}
			qt.New(t).Assert(sqlsyntax.ReadsDescribed(described), qt.Equals, test.want)
		})
	}
}
