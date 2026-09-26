package parser_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/internal/parser"
)

// A DO block written as a string literal is read as the code it holds, the way
// a routine body is, so what migration lint reads in it is what the server
// runs (stokaro/ptah#3691).
func TestParse_AQuotedDoBlockIsItsCode(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{name: "a doubled quote", sql: `DO 'BEGIN RAISE NOTICE ''x''; END';`},
		{name: "an escape string", sql: `DO E'BEGIN RAISE NOTICE \'x\'; END';`},
		{name: "a dollar quote", sql: `DO $$BEGIN RAISE NOTICE 'x'; END$$;`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parser.NewParser(test.sql, parser.WithDialect("postgres")).Parse()

			c.Assert(err, qt.IsNil)
			c.Assert(statements.Statements, qt.HasLen, 1)
			block, isBlock := statements.Statements[0].(*ast.PostgresDoBlockNode)
			c.Assert(isBlock, qt.IsTrue)
			c.Assert(block.Body.SQL, qt.Equals, `BEGIN RAISE NOTICE 'x'; END`)
		})
	}
}

// A DO block whose closing quote is missing is refused rather than read as
// code. The statement is refused before its body is read: the unterminated
// literal runs to the end of the input, so no semicolon ends the statement.
func TestParse_AnUnterminatedDoBlockIsRefused(t *testing.T) {
	c := qt.New(t)

	_, err := parser.NewParser(`DO 'BEGIN PERFORM 1; END`, parser.WithDialect("postgres")).Parse()

	c.Assert(err, qt.ErrorMatches, `unterminated DO statement at position \d+`)
}

// Without a dialect the lexer is the permissive one, which takes a backslash
// in a string as an escape, so a body is read back by the same rule rather
// than refused as a literal with a lone quote in it.
func TestParse_APermissiveReadUndoesABackslashEscape(t *testing.T) {
	c := qt.New(t)

	statements, err := parser.NewParser(`CREATE FUNCTION f() RETURNS text LANGUAGE sql AS 'SELECT \'x\'';`).Parse()

	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)
	function, isFunction := statements.Statements[0].(*ast.CreateFunctionNode)
	c.Assert(isFunction, qt.IsTrue)
	c.Assert(function.Body, qt.Equals, `SELECT 'x'`)
}
