package parser_test

import (
	"errors"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/internal/parser"
)

// firstStatements stands in front of a refused statement: a comment in another
// script, and a statement whose string runs over two lines, so the next
// statement stands on line 7. The newline inside the string has to count, and
// the multi-byte characters must not move anything on a later line.
const firstStatements = "-- Каталог: таблицы\nCREATE TABLE catalog (\n  note TEXT DEFAULT 'one\ntwo'\n);\n\n"

// A refusal reports the line and column of the place it stopped, in the document
// as written. wantAt is what the byte offset finds in that document, which is
// what proves the two agree: the offset is a claim about the text, read here
// independently of the line and column.
func TestRefusalPosition(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		text    string
		wantErr string
		wantAt  string
	}{
		{
			name: "the first statement",
			text: "CREATE TABLE a (id INTEGER) BOGUS;",

			wantErr: `line 1, column 29: unsupported table option: BOGUS`,
			wantAt:  "BOGUS",
		},
		{
			name: "a statement after one with a multi-line body",
			text: firstStatements + "CREATE TABLE b (id INTEGER) BOGUS;",

			wantErr: `line 7, column 29: unsupported table option: BOGUS`,
			wantAt:  "BOGUS",
		},
		{
			name: "deep in a long file",
			text: strings.Repeat("CREATE TABLE t (\n  id INTEGER\n);\n", 400) + "CREATE TABLE b (id INTEGER) BOGUS;",

			wantErr: `line 1201, column 29: unsupported table option: BOGUS`,
			wantAt:  "BOGUS",
		},
		{
			name: "a table name that wraps the expected list",
			text: firstStatements + "CREATE TABLE e1 PARTITION BY RANGE (a);",

			wantErr: `line 7, column 17: expected '\(' after table name: expected Operator, got Identifier`,
			wantAt:  "PARTITION",
		},
		{
			name: "a multi-byte identifier counts one column a character",
			text: "CREATE TABLE заказы BOGUS;",

			wantErr: `line 1, column 21: unsupported table option: BOGUS`,
			wantAt:  "BOGUS",
		},
		{
			name: "a four-byte character counts one column",
			text: `CREATE TABLE "😀" BOGUS;`,

			wantErr: `line 1, column 18: unsupported table option: BOGUS`,
			wantAt:  "BOGUS",
		},
		{
			name: "a tab counts one column",
			text: "\tCREATE\tTABLE\tb\tBOGUS;",

			wantErr: `line 1, column 17: unsupported table option: BOGUS`,
			wantAt:  "BOGUS",
		},
		{
			name: "CRLF line endings",
			text: "CREATE TABLE a (\r\n  id INTEGER\r\n);\r\nCREATE TABLE b BOGUS;\r\n",

			wantErr: `line 4, column 16: unsupported table option: BOGUS`,
			wantAt:  "BOGUS",
		},
		{
			name: "CR line endings",
			text: "CREATE TABLE a (\r  id INTEGER\r);\rCREATE TABLE b BOGUS;\r",

			wantErr: `line 4, column 16: unsupported table option: BOGUS`,
			wantAt:  "BOGUS",
		},
		{
			name: "a line comment ended by CR",
			text: "-- note\rCREATE TABLE b BOGUS;\r",

			wantErr: `line 2, column 16: unsupported table option: BOGUS`,
			wantAt:  "BOGUS",
		},
		{
			name: "after a multi-line comment on the same line",
			text: "/* one\n   two */ CREATE TABLE b BOGUS;",

			wantErr: `line 2, column 26: unsupported table option: BOGUS`,
			wantAt:  "BOGUS",
		},
		{
			name: "at a string that is never closed, which starts the refusal",
			text: "CREATE TABLE a (id INTEGER);\nCOMMENT ON TABLE a IS 'never\nclosed\n",

			wantErr: `(?s)line 2, column 23: 'never.*is not a string constant the server reads, for the comment text`,
			wantAt:  "'never",
		},
		{
			name: "after a client delimiter directive, which is not in the text the parser reads",
			text: "CREATE TABLE a (id INT);\nDELIMITER $$\nCREATE PROCEDURE p()\nBEGIN\n  SELECT 1;\nEND$$\nDELIMITER ;\nCREATE TABLE b BOGUS;\n",

			wantErr: `line 8, column 16: unsupported table option: BOGUS`,
			wantAt:  "BOGUS",
		},
		{
			name: "after terminators a delimiter rewrote, on the line they are on",
			text: "DELIMITER $$\nCREATE TABLE a (id INT)$$ CREATE TABLE b BOGUS$$\n",

			wantErr: `line 2, column 42: unsupported table option: BOGUS`,
			wantAt:  "BOGUS",
		},
		{
			name: "after an Atlas delimiter directive",
			text: "-- atlas:delimiter ~~\nCREATE TABLE a (id INT)~~\nCREATE TABLE b BOGUS~~\n",

			wantErr: `line 3, column 16: unsupported table option: BOGUS`,
			wantAt:  "BOGUS",
		},
		{
			name:    "a procedure, whose clauses are read again from the statement's own text",
			dialect: platform.Postgres,
			text:    firstStatements + "CREATE PROCEDURE p() EXTERNAL BOGUS AS $$ SELECT 1 $$;",

			wantErr: `line 7, column 22: expected SECURITY after EXTERNAL`,
			wantAt:  "EXTERNAL",
		},
		{
			name: "a constraint comment, whose target is read again by itself",
			text: firstStatements + "COMMENT ON CONSTRAINT c FOO t IS 'x';",

			wantErr: `line 7, column 25: COMMENT ON CONSTRAINT requires ON: expected 'ON', got 'FOO'`,
			wantAt:  "FOO",
		},
		{
			name: "a constraint comment set to NULL, read the same way",
			text: firstStatements + "COMMENT ON CONSTRAINT c FOO t IS NULL;",

			wantErr: `line 7, column 25: COMMENT ON CONSTRAINT requires ON: expected 'ON', got 'FOO'`,
			wantAt:  "FOO",
		},
		{
			name: "a token after a constraint comment's table",
			text: firstStatements + "COMMENT ON CONSTRAINT c ON t BOGUS IS 'x';",

			wantErr: `line 7, column 30: COMMENT ON CONSTRAINT has an unexpected token "BOGUS"`,
			wantAt:  "BOGUS",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parser.NewParser(test.text, parser.WithDialect(test.dialect)).Parse()

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(statements, qt.IsNil)
			at, ok := errors.AsType[*ptaherr.ParseError](err)
			c.Assert(ok, qt.IsTrue)
			c.Assert(test.text[at.Offset:], qt.Satisfies, hasPrefix(test.wantAt))
		})
	}
}

// A refusal at the end of the file names the place after its last character: the
// last line when it has no line ending, and column 1 of the next line when it has
// one. No token stands there, so the offset is the length of the text.
func TestRefusalPosition_AtTheEndOfTheFile(t *testing.T) {
	tests := []struct {
		name    string
		text    string
		wantErr string
	}{
		{name: "no final line ending", text: "CREATE TABLE a (\n  id INTEGER", wantErr: `line 2, column 13: expected ',' or '\)' after table element`},
		{name: "a final LF", text: "CREATE TABLE a (\n  id INTEGER\n", wantErr: `line 3, column 1: expected ',' or '\)' after table element`},
		{name: "a final CRLF", text: "CREATE TABLE a (\r\n  id INTEGER\r\n", wantErr: `line 3, column 1: expected ',' or '\)' after table element`},
		{name: "a final CR", text: "CREATE TABLE a (\r  id INTEGER\r", wantErr: `line 3, column 1: expected ',' or '\)' after table element`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := parser.NewParser(test.text).Parse()

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			at, ok := errors.AsType[*ptaherr.ParseError](err)
			c.Assert(ok, qt.IsTrue)
			c.Assert(at.Offset, qt.Equals, len(test.text))
		})
	}
}

// A file that is not UTF-8 still has lines, and each byte that is not valid UTF-8
// is one column, as a Latin-1 editor shows it.
func TestRefusalPosition_ABytePastUTF8(t *testing.T) {
	c := qt.New(t)
	text := "-- caf\xe9\nCREATE TABLE b BOGUS;\n"

	_, err := parser.NewParser(text).Parse()

	c.Assert(err, qt.ErrorMatches, `line 2, column 16: unsupported table option: BOGUS`)
}

// hasPrefix reports whether a string starts with prefix.
func hasPrefix(prefix string) func(string) bool {
	return func(text string) bool { return strings.HasPrefix(text, prefix) }
}
