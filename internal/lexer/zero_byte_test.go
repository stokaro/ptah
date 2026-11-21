package lexer_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dialectlexer"
)

// TestLexer_AZeroByteIsNotTheEnd pins that a zero byte is read as a character
// in every construct, so the text after it is read too. A lexer that took the
// byte for the end of the input dropped the statements after it, and
// `ptah migrations up` recorded the version as applied without them.
func TestLexer_AZeroByteIsNotTheEnd(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  []string
	}{
		{
			name: "between statements", input: "SELECT 1 \x00; CREATE TABLE t (id int)",
			want: []string{"Identifier SELECT", "Identifier 1", "Operator \x00", "Semicolon ;", "Identifier CREATE", "Identifier TABLE",
				"Identifier t", "Operator (", "Identifier id", "Identifier int", "Operator )"},
		},
		{
			name: "in a line comment", input: "SELECT 1 -- \x00\nSELECT 2",
			want: []string{"Identifier SELECT", "Identifier 1", "Identifier SELECT", "Identifier 2"},
		},
		{
			name: "in a block comment", input: "SELECT 1 /* \x00 */; SELECT 2",
			want: []string{"Identifier SELECT", "Identifier 1", "Semicolon ;", "Identifier SELECT", "Identifier 2"},
		},
		{
			name: "in a string", input: "SELECT '\x00'; SELECT 2",
			want: []string{"Identifier SELECT", "String '\x00'", "Semicolon ;", "Identifier SELECT", "Identifier 2"},
		},
		{
			name: "in a dollar-quoted string", input: "SELECT $$\x00$$; SELECT 2",
			want: []string{"Identifier SELECT", "String $$\x00$$", "Semicolon ;", "Identifier SELECT", "Identifier 2"},
		},
		{
			name: "in a quoted identifier", input: "SELECT 1 AS \"\x00\"; SELECT 2",
			want: []string{"Identifier SELECT", "Identifier 1", "Identifier AS", "String \"\x00\"", "Semicolon ;", "Identifier SELECT", "Identifier 2"},
		},
		{
			name: "after a backslash in a string", input: "SELECT 'a\\\x00'; SELECT 2",
			want: []string{"Identifier SELECT", "String 'a\\\x00'", "Semicolon ;", "Identifier SELECT", "Identifier 2"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(significantTokens(test.input, dialectlexer.Options("postgres")), qt.DeepEquals, test.want)
		})
	}
}

// TestLexer_AZeroByteInMySQLText pins the same for the MySQL lexer, whose
// backticks, backslash escapes and dash-dash rule read a zero byte through
// their own paths.
func TestLexer_AZeroByteInMySQLText(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  []string
	}{
		{
			name: "in a backticked identifier", input: "SELECT a AS `\x00`; SELECT b",
			want: []string{"Identifier SELECT", "Identifier a", "Identifier AS", "Identifier `\x00`", "Semicolon ;", "Identifier SELECT", "Identifier b"},
		},
		{
			name: "after a backslash in a string", input: "SELECT 'a\\\x00'; SELECT b",
			want: []string{"Identifier SELECT", "String 'a\\\x00'", "Semicolon ;", "Identifier SELECT", "Identifier b"},
		},
		{
			name: "after a dash-dash", input: "SELECT a --\x00\nSELECT b",
			want: []string{"Identifier SELECT", "Identifier a", "Identifier SELECT", "Identifier b"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(significantTokens(test.input, dialectlexer.Options("mysql")), qt.DeepEquals, test.want)
		})
	}
}

// TestLexer_AZeroByteInABracketedIdentifier pins the same for the bracketed
// identifiers of SQLite and SQL Server, whose scanner has its own end check.
func TestLexer_AZeroByteInABracketedIdentifier(t *testing.T) {
	c := qt.New(t)
	c.Assert(significantTokens("SELECT a AS [\x00]; SELECT b", dialectlexer.Options("sqlite")), qt.DeepEquals,
		[]string{"Identifier SELECT", "Identifier a", "Identifier AS", "Identifier [\x00]", "Semicolon ;", "Identifier SELECT", "Identifier b"})
}
