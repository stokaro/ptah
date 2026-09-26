package lexer_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
)

// Each row is a string token and the text it stands for. The PostgreSQL rows
// are measured on PostgreSQL 18.6: each token compares equal to the text on
// its right, and a routine created with the first, third and last as its body
// stores that text in pg_proc.prosrc (stokaro/ptah#3691).
func TestStringValue_HappyPath(t *testing.T) {
	postgres := dialectlexer.Options("postgres")
	mysql := dialectlexer.Options("mysql")
	tests := []struct {
		name    string
		literal string
		options lexer.Options
		want    string
	}{
		{name: "a doubled quote", literal: `'SELECT ''x'''`, options: postgres, want: `SELECT 'x'`},
		{name: "a backslash in a standard string", literal: `'C:\new'`, options: postgres, want: `C:\new`},
		{name: "an escape string", literal: `E'SELECT \'x\''`, options: postgres, want: `SELECT 'x'`},
		{name: "an escape string in lower case", literal: `e'a\tb'`, options: postgres, want: "a\tb"},
		{name: "an escape string with a doubled quote", literal: `E'it''s'`, options: postgres, want: `it's`},
		{name: "escape string letters", literal: `E'\b\f\n\r\t'`, options: postgres, want: "\b\f\n\r\t"},
		{name: "an escaped backslash", literal: `E'a\\b'`, options: postgres, want: `a\b`},
		{name: "octal and hex", literal: `E'\101\x42\x4'`, options: postgres, want: "AB\x04"},
		{name: "unicode", literal: `E'\u00e9\U0001F600'`, options: postgres, want: "é😀"},
		{name: "any other escaped character", literal: `E'\q'`, options: postgres, want: `q`},
		{name: "a dollar quote", literal: `$$SELECT 'x'$$`, options: postgres, want: `SELECT 'x'`},
		{name: "a tagged dollar quote holding two dollars", literal: `$f$SELECT $$x$$$f$`, options: postgres, want: `SELECT $$x$$`},
		{name: "an empty string", literal: `''`, options: postgres, want: ``},
		{name: "a MySQL backslash escape", literal: `'it\'s\n'`, options: mysql, want: "it's\n"},
		{name: "MySQL keeps the LIKE escapes", literal: `'a\%b\_c'`, options: mysql, want: `a\%b\_c`},
		{name: "a MySQL double-quoted string", literal: `"say ""hi"""`, options: mysql, want: `say "hi"`},
		{name: "E is an identifier without the PostgreSQL option", literal: `'E'`, options: mysql, want: `E`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, ok := lexer.StringValue(test.literal, test.options)

			c.Assert(ok, qt.IsTrue)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// A token that is not a whole string literal has no text, rather than one
// that still holds its quoting.
func TestStringValue_FailurePath(t *testing.T) {
	postgres := dialectlexer.Options("postgres")
	tests := []struct {
		name    string
		literal string
		options lexer.Options
	}{
		{name: "an identifier", literal: `body`, options: postgres},
		{name: "an unterminated string", literal: `'SELECT 1`, options: postgres},
		{name: "a lone quote inside", literal: `'a'b'`, options: postgres},
		{name: "an unterminated dollar quote", literal: `$f$SELECT 1`, options: postgres},
		{name: "a dollar quote closed by another tag", literal: `$f$SELECT 1$g$`, options: postgres},
		{name: "a lone dollar", literal: `$`, options: postgres},
		{name: "an escape string without its option", literal: `E'x'`, options: lexer.Options{StandardStrings: true}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, ok := lexer.StringValue(test.literal, test.options)

			c.Assert(ok, qt.IsFalse)
			c.Assert(got, qt.Equals, "")
		})
	}
}
