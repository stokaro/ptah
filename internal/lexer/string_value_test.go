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

// Each row is a string constant PostgreSQL 18.6 applies, and the text it
// stores: a continued string's segments join, an escape string's are read one
// at a time, and a Unicode escape string's escapes are read once its segments
// are joined, so one may span the gap (stokaro/ptah#3731).
func TestStringValue_PostgreSQLStringConstants_HappyPath(t *testing.T) {
	postgres := dialectlexer.Options("postgres")
	tests := []struct {
		name    string
		literal string
		want    string
	}{
		{name: "a continued string", literal: "'first part, '\n'second part'", want: "first part, second part"},
		{name: "a line comment in the gap", literal: "'a'  -- note\n  'b'", want: "ab"},
		{name: "three segments", literal: "'a'\n\n   'b'\n'c'", want: "abc"},
		{name: "a doubled quote in a continued segment", literal: "'it'\n'''s'", want: "it's"},
		{name: "a continued escape string", literal: "E'a\\t'\n'b\\t'", want: "a\tb\t"},
		{name: "an escape string reads each segment alone", literal: "E'\\x4'\n'1'", want: "\x041"},
		{name: "octal escapes that spell UTF-8", literal: `E'\303\251'`, want: "é"},
		{name: "an escape string surrogate pair", literal: `E'\uD83D\uDE00'`, want: "😀"},
		{name: "an eight-digit surrogate pair", literal: `E'\U0000D83D\U0000DE00'`, want: "😀"},
		{name: "a Unicode escape string", literal: `U&'d\0061t\+000061'`, want: "data"},
		{name: "a doubled backslash", literal: `U&'data \\'`, want: `data \`},
		{name: "a Unicode escape string in lower case", literal: `u&'\0041'`, want: "A"},
		{name: "a doubled quote in a Unicode escape string", literal: `U&'a''b'`, want: "a'b"},
		{name: "a six-digit escape", literal: `U&'\+01F600'`, want: "😀"},
		{name: "a Unicode surrogate pair", literal: `U&'\D83D\DE00'`, want: "😀"},
		{name: "a six-digit surrogate pair", literal: `U&'\+00D83D\+00DE00'`, want: "😀"},
		{name: "an escape across the gap", literal: "U&'\\00'\n'61'", want: "a"},
		{name: "a surrogate pair across the gap", literal: "U&'\\D83D'\n'\\DE00'", want: "😀"},
		{name: "an escape character", literal: `U&'d!0061t!+000061' UESCAPE '!'`, want: "data"},
		{name: "a doubled escape character", literal: `U&'a!!b' UESCAPE '!'`, want: "a!b"},
		{name: "an escape character in lower case", literal: `U&'d!0061' uescape '!'`, want: "da"},
		{name: "an escape character across comments", literal: "U&'d!0061' /* c */ UESCAPE -- x\n '!'", want: "da"},
		{name: "an escape character on the next line", literal: "U&'d!0061'\n UESCAPE '!'", want: "da"},
		{name: "an escape character as an escape string", literal: `U&'d!0061' UESCAPE E'!'`, want: "da"},
		{name: "an escape character between dollar quotes", literal: `U&'d!0061' UESCAPE $$!$$`, want: "da"},
		{name: "the backslash named as the escape character", literal: `U&'\0041' UESCAPE '\'`, want: "A"},
		{name: "a continued string and its escape character", literal: "U&'d!0061'\n'!0062' UESCAPE '!'", want: "dab"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, ok := lexer.StringValue(test.literal, postgres)

			c.Assert(ok, qt.IsTrue)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// Each row is a string PostgreSQL 18.6 refuses, as a syntax error or for an
// escape that names no character, so it has no text here either.
func TestStringValue_PostgreSQLStringConstants_FailurePath(t *testing.T) {
	postgres := dialectlexer.Options("postgres")
	tests := []struct {
		name    string
		literal string
		options lexer.Options
	}{
		{name: "two strings on one line", literal: "'a' 'b'", options: postgres},
		{name: "a block comment in the gap", literal: "'a' /* x */\n'b'", options: postgres},
		{name: "an identifier before a quoted string", literal: "\"a\"\n'b'", options: postgres},
		{name: "a continued string without the option", literal: "'a'\n'b'", options: dialectlexer.Options("cockroachdb")},
		{name: "a Unicode escape string without the option", literal: `U&'\0041'`, options: dialectlexer.Options("cockroachdb")},
		{name: "an escape short of its digits", literal: `E'\u00'`, options: postgres},
		{name: "an escaped zero character", literal: `E'\u0000'`, options: postgres},
		{name: "a zero byte", literal: `E'\0'`, options: postgres},
		{name: "a byte that is not UTF-8", literal: `E'\377'`, options: postgres},
		{name: "a lone low surrogate", literal: `E'\uDE00'`, options: postgres},
		{name: "a high surrogate before a character", literal: `E'\uD83Dx'`, options: postgres},
		{name: "a high surrogate before another escape", literal: `E'\uD83D\u0041'`, options: postgres},
		{name: "a Unicode zero", literal: `U&'\0000'`, options: postgres},
		{name: "a lone high surrogate", literal: `U&'\D800'`, options: postgres},
		{name: "a lone low Unicode surrogate", literal: `U&'\DE00'`, options: postgres},
		{name: "a high surrogate before a letter", literal: `U&'\D83Dx'`, options: postgres},
		{name: "a value past U+10FFFF", literal: `U&'\+110000'`, options: postgres},
		{name: "an escape that is not hex", literal: `U&'a\b'`, options: postgres},
		{name: "a trailing escape character", literal: `U&'\0061\'`, options: postgres},
		{name: "a hex digit as the escape character", literal: `U&'d!0061' UESCAPE 'a'`, options: postgres},
		{name: "two escape characters", literal: `U&'d!0061' UESCAPE '!!'`, options: postgres},
		{name: "a continued escape character", literal: "U&'d!0061' UESCAPE '!'\n'!'", options: postgres},
		{name: "a plus sign as the escape character", literal: `U&'d!0061' UESCAPE '+'`, options: postgres},
		{name: "a space as the escape character", literal: `U&'x' UESCAPE ' '`, options: postgres},
		{name: "a double quote as the escape character", literal: `U&'d"0061' UESCAPE '"'`, options: postgres},
		{name: "a two-byte escape character", literal: `U&'\0061' UESCAPE 'é'`, options: postgres},
		{name: "a Unicode escape string as the escape character", literal: `U&'d!0061' UESCAPE U&'!'`, options: postgres},
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
