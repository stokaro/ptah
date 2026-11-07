package pgsyntax_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/stokaro/ptah/sqlsyntax/pgsyntax"
)

// valueAt reads the constant that starts the text, and returns its value, the
// text of the tokens it spans, and whether it stands for a value.
func valueAt(c *qt.C, text string, options pgsyntax.Options) (value, span string, ok bool) {
	tokens, err := pgsyntax.Scan(text, options)
	c.Assert(err, qt.IsNil)
	value, next, ok := pgsyntax.Value(text, tokens, 0, options)
	return value, text[tokens[0].Start:tokens[next-1].End], ok
}

// Each row's value is what PostgreSQL 18.6 returned for SELECT of the text,
// measured with psql -c, which sends the text unchanged.
func TestValue_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name, text, want, span string
	}{
		{name: "a doubled quote", text: "'it''s' x", want: "it's", span: "'it''s'"},
		{name: "a national string", text: "N'n'", want: "n", span: "N'n'"},
		{name: "an escape string's escapes", text: `E'\101\x41\u0041\U00000041\q\n'`, want: "AAAAq\n", span: `E'\101\x41\u0041\U00000041\q\n'`},
		{name: "an escaped and a doubled quote", text: `E'it\'s''s'`, want: "it's's", span: `E'it\'s''s'`},
		{name: "octal escapes that write UTF-8, the first past 0xFF keeping its low byte", text: `E'\703\251'`, want: "é", span: `E'\703\251'`},
		{name: "hex escapes that write UTF-8", text: `E'\xc3\xa9'`, want: "é", span: `E'\xc3\xa9'`},
		{name: "a surrogate pair in an escape string", text: `E'\uD83D\uDE00'`, want: "\U0001F600", span: `E'\uD83D\uDE00'`},
		{name: "a segment on a later line", text: "'a' -- c\n'b' x", want: "ab", span: "'a' -- c\n'b'"},
		{name: "a segment after blank lines and a comment", text: "'a'\n\n  -- c\n  'b'", want: "ab", span: "'a'\n\n  -- c\n  'b'"},
		{name: "an escape string's segment reads escapes", text: "E'a'\n'b\\'c'", want: "ab'c", span: "E'a'\n'b\\'c'"},
		{name: "an escape string's segments each read escapes", text: "E'\\101'\n'\\102'", want: "AB", span: "E'\\101'\n'\\102'"},
		{name: "a national string's segment", text: "N'a'\n'b'", want: "ab", span: "N'a'\n'b'"},
		{name: "a segment on the same line continues nothing", text: "'a'  \t 'b'", want: "a", span: "'a'"},
		{name: "a block comment ends the constant", text: "'a' /* c */\n'b'", want: "a", span: "'a'"},
		{name: "an escape string does not continue a string", text: "'a'\nE'b'", want: "a", span: "'a'"},
		{name: "a Unicode escape string", text: `U&'d\0061t'`, want: "dat", span: `U&'d\0061t'`},
		{name: "a six-digit Unicode escape", text: `U&'\+01F600'`, want: "\U0001F600", span: `U&'\+01F600'`},
		{name: "a surrogate pair in a Unicode escape string", text: `U&'\D83D\DE00'`, want: "\U0001F600", span: `U&'\D83D\DE00'`},
		{name: "a UESCAPE clause", text: "U&'d!0061t' UESCAPE '!' x", want: "dat", span: "U&'d!0061t' UESCAPE '!'"},
		{name: "a doubled escape character", text: "U&'!!x' UESCAPE '!'", want: "!x", span: "U&'!!x' UESCAPE '!'"},
		{name: "segments joined before the escapes are read", text: "U&'a'\n'\\0062'", want: "ab", span: "U&'a'\n'\\0062'"},
		{name: "a UESCAPE clause among comments", text: "U&'a!0062' -- x\n UESCAPE /* y */ '!'", want: "ab",
			span: "U&'a!0062' -- x\n UESCAPE /* y */ '!'"},
		{name: "a UESCAPE clause naming an escape string", text: "U&'a' UESCAPE E'!'", want: "a", span: "U&'a' UESCAPE E'!'"},
		{name: "a UESCAPE clause naming a dollar-quoted string", text: "U&'a' UESCAPE $$!$$", want: "a", span: "U&'a' UESCAPE $$!$$"},
		{name: "a dollar-quoted string", text: "$fn$ it's $$ $fn$;", want: " it's $$ ", span: "$fn$ it's $$ $fn$"},
		{name: "a quoted identifier", text: `"a""b".c`, want: `a"b`, span: `"a""b"`},
		{name: "a Unicode escape identifier", text: `U&"a\0062"`, want: "ab", span: `U&"a\0062"`},
		{name: "a Unicode escape identifier with a UESCAPE clause", text: `U&"x!0079" UESCAPE '!'`, want: "xy", span: `U&"x!0079" UESCAPE '!'`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			value, span, ok := valueAt(c, test.text, pgsyntax.Options{})

			c.Assert(ok, qt.IsTrue)
			c.Assert(value, qt.Equals, test.want)
			c.Assert(span, qt.Equals, test.span)
		})
	}
}

// Under PlainContinuations a continued segment is scanned as an ordinary
// string, and its text still reads escapes once joined, as DuckDB 2.0 reads
// it: E'a\n' and 'b\n' on the next line stand for a, a line ending, b and a
// line ending, and E'a' and 'b\' for ab\.
func TestValue_PlainContinuations(t *testing.T) {
	for _, test := range []struct {
		name, text, want, span string
	}{
		{name: "escapes in the continued segment", text: "E'a\\n'\n'b\\n'", want: "a\nb\n", span: "E'a\\n'\n'b\\n'"},
		{name: "a backslash before the segment's closing quote", text: "E'a'\n'b\\'; x", want: "ab\\", span: "E'a'\n'b\\'"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			value, span, ok := valueAt(c, test.text, pgsyntax.Options{PlainContinuations: true})

			c.Assert(ok, qt.IsTrue)
			c.Assert(value, qt.Equals, test.want)
			c.Assert(span, qt.Equals, test.span)
		})
	}
}

// With standard_conforming_strings off, an ordinary string reads backslash
// escapes, in each of its segments.
func TestValue_BackslashEscapes(t *testing.T) {
	c := qt.New(t)

	value, span, ok := valueAt(c, "'it\\'s'\n'\\101' x", pgsyntax.Options{BackslashEscapes: true})

	c.Assert(ok, qt.IsTrue)
	c.Assert(value, qt.Equals, "it'sA")
	c.Assert(span, qt.Equals, "'it\\'s'\n'\\101'")
}

// A token that stands for no text, and a constant the server refuses, read no
// value. Each refused row names the error PostgreSQL 18.6 gave.
func TestValue_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name, text string
	}{
		{name: "a word", text: "a"},
		{name: "a number", text: "1"},
		{name: "a bit string", text: "B'101'"},
		{name: "a hex bit string", text: "X'1F'"},
		{name: "an escape that writes a byte that is not UTF-8: invalid byte sequence", text: `E'\xff'`},
		{name: "an escape that writes a zero byte: invalid byte sequence", text: `E'\0'`},
		{name: "a high surrogate alone: invalid Unicode surrogate pair", text: `E'\uD83D'`},
		{name: "a short Unicode escape in an escape string: invalid Unicode escape", text: `E'\u12'`},
		{name: "a short Unicode escape: invalid Unicode escape", text: `U&'\00'`},
		{name: "an escape character that starts no escape: invalid Unicode escape", text: "U&'!x' UESCAPE '!'"},
		{name: "a UESCAPE constant of two characters: invalid Unicode escape character", text: "U&'a' UESCAPE 'ab'"},
		{name: "a UESCAPE constant of two characters that are no hex digits: invalid Unicode escape character", text: "U&'a' UESCAPE '!?'"},
		{name: "a plus sign as the escape character: invalid Unicode escape character", text: "U&'a' UESCAPE '+'"},
		{name: "a hex digit as the escape character: invalid Unicode escape character", text: "U&'a' UESCAPE 'a'"},
		{name: "a Unicode escape string as the escape character: UESCAPE must be followed by a simple string literal", text: "U&'a' UESCAPE U&'!'"},
		{name: "a UESCAPE keyword without a constant", text: "U&'a' UESCAPE x"},
		{name: "a code point past 32 bits of hex: invalid Unicode escape value", text: `E'\UFFFFFFFF'`},
		{name: "a zero-length quoted identifier: zero-length delimited identifier", text: `""`},
		{name: "a zero-length Unicode escape identifier: zero-length delimited identifier", text: `U&""`},
		{name: "a Unicode escape that names zero: invalid Unicode escape value", text: `U&"\0000"`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			tokens, err := pgsyntax.Scan(test.text, pgsyntax.Options{})
			c.Assert(err, qt.IsNil)

			value, next, ok := pgsyntax.Value(test.text, tokens, 0, pgsyntax.Options{})

			c.Assert(ok, qt.IsFalse)
			c.Assert(value, qt.Equals, "")
			c.Assert(next, qt.Equals, 1)
		})
	}
}

// With standard_conforming_strings off, the server refuses a Unicode escape
// string and identifier.
func TestValue_UnicodeEscapesUnderBackslashEscapes(t *testing.T) {
	for _, text := range []string{`U&'a'`, `U&"a"`} {
		t.Run(text, func(t *testing.T) {
			c := qt.New(t)
			options := pgsyntax.Options{BackslashEscapes: true}
			tokens, err := pgsyntax.Scan(text, options)
			c.Assert(err, qt.IsNil)

			value, next, ok := pgsyntax.Value(text, tokens, 0, options)

			c.Assert(ok, qt.IsFalse)
			c.Assert(value, qt.Equals, "")
			c.Assert(next, qt.Equals, 1)
		})
	}
}

// An index outside the tokens reads nothing.
func TestValue_OutsideTheTokens(t *testing.T) {
	for _, test := range []struct {
		name  string
		index int
	}{{name: "before the first", index: -1}, {name: "past the last", index: 1}} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			tokens, err := pgsyntax.Scan("'a'", pgsyntax.Options{})
			c.Assert(err, qt.IsNil)

			value, next, ok := pgsyntax.Value("'a'", tokens, test.index, pgsyntax.Options{})

			c.Assert(ok, qt.IsFalse)
			c.Assert(value, qt.Equals, "")
			c.Assert(next, qt.Equals, test.index+1)
		})
	}
}
