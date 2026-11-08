package pgsyntax_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/sqlsyntax/pgsyntax"
)

// shape renders tokens as kind and text, white space left out, so a row reads
// as the server would split the text.
func shape(text string, tokens []pgsyntax.Token) []string {
	var parts []string
	for _, token := range tokens {
		if token.Kind != pgsyntax.Whitespace {
			parts = append(parts, token.Kind.String()+" "+token.Text(text))
		}
	}
	return parts
}

// Each form is one token, read with PostgreSQL's default rules.
func TestScan_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name, text string
		want       []string
	}{
		{name: "a nested block comment", text: "/* a /* b */ c */ x",
			want: []string{"BlockComment /* a /* b */ c */", "Word x"}},
		{name: "a dollar-quoted string holding semicolons and quotes", text: "$fn$ SELECT 'a'; $$; $fn$;",
			want: []string{"DollarString $fn$ SELECT 'a'; $$; $fn$", "Punctuation ;"}},
		{name: "an empty dollar tag", text: "$$it's$$", want: []string{"DollarString $$it's$$"}},
		{name: "a positional parameter", text: "$1 + $12", want: []string{"Parameter $1", "Operator +", "Parameter $12"}},
		{name: "an escape string", text: `E'it\'s' e'\\'`, want: []string{`String E'it\'s'`, `String e'\\'`}},
		{name: "a backslash in an ordinary string is a character", text: `'a\' 'b'`, want: []string{`String 'a\'`, `String 'b'`}},
		{name: "a doubled quote", text: `'it''s' "a""b"`, want: []string{`String 'it''s'`, `QuotedIdentifier "a""b"`}},
		{name: "a Unicode string and identifier", text: `U&'d\0061t' UESCAPE '!' u&"x"`,
			want: []string{`String U&'d\0061t'`, "Word UESCAPE", "String '!'", `QuotedIdentifier u&"x"`}},
		{name: "bit, hex and national strings", text: "B'101' X'1F' N'n'", want: []string{"String B'101'", "String X'1F'", "String N'n'"}},
		{name: "a line comment ends at its line", text: "a -- b; c\nd", want: []string{"Word a", "LineComment -- b; c", "Word d"}},
		{name: "a comment that starts inside an operator", text: "a +-- c\n+/* d */b",
			want: []string{"Word a", "Operator +", "LineComment -- c", "Operator +", "BlockComment /* d */", "Word b"}},
		{name: "numbers", text: "1 1.5 .5 1e10 2.5E-3 0x1F 1_000", want: []string{"Number 1", "Number 1.5", "Number .5", "Number 1e10",
			"Number 2.5E-3", "Number 0x1F", "Number 1_000"}},
		{name: "a range is not a fraction", text: "1..2", want: []string{"Number 1", "Punctuation ..", "Number 2"}},
		{name: "a name with a dollar sign and a cast", text: "a$b::text", want: []string{"Word a$b", "Punctuation ::", "Word text"}},
		{name: "a colon before a colon-equals", text: "a[1:2] x := 1", want: []string{"Word a", "Punctuation [", "Number 1", "Punctuation :", "Number 2",
			"Punctuation ]", "Word x", "Punctuation :=", "Number 1"}},
		{name: "punctuation", text: "f(a[1], b);", want: []string{"Word f", "Punctuation (", "Word a", "Punctuation [", "Number 1",
			"Punctuation ]", "Punctuation ,", "Word b", "Punctuation )", "Punctuation ;"}},
		{name: "a lone dollar sign", text: "a $ b", want: []string{"Word a", "Operator $", "Word b"}},
		{name: "a name in another script", text: "заказ.id", want: []string{"Word заказ", "Punctuation .", "Word id"}},
		{name: "a segment that continues an escape string reads its escapes", text: "E'a' -- c\n'b\\'c' x",
			want: []string{"String E'a'", "LineComment -- c", `String 'b\'c'`, "Word x"}},
		{name: "a block comment ends the constant", text: "E'a' /* c */\n'b\\' x",
			want: []string{"String E'a'", "BlockComment /* c */", `String 'b\'`, "Word x"}},
		{name: "a segment on the same line continues nothing", text: "E'a' 'b\\' x",
			want: []string{"String E'a'", `String 'b\'`, "Word x"}},
		{name: "a vertical tab is white space", text: "1\v+1", want: []string{"Number 1", "Operator +", "Number 1"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			tokens, err := pgsyntax.Scan(test.text, pgsyntax.Options{})

			c.Assert(err, qt.IsNil)
			c.Assert(shape(test.text, tokens), qt.DeepEquals, test.want)
		})
	}
}

// The tokens cover the text with no gap and no overlap.
func TestScan_CoversTheText(t *testing.T) {
	c := qt.New(t)
	text := "CREATE TABLE \"t\" (a text DEFAULT E'x\\'y' /* c /* d */ */ -- e\n);\n$$ $$"

	tokens, err := pgsyntax.Scan(text, pgsyntax.Options{})

	c.Assert(err, qt.IsNil)
	end := 0
	for _, token := range tokens {
		c.Assert(token.Start, qt.Equals, end)
		end = token.End
	}
	c.Assert(end, qt.Equals, len(text))
}

// With standard_conforming_strings off, a backslash in an ordinary string
// escapes the quote after it.
func TestScan_BackslashEscapes(t *testing.T) {
	c := qt.New(t)
	text := `'it\'s' x`

	tokens, err := pgsyntax.Scan(text, pgsyntax.Options{BackslashEscapes: true})

	c.Assert(err, qt.IsNil)
	c.Assert(shape(text, tokens), qt.DeepEquals, []string{`String 'it\'s'`, "Word x"})
}

// DuckDB 2.0 reads a segment that continues an escape string as an ordinary
// string: E'a', then 'b\' on the next line, is one constant and a semicolon
// after it ends the statement. Measured with duckdb-go v2.20000.0, where
// running both statements of the text left the value ab\ and the second
// table. PostgreSQL reads the segment with escapes and refuses the text.
func TestScan_PlainContinuations(t *testing.T) {
	c := qt.New(t)
	text := "E'a'\n'b\\'; x"

	tokens, err := pgsyntax.Scan(text, pgsyntax.Options{PlainContinuations: true})

	c.Assert(err, qt.IsNil)
	c.Assert(shape(text, tokens), qt.DeepEquals, []string{"String E'a'", `String 'b\'`, "Punctuation ;", "Word x"})
}

// A construct that does not close is refused at the byte where it opens.
func TestScan_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name, text string
		offset     int
		reason     string
	}{
		{name: "a string", text: "a 'b", offset: 2, reason: "a string does not close"},
		{name: "an escaped quote leaves a string open", text: `E'a\'`, offset: 1, reason: "a string does not close"},
		{name: "a segment that continues an escape string reads its escapes", text: "E'a'\n'b\\'; x", offset: 5, reason: "a string does not close"},
		{name: "a quoted identifier", text: `"a`, offset: 0, reason: "a quoted identifier does not close"},
		{name: "a dollar-quoted string", text: "x $f$ a $g$", offset: 2, reason: "a dollar-quoted string $f$ does not close"},
		{name: "a nested block comment", text: "/* a /* b */", offset: 0, reason: "a block comment does not close"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			tokens, err := pgsyntax.Scan(test.text, pgsyntax.Options{})

			c.Assert(err, qt.DeepEquals, error(&pgsyntax.SyntaxError{Offset: test.offset, Reason: test.reason}))
			c.Assert(tokens, qt.IsNil)
		})
	}
}

// Statements split at semicolons outside strings, identifiers, dollar quotes and
// comments, and leave out what is only white space and comments.
func TestStatements_HappyPath(t *testing.T) {
	c := qt.New(t)
	text := "-- head\nCREATE FUNCTION f() RETURNS text AS $$ SELECT ';' $$ LANGUAGE sql;\n;\n/* only a comment */;\n" +
		"INSERT INTO \"a;b\" VALUES ('c;d') /* e; */\n"

	statements, err := pgsyntax.Statements(text, pgsyntax.Options{})

	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.HasLen, 2)
	c.Assert(statements[0].Text, qt.Equals, "CREATE FUNCTION f() RETURNS text AS $$ SELECT ';' $$ LANGUAGE sql")
	c.Assert(statements[1].Text, qt.Equals, `INSERT INTO "a;b" VALUES ('c;d')`)
	c.Assert(text[statements[1].Start:statements[1].End], qt.Equals, statements[1].Text)
}

// A text the scanner refuses has no statements.
func TestStatements_FailurePath(t *testing.T) {
	c := qt.New(t)

	statements, err := pgsyntax.Statements("SELECT 1; SELECT 'a", pgsyntax.Options{})

	c.Assert(err, qt.DeepEquals, error(&pgsyntax.SyntaxError{Offset: 17, Reason: "a string does not close"}))
	c.Assert(statements, qt.IsNil)
}

// A quoted identifier or string reads back as one token under either setting
// of standard_conforming_strings, and an identifier keeps its case.
func TestQuote_ReadsBack(t *testing.T) {
	for _, test := range []struct {
		name, quoted, want string
		kind               pgsyntax.Kind
	}{
		{name: "an identifier with a quote and upper case", quoted: pgsyntax.QuoteIdentifier(`My "T"`), want: `"My ""T"""`, kind: pgsyntax.QuotedIdentifier},
		{name: "a string with a quote", quoted: pgsyntax.QuoteString(`it's`), want: `'it''s'`, kind: pgsyntax.String},
		{name: "a string with a backslash", quoted: pgsyntax.QuoteString(`a\'b`), want: `E'a\\''b'`, kind: pgsyntax.String},
	} {
		for _, options := range []pgsyntax.Options{{}, {BackslashEscapes: true}} {
			t.Run(test.name, func(t *testing.T) {
				c := qt.New(t)

				tokens, err := pgsyntax.Scan(test.quoted, options)

				c.Assert(test.quoted, qt.Equals, test.want)
				c.Assert(err, qt.IsNil)
				c.Assert(tokens, qt.DeepEquals, []pgsyntax.Token{{Kind: test.kind, Start: 0, End: len(test.want)}})
			})
		}
	}
}

// Next reads one construct and stops there, whatever follows it, so a comment is
// read to its own end although the text after it never closes a string.
func TestNext_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name, text string
		want       pgsyntax.Token
	}{
		{name: "a nested comment before an unclosed string", text: "/* a /* b */ */ 'open", want: pgsyntax.Token{Kind: pgsyntax.BlockComment, End: 15}},
		{name: "a dollar quote", text: "$x$;$x$ rest", want: pgsyntax.Token{Kind: pgsyntax.DollarString, End: 7}},
		{name: "a word", text: "abc def", want: pgsyntax.Token{Kind: pgsyntax.Word, End: 3}},
		{name: "no text", text: "", want: pgsyntax.Token{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			token, err := pgsyntax.Next(test.text, pgsyntax.Options{})

			c.Assert(err, qt.IsNil)
			c.Assert(token, qt.DeepEquals, test.want)
		})
	}
}

// A first construct that does not close is refused.
func TestNext_FailurePath(t *testing.T) {
	c := qt.New(t)

	token, err := pgsyntax.Next("/* a /* b */ c", pgsyntax.Options{})

	c.Assert(err, qt.DeepEquals, error(&pgsyntax.SyntaxError{Offset: 0, Reason: "a block comment does not close"}))
	c.Assert(token, qt.Equals, pgsyntax.Token{})
}
