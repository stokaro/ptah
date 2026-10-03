package revisiontable_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/lexer"
	"ptah.run/internal/revisiontable"
)

// A YQL literal escapes a backslash and a quote with a backslash and is typed
// Utf8. Measured on YDB 26.2.1.14: '20260103_a\\b\'c'u reads back as
// 20260103_a\b'c of type Utf8.
func TestVersionLiteral_YDB(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  string
	}{
		{name: "a plain version", value: "20260103120000", want: `'20260103120000'u`},
		{name: "a quote and a backslash", value: `20260103_a\b'c`, want: `'20260103_a\\b\'c'u`},
		{name: "a doubled quote stays two quotes", value: `a''b`, want: `'a\'\'b'u`},
		{name: "a semicolon and comment markers", value: "a;--b/*c", want: `'a;--b/*c'u`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(revisiontable.VersionLiteral("ydb", test.value), qt.Equals, test.want)
		})
	}
}

// The literal reads back as the value under the lexer's own YQL rules, so the
// revision a statement names is the one it was given, whatever it holds.
func TestVersionLiteral_YDBReadsBackUnderTheYQLLexer(t *testing.T) {
	for _, value := range []string{"20260103120000", `a\b'c"d`, "a\nb", "@@x@@", `\'`, "ü"} {
		c := qt.New(t)
		literal := revisiontable.VersionLiteral("ydbs", value)

		l := lexer.NewLexerWithOptions(literal, lexer.Options{YQL: true})
		token := l.NextToken()
		text, ok := lexer.StringValue(token.Value, lexer.Options{YQL: true})

		c.Assert(token, qt.Equals, lexer.Token{Type: lexer.TokenString, Value: literal, Start: 0, End: len(literal)})
		c.Assert(l.NextToken().Type, qt.Equals, lexer.TokenEOF)
		c.Assert(ok, qt.IsTrue)
		c.Assert(text, qt.Equals, value)
	}
}
