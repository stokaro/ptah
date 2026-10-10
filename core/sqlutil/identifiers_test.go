package sqlutil_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/sqlutil"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
)

func TestQuoteYQLIdentifier_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{name: "a plain name", input: "items", want: "`items`"},
		{name: "a path and a literal dot keep their spelling", input: "jobs.daily/tick", want: "`jobs.daily/tick`"},
		{name: "a backtick is escaped, not doubled", input: "a`b", want: "`a\\`b`"},
		{name: "a backslash is escaped", input: `a\b`, want: "`a\\\\b`"},
		{name: "a trailing backslash cannot escape the closing backtick", input: `a\`, want: "`a\\\\`"},
		{name: "an empty name", input: "", want: "``"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(sqlutil.QuoteYQLIdentifier(test.input), qt.Equals, test.want)
		})
	}
}

// TestQuoteYQLIdentifier_TheLexerReadsOneName holds the quoting to the YQL
// lexer that reads it back: the quoted name is one token that ends where the
// quoting ends, so no name can close its backticks early and let the rest of
// it be read as YQL.
func TestQuoteYQLIdentifier_TheLexerReadsOneName(t *testing.T) {
	for _, name := range []string{"items", "a`b", `a\`, "a\\`; DROP TABLE x; --", "``"} {
		t.Run(name, func(t *testing.T) {
			c := qt.New(t)
			quoted := sqlutil.QuoteYQLIdentifier(name)

			token := lexer.NewLexerWithOptions(quoted+" AS x", dialectlexer.Options(platform.YDB)).NextToken()

			c.Assert(token.Start, qt.Equals, 0)
			c.Assert(token.End, qt.Equals, len(quoted))
		})
	}
}
