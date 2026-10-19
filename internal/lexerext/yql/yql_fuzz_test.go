package yql_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/lexer"
	"ptah.run/internal/lexerext/yql"
)

// FuzzScanner holds that the YQL scanner reaches the end of any text, within
// one token per byte and its EOF token.
func FuzzScanner(f *testing.F) {
	for _, seed := range []string{
		"--!syntax_v1\nSELECT @@a@@@@b@@j, \"x\\\"y\"u, `t\\`n`;",
		"SELECT 0x1fAS, 1.foo, 1uFROM `dir/t`;",
		"@@unterminated",
		"'unterminated \\'",
		"/* unterminated",
		"$тег$héllo$тег$",
	} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, input string) {
		c := qt.New(t)
		l := lexer.NewLexerWithOptions(input, lexer.Options{Scanner: yql.Scanner{}})
		reached := false
		for tokens := 0; tokens <= len(input)+1 && !reached; tokens++ {
			reached = l.NextToken().Type == lexer.TokenEOF
		}
		c.Assert(reached, qt.IsTrue, qt.Commentf("no EOF after %d tokens", len(input)+1))
	})
}
