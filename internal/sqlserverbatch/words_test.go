package sqlserverbatch_test

import (
	"regexp"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
	"ptah.run/internal/sqlserverbatch"
)

// lexerTokens returns the tokens the lexer reads from text, with no
// whitespace and no comments.
func lexerTokens(text string) []lexer.Token {
	var tokens []lexer.Token
	lex := lexer.NewLexerWithOptions(text, dialectlexer.Options(platform.SQLServer))
	for tok := lex.NextToken(); tok.Type != lexer.TokenEOF; tok = lex.NextToken() {
		tokens = append(tokens, tok)
	}
	return slices.DeleteFunc(tokens, func(tok lexer.Token) bool {
		return tok.Type == lexer.TokenWhitespace || tok.Type == lexer.TokenComment
	})
}

// lexerWords returns the values of the tokens of text, joined by JoinWords.
func lexerWords(c *qt.C, text string) []string {
	c.Helper()
	joined, err := sqlserverbatch.JoinWords(text, lexerTokens(text))
	c.Assert(err, qt.IsNil)
	values := make([]string, len(joined))
	for index, tok := range joined {
		values[index] = tok.Value
	}
	return values
}

// TestJoinWords_JoinsWhatTheServerReadsAsOneName holds names the server reads
// as one word, each measured on SQL Server 2025: a database x@, x#y, x@@ or
// SELECT@x received the row an INSERT wrote through the name, and #t@x is one
// temporary table. A bracketed name, a name before a space and a variable are
// not joined.
func TestJoinWords_JoinsWhatTheServerReadsAsOneName(t *testing.T) {
	tests := []struct {
		name string
		text string
		want []string
	}{
		{name: "a name ending in @", text: "x@.dbo.kept", want: []string{"x@", ".", "dbo", ".", "kept"}},
		{name: "a name with @ inside", text: "x@d_at2v.dbo.kept", want: []string{"x@d_at2v", ".", "dbo", ".", "kept"}},
		{name: "a name ending in @@", text: "x@@.dbo.kept", want: []string{"x@@", ".", "dbo", ".", "kept"}},
		{name: "a name with # inside", text: "x#y.dbo.kept", want: []string{"x#y", ".", "dbo", ".", "kept"}},
		{name: "a name ending in #", text: "x#.dbo.kept", want: []string{"x#", ".", "dbo", ".", "kept"}},
		{name: "a temporary name with @ inside", text: "#t@x", want: []string{"#t@x"}},
		{name: "a statement word joined to a variable", text: "SELECT@x", want: []string{"SELECT@x"}},
		{name: "a name ending in @ before a space", text: "x@ .dbo.kept", want: []string{"x@", ".", "dbo", ".", "kept"}},
		{name: "a bracketed name before a variable", text: "[x]@y", want: []string{"[x]", "@", "y"}},
		{name: "a name before a space and a variable", text: "x @y", want: []string{"x", "@", "y"}},
		{name: "a variable", text: "@v@w", want: []string{"@", "v@w"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(lexerWords(c, test.text), qt.DeepEquals, test.want)
		})
	}
}

// A word whose end falls inside a token the lexer reads is refused rather
// than cut: in x@1.5 the lexer reads the number 1.5, where the server reads
// the name x@1.
func TestJoinWords_RefusesAWordTheLexerReadsDifferently(t *testing.T) {
	c := qt.New(t)
	text := "SELECT x@1.5"

	joined, err := sqlserverbatch.JoinWords(text, lexerTokens(text))

	c.Assert(err, qt.ErrorIs, sqlserverbatch.ErrUnreadable)
	c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(
		"cannot find the T-SQL statement boundaries: a name the lexer reads differently at offset 7"))
	c.Assert(joined, qt.IsNil)
}

// TestRead_ReadsANameWithAnAtOrAHashAsOneWord splits batches whose names hold
// an @ or a # after their first character. Read as the lexer reads them, the
// # would be refused and the @ would start a variable.
func TestRead_ReadsANameWithAnAtOrAHashAsOneWord(t *testing.T) {
	tests := []struct {
		name string
		text string
		want []string
	}{
		{
			name: "a database name ending in @",
			text: "INSERT INTO x@.dbo.kept VALUES (2) SELECT 1",
			want: []string{"simple: INSERT INTO x@.dbo.kept VALUES (2)", "simple: SELECT 1"},
		},
		{
			name: "a database name with # inside",
			text: "INSERT INTO x#y.dbo.kept VALUES (1) SELECT 1",
			want: []string{"simple: INSERT INTO x#y.dbo.kept VALUES (1)", "simple: SELECT 1"},
		},
		{
			name: "an alias with @ inside",
			text: "SELECT 1 AS a@b SELECT 2",
			want: []string{"simple: SELECT 1 AS a@b", "simple: SELECT 2"},
		},
		{
			name: "an alias ending in #",
			text: "SELECT 1 AS x# SELECT 2",
			want: []string{"simple: SELECT 1 AS x#", "simple: SELECT 2"},
		},
		{
			name: "a temporary table with @ inside",
			text: "CREATE TABLE #t@x (id int) DROP TABLE #t@x",
			want: []string{"simple: CREATE TABLE #t@x (id int)", "simple: DROP TABLE #t@x"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statements, err := sqlserverbatch.Read(test.text)
			c.Assert(err, qt.IsNil)
			c.Assert(render(statements, ""), qt.DeepEquals, test.want)
		})
	}
}

// TestRegularIdentifier covers the words the server reads unquoted.
func TestRegularIdentifier(t *testing.T) {
	tests := []struct {
		word string
		want bool
	}{
		{word: "items", want: true},
		{word: "_x", want: true},
		{word: "#t", want: true},
		{word: "x@d", want: true},
		{word: "x$y#z1", want: true},
		{word: "é", want: true},
		{word: "@v", want: false},
		{word: "1x", want: false},
		{word: "$x", want: false},
		{word: "x y", want: false},
		{word: "x.y", want: false},
		{word: "", want: false},
	}
	for _, test := range tests {
		t.Run(test.word, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(sqlserverbatch.RegularIdentifier(test.word), qt.Equals, test.want)
		})
	}
}
