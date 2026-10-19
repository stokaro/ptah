package lexer_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/lexer"
)

// sharedToken is the part of a token a row pins: its type and its text.
type sharedToken struct {
	Type  lexer.TokenType
	Value string
}

// lexedTokens lexes input under options and keeps each token's type and
// text, without the EOF token.
func lexedTokens(input string, options lexer.Options) []sharedToken {
	l := lexer.NewLexerWithOptions(input, options)
	var tokens []sharedToken
	for token := l.NextToken(); token.Type != lexer.TokenEOF; token = l.NextToken() {
		tokens = append(tokens, sharedToken{Type: token.Type, Value: token.Value})
	}
	return tokens
}

// The shared rules read each of these texts by their own reading, where YDB's
// YQL lexer reads it otherwise: no rule of an engine's own lexer reaches the
// shared scanner. Each row is lexed under the options a dialect read by the
// shared rules uses.
func TestLexer_SharedRulesKeepTheirReading(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		options lexer.Options
		want    []sharedToken
	}{
		{
			name:    "MySQL system variables keep their at signs apart",
			input:   "@@session.x;",
			options: lexer.Options{StandardStrings: true, BackslashEscapes: true},
			want: []sharedToken{
				{lexer.TokenOperator, "@"}, {lexer.TokenOperator, "@"}, {lexer.TokenIdentifier, "session"},
				{lexer.TokenOperator, "."}, {lexer.TokenIdentifier, "x"}, {lexer.TokenSemicolon, ";"},
			},
		},
		{
			name:    "PostgreSQL keeps its dollar quote",
			input:   "$a$;$a$",
			options: lexer.Options{StandardStrings: true, PostgreSQLEscapeStrings: true},
			want:    []sharedToken{{lexer.TokenString, "$a$;$a$"}},
		},
		{
			name:    "MySQL keeps its doubled quote",
			input:   `'a\''';'`,
			options: lexer.Options{StandardStrings: true, BackslashEscapes: true},
			want:    []sharedToken{{lexer.TokenString, `'a\''';'`}},
		},
		{
			name:    "MySQL keeps its hash comment",
			input:   "# c;",
			options: lexer.Options{StandardStrings: true, BackslashEscapes: true},
			want:    []sharedToken{{lexer.TokenComment, "# c;"}},
		},
		{
			name:    "a suffix stays a separate identifier",
			input:   `'x'u`,
			options: lexer.Options{StandardStrings: true},
			want:    []sharedToken{{lexer.TokenString, `'x'`}, {lexer.TokenIdentifier, "u"}},
		},
		{
			name:    "a leading bang comment stays a comment",
			input:   "--!x",
			options: lexer.Options{StandardStrings: true},
			want:    []sharedToken{{lexer.TokenComment, "--!x"}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(lexedTokens(test.input, test.options), qt.DeepEquals, test.want)
		})
	}
}
