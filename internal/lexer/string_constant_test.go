package lexer_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
)

// significantTokens lists the tokens of input that are neither whitespace nor
// a comment, each as its type and its text.
func significantTokens(input string, options lexer.Options) []string {
	var tokens []string
	l := lexer.NewLexerWithOptions(input, options)
	for token := l.NextToken(); token.Type != lexer.TokenEOF; token = l.NextToken() {
		if token.Type != lexer.TokenWhitespace && token.Type != lexer.TokenComment {
			tokens = append(tokens, token.Type.String()+" "+token.Value)
		}
	}
	return tokens
}

// Each input is one string constant to PostgreSQL 18.6, which applies it as
// one: the continued forms join, a UESCAPE clause belongs to the string before
// it (stokaro/ptah#3731). One token each keeps a reader from meeting half of
// a constant.
func TestLexer_PostgreSQLStringConstantIsOneToken(t *testing.T) {
	postgres := dialectlexer.Options("postgres")
	tests := []struct {
		name  string
		input string
	}{
		{name: "continued on the next line", input: "'first part, '\n'second part'"},
		{name: "continued after a line comment", input: "'a'  -- note\n  'b'"},
		{name: "continued past comment lines", input: "'a'\n-- c1\n-- c2\n'b'"},
		{name: "continued over blank lines", input: "'a'\n\n   'b'\n'c'"},
		{name: "continued over tabs around the newline", input: "'a'\t\n\t'b'"},
		{name: "continued over a vertical tab and a newline", input: "'a'\v\n'b'"},
		{name: "continued over CRLF", input: "'a'\r\n'b'"},
		{name: "continued over a bare carriage return", input: "'a'\r'b'"},
		{name: "a continued escape string", input: "E'a\\''\n'b\\t'"},
		{name: "a Unicode escape string", input: `U&'d\0061t\+000061'`},
		{name: "a Unicode escape string in lower case", input: `u&'\0041'`},
		{name: "a continued Unicode escape string", input: "U&'d\\0061'\n'\\0074a'"},
		{name: "a Unicode escape clause", input: `U&'d!0061' UESCAPE '!'`},
		{name: "a Unicode escape clause across comments", input: "U&'d!0061' /* c */ uescape -- x\n '!'"},
		{name: "a continued Unicode escape string and its clause", input: "U&'d!0061'\n'!0062' UESCAPE '!'"},
		{name: "a Unicode escape clause with an escape string", input: `U&'d!0061' UESCAPE E'!'`},
		{name: "a Unicode escape clause with a dollar quote", input: `U&'d!0061' UESCAPE $$!$$`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(significantTokens(test.input, postgres), qt.DeepEquals, []string{"String " + test.input})
		})
	}
}

// Each input is two tokens to PostgreSQL 18.6, which refuses the second as a
// syntax error or reads it as a token of its own, so each stays two here.
func TestLexer_PostgreSQLStringConstantsStayApart(t *testing.T) {
	postgres := dialectlexer.Options("postgres")
	tests := []struct {
		name  string
		input string
		want  []string
	}{
		{name: "two strings on one line", input: "'a' 'b'", want: []string{"String 'a'", "String 'b'"}},
		{name: "a form feed is not a newline", input: "'a'\f'b'", want: []string{"String 'a'", "String 'b'"}},
		{name: "a block comment in the gap", input: "'a' /* x */\n'b'", want: []string{"String 'a'", "String 'b'"}},
		{name: "a block comment after the newline", input: "'a'\n/* x */ 'b'", want: []string{"String 'a'", "String 'b'"}},
		{name: "a dollar quote is not continued", input: "$$a$$\n'b'", want: []string{"String $$a$$", "String 'b'"}},
		{name: "a dollar quote does not continue", input: "'a'\n$$b$$", want: []string{"String 'a'", "String $$b$$"}},
		{name: "an escape string does not continue", input: "'a'\nE'b'", want: []string{"String 'a'", "String E'b'"}},
		{name: "identifiers do not continue", input: "\"a\"\n\"b\"", want: []string{`String "a"`, `String "b"`}},
		{name: "a space after U&", input: `U& '\0041'`, want: []string{"Identifier U", "Operator &", `String '\0041'`}},
		{
			name:  "an alias after a Unicode escape string",
			input: `U&'x' uescape FROM t`,
			want:  []string{`String U&'x'`, "Identifier uescape", "Identifier FROM", "Identifier t"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(significantTokens(test.input, postgres), qt.DeepEquals, test.want)
		})
	}
}

// CockroachDB 26.3 refuses a -- comment between continued strings and has no
// U&'...' strings, so its lexer reads neither PostgreSQL form: a file that
// uses one is refused rather than read by a rule the server does not share.
func TestLexer_CockroachDBKeepsPostgreSQLStringConstantsApart(t *testing.T) {
	cockroach := dialectlexer.Options("cockroachdb")
	tests := []struct {
		name  string
		input string
		want  []string
	}{
		{name: "a continued string", input: "'a'\n'b'", want: []string{"String 'a'", "String 'b'"}},
		{name: "a Unicode escape string", input: `U&'\0041'`, want: []string{"Identifier U", "Operator &", `String '\0041'`}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(significantTokens(test.input, cockroach), qt.DeepEquals, test.want)
		})
	}
}
