package lexer_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/lexer"
)

// meaningful lists the tokens of text, as "Type:text", whitespace left out.
func meaningful(text string, options lexer.Options) []string {
	scan := lexer.NewLexerWithOptions(text, options)
	var found []string
	for token := scan.NextToken(); token.Type != lexer.TokenEOF; token = scan.NextToken() {
		if token.Type != lexer.TokenWhitespace {
			found = append(found, token.Type.String()+":"+token.Value)
		}
	}
	return found
}

// Each option the MySQL family sets, measured on MySQL 8.4.13 and MariaDB
// 11.8.9, read with the option and without it: base is the options the text
// needs either way, and without the option each text reads as every other
// dialect reads it.
func TestLexer_MySQLFamilyOptions(t *testing.T) {
	tests := []struct {
		name    string
		base    lexer.Options
		options lexer.Options
		text    string
		with    []string
		without []string
	}{
		{
			name:    "a carriage return inside a hash comment",
			options: lexer.Options{LineCommentsEndAtLineFeed: true},
			text:    "a #\rb\nc",
			with:    []string{"Identifier:a", "Comment:#\rb", "Identifier:c"},
			without: []string{"Identifier:a", "Comment:#", "Identifier:b", "Identifier:c"},
		},
		{
			name:    "a carriage return inside a dash comment",
			options: lexer.Options{LineCommentsEndAtLineFeed: true},
			text:    "a -- \rb\nc",
			with:    []string{"Identifier:a", "Comment:-- \rb", "Identifier:c"},
			without: []string{"Identifier:a", "Comment:-- ", "Identifier:b", "Identifier:c"},
		},
		{
			name:    "a backslash before a backtick",
			options: lexer.Options{LiteralBacktickBackslashes: true},
			text:    "`a\\` b `c`",
			with:    []string{"Identifier:`a\\`", "Identifier:b", "Identifier:`c`"},
			without: []string{"Identifier:`a\\` b `", "Identifier:c", "Identifier:`"},
		},
		{
			name:    "dollar signs",
			options: lexer.Options{DollarNames: true},
			text:    "a $$ b $$ c$d $1",
			with:    []string{"Identifier:a", "Identifier:$$", "Identifier:b", "Identifier:$$", "Identifier:c$d", "Identifier:$1"},
			without: []string{"Identifier:a", "String:$$ b $$", "Identifier:c$d", "Identifier:$1"},
		},
		{
			name:    "a name that begins with digits",
			options: lexer.Options{DigitLeadingNames: true},
			text:    "1other.t 1e5x 1e5 0x1F 0X1F",
			with:    []string{"Identifier:1other", "Operator:.", "Identifier:t", "Identifier:1e5", "Identifier:x", "Identifier:1e5", "Identifier:0", "Identifier:x1F", "Identifier:0X1F"},
			without: []string{"Identifier:1", "Identifier:other", "Operator:.", "Identifier:t", "Identifier:1", "Identifier:e5x", "Identifier:1", "Identifier:e5", "Identifier:0", "Identifier:x1F", "Identifier:0", "Identifier:X1F"},
		},
		{
			name:    "a double-quoted name",
			options: lexer.Options{DoubleQuotedNames: true},
			text:    `"a\" 'b'`,
			with:    []string{`Identifier:"a\"`, "String:'b'"},
			without: []string{`String:"a\" 'b'`},
		},
		{
			name:    "a dash before a no-break space",
			base:    lexer.Options{RequireWhitespaceAfterDashDash: true},
			options: lexer.Options{RequireWhitespaceAfterDashDash: true, NonASCIINames: true},
			text:    "1 -- a\n2",
			with:    []string{"Identifier:1", "Operator:-", "Operator:-", "Identifier: a", "Identifier:2"},
			without: []string{"Identifier:1", "Comment:-- a", "Identifier:2"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(meaningful(test.text, test.options), qt.DeepEquals, test.with)
			c.Assert(meaningful(test.text, test.base), qt.DeepEquals, test.without)
		})
	}
}
