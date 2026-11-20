package dialectlexer_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
)

// typedTokens returns each token of text but white space, read with the lexer
// options Options answers for dialect.
func typedTokens(dialect, text string) []lexer.Token {
	lex := lexer.NewLexerWithOptions(text, dialectlexer.Options(dialect))
	var tokens []lexer.Token
	for token := lex.NextToken(); token.Type != lexer.TokenEOF; token = lex.NextToken() {
		if token.Type != lexer.TokenWhitespace {
			tokens = append(tokens, token)
		}
	}
	return tokens
}

// tokenValues returns the text of each token.
func tokenValues(tokens []lexer.Token) []string {
	var values []string
	for _, token := range tokens {
		values = append(values, token.Value)
	}
	return values
}

// commentValues returns the text of each comment token.
func commentValues(tokens []lexer.Token) []string {
	var values []string
	for _, token := range tokens {
		if token.Type == lexer.TokenComment {
			values = append(values, token.Value)
		}
	}
	return values
}

// The markers a descriptor lists open a comment wherever they stand, with no
// exception for the character that follows: MySQL and MariaDB read `#=` and
// `#>` as comments, and ClickHouse reads # and // alike.
func TestOptions_ListedLineCommentMarkers_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		text    string
		want    []string
	}{
		{"mysql hash then space", platform.MySQL, "SELECT 1 # note\nSELECT 2", []string{"SELECT", "1", "# note", "SELECT", "2"}},
		{"mysql hash then equals", platform.MySQL, "#=== don't ===\nSELECT 2", []string{"#=== don't ===", "SELECT", "2"}},
		{"mysql hash then greater-than", platform.MySQL, "SELECT 1 #> 2", []string{"SELECT", "1", "#> 2"}},
		{"mysql hash then hash", platform.MySQL, "SELECT 1 ## x", []string{"SELECT", "1", "## x"}},
		{"mariadb hash then equals", platform.MariaDB, "#=== don't ===\nSELECT 2", []string{"#=== don't ===", "SELECT", "2"}},
		{"mariadb hash then greater-than", platform.MariaDB, "SELECT 1 #> 2", []string{"SELECT", "1", "#> 2"}},
		{"clickhouse hash then equals", platform.ClickHouse, "SELECT 1 #= x\nSELECT 2", []string{"SELECT", "1", "#= x", "SELECT", "2"}},
		{"clickhouse slashes", platform.ClickHouse, "SELECT 1 // it's\nSELECT 2", []string{"SELECT", "1", "// it's", "SELECT", "2"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			tokens := typedTokens(test.dialect, test.text)

			c.Assert(tokenValues(tokens), qt.DeepEquals, test.want)
			c.Assert(commentValues(tokens), qt.HasLen, 1)
		})
	}
}

// A dialect that lists no marker reads # as an operator character, whatever
// follows it, so a later apostrophe opens a literal that closes as written.
func TestOptions_UnlistedHash_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		text    string
		want    []string
	}{
		{"postgres xor", platform.Postgres, "SELECT 5 # 3 AS x, 'it''s'", []string{"SELECT", "5", "#", "3", "AS", "x", ",", "'it''s'"}},
		{"postgres json path", platform.Postgres, `SELECT '{"a":1}'::jsonb #> '{a}'`, []string{"SELECT", `'{"a":1}'`, ":", ":", "jsonb", "#", ">", "'{a}'"}},
		{"postgres hash then letter", platform.Postgres, "SELECT 5 #x 'a'", []string{"SELECT", "5", "#", "x", "'a'"}},
		{"cockroachdb xor", platform.CockroachDB, "SELECT 5 # 3, 'it''s'", []string{"SELECT", "5", "#", "3", ",", "'it''s'"}},
		{"yugabytedb xor", platform.YugabyteDB, "SELECT 5 # 3, 'it''s'", []string{"SELECT", "5", "#", "3", ",", "'it''s'"}},
		{"sqlite hash", platform.SQLite, "SELECT 5 # 3, 'it''s'", []string{"SELECT", "5", "#", "3", ",", "'it''s'"}},
		{"oracle hash", platform.Oracle, "SELECT 5 # 3, 'it''s' FROM dual", []string{"SELECT", "5", "#", "3", ",", "'it''s'", "FROM", "dual"}},
		{"sqlserver temp name", platform.SQLServer, "SELECT * FROM #temp WHERE a = 'x'", []string{"SELECT", "*", "FROM", "#temp", "WHERE", "a", "=", "'x'"}},
		{"sqlserver global temp name", platform.SQLServer, "SELECT * FROM ##temp", []string{"SELECT", "*", "FROM", "##temp"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			var texts []string
			for _, token := range typedTokens(test.dialect, test.text) {
				c.Assert(token.Type, qt.Not(qt.Equals), lexer.TokenComment)
				texts = append(texts, token.Value)
			}

			c.Assert(texts, qt.DeepEquals, test.want)
		})
	}
}

// With no dialect the markers are not known, and a # reads as a comment unless
// an operator character follows it.
func TestOptions_NoDialectKeepsTheHashHeuristic_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		text string
		want []lexer.TokenType
	}{
		{"hash then space", "a # b", []lexer.TokenType{lexer.TokenIdentifier, lexer.TokenComment}},
		{"hash then operator", "a #> b", []lexer.TokenType{lexer.TokenIdentifier, lexer.TokenOperator, lexer.TokenOperator, lexer.TokenIdentifier}},
	}
	for _, dialect := range []string{"", "no-such-dialect"} {
		for _, test := range tests {
			t.Run(dialect+" "+test.name, func(t *testing.T) {
				c := qt.New(t)

				var got []lexer.TokenType
				for _, token := range typedTokens(dialect, test.text) {
					got = append(got, token.Type)
				}

				c.Assert(got, qt.DeepEquals, test.want)
			})
		}
	}
}

// An engine an executable provider declares gets the rule its declaration
// states: a "#" it lists opens a comment, and one it leaves out is an operator.
func TestDeclareLexical_LineComments_HappyPath(t *testing.T) {
	c := qt.New(t)
	releaseListed, err := dialectlexer.DeclareLexical(descriptor.Lexical{LineComments: []string{"#"}}, "hash-engine")
	c.Assert(err, qt.IsNil)
	c.Cleanup(releaseListed)
	releaseUnlisted, err := dialectlexer.DeclareLexical(descriptor.Lexical{}, "plain-engine")
	c.Assert(err, qt.IsNil)
	c.Cleanup(releaseUnlisted)
	text := "a #> don't\nb"

	c.Assert(tokenTexts("hash-engine", text), qt.DeepEquals, []string{"a", "#> don't", "b"})
	c.Assert(tokenTexts("plain-engine", text), qt.DeepEquals, []string{"a", "#", ">", "don", "'t\nb"})
}

// ClientDelimiterRules carries the declared markers, and says they are the
// whole list, to the delimiter rewrite.
func TestClientDelimiterRules_LineComments_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		want    []string
	}{
		{"mysql", platform.MySQL, []string{"#"}},
		{"clickhouse", platform.ClickHouse, []string{"#", "//"}},
		{"postgres", platform.Postgres, nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			rules, ok := dialectlexer.ClientDelimiterRules(test.dialect, capability.Capabilities{})

			c.Assert(ok, qt.IsTrue)
			c.Assert(rules.LineCommentsDeclared, qt.IsTrue)
			c.Assert(rules.LineComments, qt.DeepEquals, test.want)
		})
	}
}
