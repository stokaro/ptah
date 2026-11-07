package dialectlexer_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
)

// postgresRules are the lexical rules a PostgreSQL-family provider engine
// states in its query dialect.
var postgresRules = descriptor.Lexical{EscapeStrings: true, ContinuedStrings: true, UnicodeStrings: true, NestedComments: true}

// tokenTexts returns the text of each token of text but white space, read
// with the lexer options Options answers for dialect.
func tokenTexts(dialect, text string) []string {
	lex := lexer.NewLexerWithOptions(text, dialectlexer.Options(dialect))
	var texts []string
	for token := lex.NextToken(); token.Type != lexer.TokenEOF; token = lex.NextToken() {
		if token.Type != lexer.TokenWhitespace {
			texts = append(texts, token.Value)
		}
	}
	return texts
}

// An engine whose provider declared PostgreSQL's rules is read by them under
// each of its names: an escape string and a nested comment are one token
// each. Once the registration ends, the engine is read by the SQL standard
// again, which splits both.
func TestDeclareLexical_HappyPath(t *testing.T) {
	c := qt.New(t)
	text := `x E'it\'s' /* a /* b */ c */ y`
	release, err := dialectlexer.DeclareLexical(postgresRules, "lexical-engine", "lexical-alias")
	c.Assert(err, qt.IsNil)

	c.Assert(dialectlexer.Options("Lexical-Alias"), qt.DeepEquals, dialectlexer.FromDescriptor(descriptor.Dialect{Lexical: postgresRules}))
	c.Assert(tokenTexts("lexical-engine", text), qt.DeepEquals, []string{"x", `E'it\'s'`, "/* a /* b */ c */", "y"})
	release()
	release()
	c.Assert(dialectlexer.Options("lexical-engine"), qt.DeepEquals, lexer.Options{StandardStrings: true})
	c.Assert(dialectlexer.Options("lexical-alias"), qt.DeepEquals, lexer.Options{StandardStrings: true})
}

// A second registration of the same rules for a name the first holds, as a
// second host of one process makes for the engine both serve, keeps the
// first: the second's release leaves it, and the first's ends it.
func TestDeclareLexical_SameRulesTwice(t *testing.T) {
	c := qt.New(t)
	first, err := dialectlexer.DeclareLexical(postgresRules, "twice-engine")
	c.Assert(err, qt.IsNil)
	second, err := dialectlexer.DeclareLexical(postgresRules, "twice-engine")
	c.Assert(err, qt.IsNil)

	second()
	c.Assert(dialectlexer.Options("twice-engine").NestedBlockComments, qt.IsTrue)
	first()
	c.Assert(dialectlexer.Options("twice-engine"), qt.DeepEquals, lexer.Options{StandardStrings: true})
}

func TestDeclareLexical_FailurePath(t *testing.T) {
	held, err := dialectlexer.DeclareLexical(postgresRules, "held-engine")
	qt.New(t).Assert(err, qt.IsNil)
	t.Cleanup(held)
	for _, test := range []struct {
		name    string
		lexical descriptor.Lexical
		names   []string
		want    string
	}{
		{name: "a name the registry describes", lexical: postgresRules, names: []string{platform.Postgres},
			want: `dialectlexer: postgres is a dialect Ptah describes, whose lexical rules its descriptor states`},
		{name: "an empty name", lexical: postgresRules, names: []string{" "}, want: `dialectlexer: declared lexical rules need an engine name`},
		{name: "an unknown executable comment style", lexical: descriptor.Lexical{ExecutableComments: "mbang"}, names: []string{"style-engine"},
			want: `dialectlexer: executable comment style "mbang" is not one of \["bang" "m-bang"\]`},
		{name: "a lexer of the engine's own", lexical: descriptor.Lexical{Lexer: descriptor.LexerYQL}, names: []string{"yql-engine"},
			want: `dialectlexer: declared rules name the lexer "yql"; a lexer of an engine's own is a scanner its provider serves`},
		{name: "other rules for a held name", lexical: descriptor.Lexical{NestedComments: true}, names: []string{"fresh-engine", "held-engine"},
			want: `dialectlexer: held-engine is registered with other lexical rules`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			release, err := dialectlexer.DeclareLexical(test.lexical, test.names...)

			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(release, qt.IsNil)
		})
	}
	// The refused declaration released the name it took before the refusal.
	t.Run("a refusal keeps no name", func(t *testing.T) {
		c := qt.New(t)
		c.Assert(dialectlexer.Options("fresh-engine"), qt.DeepEquals, lexer.Options{StandardStrings: true})
		c.Assert(dialectlexer.Options("held-engine").PostgreSQLEscapeStrings, qt.IsTrue)
	})
}
