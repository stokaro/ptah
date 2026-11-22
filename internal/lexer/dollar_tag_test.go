package lexer_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/lexer"
)

// TestDollarTagRule_OpeningTag_HappyPath reads the length of each tag a rule
// allows, both dollar signs included. A character outside ASCII is two bytes
// here, so `$é$` is four.
func TestDollarTagRule_OpeningTag_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name string
		rule lexer.DollarTagRule
		text string
		want int
	}{
		{name: "postgres empty tag", rule: lexer.PostgreSQLDollarTags, text: "$$ x $$", want: 2},
		{name: "postgres digit after the first", rule: lexer.PostgreSQLDollarTags, text: "$a1$x$a1$", want: 4},
		{name: "postgres underscore first", rule: lexer.PostgreSQLDollarTags, text: "$_1$", want: 4},
		{name: "postgres character outside ASCII", rule: lexer.PostgreSQLDollarTags, text: "$é$x$é$", want: 4},
		{name: "heredoc digit first", rule: lexer.HeredocDollarTags, text: "$1abc$x$1abc$", want: 6},
		{name: "heredoc digit alone", rule: lexer.HeredocDollarTags, text: "$1$", want: 3},
		{name: "heredoc empty tag", rule: lexer.HeredocDollarTags, text: "$$", want: 2},
		{name: "heredoc character outside ASCII", rule: lexer.HeredocDollarTags, text: "$é$x$é$", want: 4},
		{name: "heredoc character outside ASCII after a letter", rule: lexer.HeredocDollarTags, text: "$aé$", want: 5},
		{name: "heredoc tag never closed", rule: lexer.HeredocDollarTags, text: "$ab$ x", want: 4},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, ok := test.rule.OpeningTag(test.text)

			c.Assert(ok, qt.IsTrue)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// TestDollarTagRule_OpeningTag_FailurePath reads text where no tag the rule
// allows opens.
func TestDollarTagRule_OpeningTag_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name string
		rule lexer.DollarTagRule
		text string
	}{
		{name: "postgres digit first", rule: lexer.PostgreSQLDollarTags, text: "$1abc$"},
		{name: "postgres parameter", rule: lexer.PostgreSQLDollarTags, text: "$1$"},
		{name: "postgres hyphen", rule: lexer.PostgreSQLDollarTags, text: "$a-b$"},
		{name: "postgres no closing sign", rule: lexer.PostgreSQLDollarTags, text: "$ab"},
		{name: "heredoc hyphen", rule: lexer.HeredocDollarTags, text: "$a-b$"},
		{name: "heredoc space", rule: lexer.HeredocDollarTags, text: "$a b$"},
		{name: "no dollar sign first", rule: lexer.PostgreSQLDollarTags, text: "a$b$"},
		{name: "empty text", rule: lexer.HeredocDollarTags, text: ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, ok := test.rule.OpeningTag(test.text)

			c.Assert(ok, qt.IsFalse)
			c.Assert(got, qt.Equals, 0)
		})
	}
}
