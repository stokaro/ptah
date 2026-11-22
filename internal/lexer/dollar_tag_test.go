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

// TestDollarQuotes_End_HappyPath reads where a dollar-quoted string that opens
// at an offset ends. A PostgreSQL string nothing closes runs to the end of the
// text, as the server reads it before refusing it, and a heredoc closes at the
// next start of its tag, inside a comment too, as ClickHouse searches for it.
func TestDollarQuotes_End_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name string
		rule lexer.DollarTagRule
		text string
		pos  int
		want int
	}{
		{name: "postgres closed", rule: lexer.PostgreSQLDollarTags, text: "SELECT $a$x;y$a$ z", pos: 7, want: 16},
		{name: "postgres empty tag", rule: lexer.PostgreSQLDollarTags, text: "$$$$;", pos: 0, want: 4},
		{name: "postgres never closed", rule: lexer.PostgreSQLDollarTags, text: "SELECT $ab$ x", pos: 7, want: 13},
		{name: "heredoc digit first", rule: lexer.HeredocDollarTags, text: "$1abc$x$1abc$;", pos: 0, want: 13},
		{name: "heredoc closed inside a comment", rule: lexer.HeredocDollarTags, text: "$a$ -- $a$\n", pos: 0, want: 10},
		{name: "heredoc after another", rule: lexer.HeredocDollarTags, text: "$a$x$a$ $a$y$a$", pos: 8, want: 15},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, ok := lexer.NewDollarQuotes(test.rule, test.text).End(test.pos)

			c.Assert(ok, qt.IsTrue)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// TestDollarQuotes_End_FailurePath is an offset where no dollar-quoted string
// opens: a tag the rule does not allow, and a heredoc tag that does not start
// again after its own end, so ClickHouse reads it as part of a name.
func TestDollarQuotes_End_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name string
		rule lexer.DollarTagRule
		text string
		pos  int
	}{
		{name: "postgres digit first", rule: lexer.PostgreSQLDollarTags, text: "$1abc$x$1abc$", pos: 0},
		{name: "heredoc never closed", rule: lexer.HeredocDollarTags, text: "SELECT 1 AS $ab$; SELECT 2", pos: 12},
		{name: "heredoc repeated only inside itself", rule: lexer.HeredocDollarTags, text: "$ab$ab$", pos: 0},
		{name: "heredoc closed only before", rule: lexer.HeredocDollarTags, text: "$a$x$a$ $a$y", pos: 8},
		{name: "no dollar sign", rule: lexer.PostgreSQLDollarTags, text: "abc", pos: 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, ok := lexer.NewDollarQuotes(test.rule, test.text).End(test.pos)

			c.Assert(ok, qt.IsFalse)
			c.Assert(got, qt.Equals, 0)
		})
	}
}

// TestDollarQuotes_Closes reads whether a tag starts at or after an offset, the
// offset itself included.
func TestDollarQuotes_Closes(t *testing.T) {
	for _, test := range []struct {
		name string
		tag  string
		from int
		want bool
	}{
		{name: "a start at the offset", tag: "$a$", from: 4, want: true},
		{name: "a start before the offset", tag: "$a$", from: 5, want: false},
		{name: "a tag the text does not hold", tag: "$b$", from: 0, want: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(lexer.NewDollarQuotes(lexer.PostgreSQLDollarTags, "$a$x$a$").Closes(test.tag, test.from), qt.Equals, test.want)
		})
	}
}

// TestNameEnd reads where a name ends: after its letters, digits,
// underscores, dollar signs and bytes above 0x7F, so `a$b$` and `a1$b$` are
// names whole.
func TestNameEnd(t *testing.T) {
	for _, test := range []struct {
		name string
		text string
		pos  int
		want int
	}{
		{name: "a dollar sign inside a name", text: "a$b$, c", pos: 0, want: 4},
		{name: "a digit before a dollar sign", text: "a1$b$ c", pos: 0, want: 5},
		{name: "an underscore first", text: "_1$b$ c", pos: 0, want: 5},
		{name: "a character outside ASCII", text: "\u00e9$b$;", pos: 0, want: 5},
		{name: "two dollar signs", text: "a$$b (", pos: 0, want: 4},
		{name: "to the end of the text", text: "x$b$", pos: 0, want: 4},
		{name: "the bytes after a dollar sign", text: "$1$b$x$b$;", pos: 1, want: 9},
		{name: "nothing continues a name", text: "($a$", pos: 0, want: 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(lexer.NameEnd(test.text, test.pos), qt.Equals, test.want)
		})
	}
}

// TestNameStart reads which bytes begin a name. A digit begins a number and a
// dollar sign may open a string, so neither begins one.
func TestNameStart(t *testing.T) {
	for _, test := range []struct {
		name string
		b    byte
		want bool
	}{
		{name: "a lowercase letter", b: 'a', want: true},
		{name: "an uppercase letter", b: 'Z', want: true},
		{name: "an underscore", b: '_', want: true},
		{name: "a byte above 0x7F", b: 0xc3, want: true},
		{name: "a digit", b: '1', want: false},
		{name: "a dollar sign", b: '$', want: false},
		{name: "a parenthesis", b: '(', want: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(lexer.NameStart(test.b), qt.Equals, test.want)
		})
	}
}
