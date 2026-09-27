package pgname_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/pgname"
)

// TestTokens_HappyPath reads text as PostgreSQL's lexer does: whitespace and
// comments go, an unquoted word is folded, and every other token is kept as
// written.
func TestTokens_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		text string
		want []string
	}{
		{name: "an element", text: "r WITH =", want: []string{"r", "with", "="}},
		{name: "spacing and a comment", text: " R /* x */  WITH\n= ", want: []string{"r", "with", "="}},
		{name: "a folded name beside a quoted one", text: `Lo = "Hi"`, want: []string{"lo", "=", `"Hi"`}},
		{name: "a string and a cast", text: "t <> 'X'::TEXT", want: []string{"t", "<>", "'X'", "::", "text"}},
		{name: "operator characters apart", text: "t < > 'X'", want: []string{"t", "<", ">", "'X'"}},
		{name: "operator characters across a comment", text: "a ~/**/~ b", want: []string{"a", "~", "~", "b"}},
		{name: "parentheses stay apart", text: "((A))", want: []string{"(", "(", "a", ")", ")"}},
		{name: "a parenthesis after an operator", text: "a =(1)", want: []string{"a", "=", "(", "1", ")"}},
		{name: "nothing", text: "", want: make([]string, 0)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(pgname.Tokens(test.text), qt.DeepEquals, test.want)
		})
	}
}
