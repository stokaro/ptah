package lexer_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/lexer"
)

// commentsAndWords lists the comments and identifiers of text, the rest left out.
func commentsAndWords(text string, options lexer.Options) []string {
	scan := lexer.NewLexerWithOptions(text, options)
	var found []string
	for token := scan.NextToken(); token.Type != lexer.TokenEOF; token = scan.NextToken() {
		if token.Type == lexer.TokenComment || token.Type == lexer.TokenIdentifier {
			found = append(found, token.Value)
		}
	}
	return found
}

// Where the options ask for nested comments, a comment that holds another is
// one comment, and the text after its own */ is text again. Without them it
// ends at the first */, as the SQL standard has it.
func TestLexer_NestedBlockComments(t *testing.T) {
	for _, test := range []struct {
		name    string
		options lexer.Options
		want    []string
	}{
		{name: "nested", options: lexer.Options{StandardStrings: true, NestedBlockComments: true},
			want: []string{"/* a /* b */ c */", "x"}},
		{name: "standard", options: lexer.Options{StandardStrings: true},
			want: []string{"/* a /* b */", "c", "x"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			found := commentsAndWords("/* a /* b */ c */ x", test.options)

			c.Assert(found, qt.DeepEquals, test.want)
		})
	}
}

// A nested comment that does not close runs to the end of the text, as an
// unnested one does.
func TestLexer_UnclosedNestedBlockComment(t *testing.T) {
	c := qt.New(t)

	found := commentsAndWords("x /* a /* b */ c", lexer.Options{StandardStrings: true, NestedBlockComments: true})

	c.Assert(found, qt.DeepEquals, []string{"x", "/* a /* b */ c"})
}
