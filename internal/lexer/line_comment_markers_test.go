package lexer_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/lexer"
)

// Where the markers are declared, each one listed opens a comment to the end
// of the line wherever it stands, and no text beside it changes that.
func TestOptions_LineComments_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		markers []string
		want    []sharedToken
	}{
		{
			name:    "hash before an operator character",
			input:   "a #> b\nc",
			markers: []string{"#"},
			want: []sharedToken{
				{lexer.TokenIdentifier, "a"}, {lexer.TokenWhitespace, " "}, {lexer.TokenComment, "#> b"},
				{lexer.TokenWhitespace, "\n"}, {lexer.TokenIdentifier, "c"},
			},
		},
		{
			name:    "a second marker",
			input:   "a // b\nc",
			markers: []string{"#", "//"},
			want: []sharedToken{
				{lexer.TokenIdentifier, "a"}, {lexer.TokenWhitespace, " "}, {lexer.TokenComment, "// b"},
				{lexer.TokenWhitespace, "\n"}, {lexer.TokenIdentifier, "c"},
			},
		},
		{
			name:    "a comment that ends the input",
			input:   "a #",
			markers: []string{"#"},
			want:    []sharedToken{{lexer.TokenIdentifier, "a"}, {lexer.TokenWhitespace, " "}, {lexer.TokenComment, "#"}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := lexedTokens(test.input, lexer.Options{LineCommentsDeclared: true, LineComments: test.markers})

			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// Where the markers are declared and the list leaves a marker out, the text is
// not a comment: # is an operator character and // is two of them.
func TestOptions_LineCommentsLeftOut_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		markers []string
		want    []sharedToken
	}{
		{
			name:  "no marker at all",
			input: "a # b",
			want: []sharedToken{
				{lexer.TokenIdentifier, "a"}, {lexer.TokenWhitespace, " "}, {lexer.TokenOperator, "#"},
				{lexer.TokenWhitespace, " "}, {lexer.TokenIdentifier, "b"},
			},
		},
		{
			name:    "slashes beside a hash marker",
			input:   "a // b",
			markers: []string{"#"},
			want: []sharedToken{
				{lexer.TokenIdentifier, "a"}, {lexer.TokenWhitespace, " "}, {lexer.TokenOperator, "/"},
				{lexer.TokenOperator, "/"}, {lexer.TokenWhitespace, " "}, {lexer.TokenIdentifier, "b"},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := lexedTokens(test.input, lexer.Options{LineCommentsDeclared: true, LineComments: test.markers})

			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// With no declaration the markers are unknown: a # starts a comment unless an
// operator character follows it, and // is never a comment.
func TestOptions_LineCommentsUndeclared_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  lexer.TokenType
	}{
		{"hash then space", "#a b", lexer.TokenComment},
		{"hash then operator character", "#>a b", lexer.TokenOperator},
		{"slashes", "//a b", lexer.TokenOperator},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := lexedTokens(test.input, lexer.Options{})

			c.Assert(got[0].Type, qt.Equals, test.want)
		})
	}
}
