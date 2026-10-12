package lexer_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/lexer"
)

// rangeToken is what a test compares of a token: its type, its text and where
// it stands in the input.
type rangeToken struct {
	Type  lexer.TokenType
	Value string
	Start int
	End   int
}

// TestLexer_Range_ReadsTheTextInsideAToken establishes that a range reads
// only its own text and reports each token's position in the whole input,
// which is what lets a reader scan the body of an executable comment and
// still name document positions.
func TestLexer_Range_ReadsTheTextInsideAToken(t *testing.T) {
	c := qt.New(t)
	input := "KEY k (a) /*!80000 INVISIBLE x*/ , b"
	outer := lexer.NewLexerWithOptions(input, lexer.Options{ExecutableComments: lexer.ExecutableCommentsMySQL})

	scan := outer.Range(19, 30)
	var got []rangeToken
	for token := scan.NextToken(); token.Type != lexer.TokenEOF; token = scan.NextToken() {
		got = append(got, rangeToken{Type: token.Type, Value: token.Value, Start: token.Start, End: token.End})
	}

	c.Assert(got, qt.DeepEquals, []rangeToken{
		{Type: lexer.TokenIdentifier, Value: "INVISIBLE", Start: 19, End: 28},
		{Type: lexer.TokenWhitespace, Value: " ", Start: 28, End: 29},
		{Type: lexer.TokenIdentifier, Value: "x", Start: 29, End: 30},
	})
}

// TestLexer_Range_KeepsTheOptions establishes that a range scans with the
// options of the lexer it came from: a MySQL range reads a version guard as
// one token, where the default options read a comment.
func TestLexer_Range_KeepsTheOptions(t *testing.T) {
	c := qt.New(t)
	input := "a /*!50100 b */ c"
	outer := lexer.NewLexerWithOptions(input, lexer.Options{ExecutableComments: lexer.ExecutableCommentsMySQL})

	token := outer.Range(2, 15).NextToken()

	c.Assert(token.Type, qt.Equals, lexer.TokenUnknown)
	c.Assert(token.Value, qt.Equals, "/*!50100 b */")
}

// TestLexer_Range_IsNotTheHeadOfTheInput establishes that a range is read as
// text after the head of its input: a YQL translation setting there is an
// ordinary comment, where the same text at the head of the input is one.
func TestLexer_Range_IsNotTheHeadOfTheInput(t *testing.T) {
	c := qt.New(t)
	input := "--!syntax_v1\nSELECT 1"
	outer := lexer.NewLexerWithOptions(input, lexer.Options{YQL: true})

	head := lexer.NewLexerWithOptions(input, lexer.Options{YQL: true}).NextToken()
	inRange := outer.Range(0, len(input)).NextToken()

	c.Assert(head.Type, qt.Equals, lexer.TokenUnknown)
	c.Assert(inRange.Type, qt.Equals, lexer.TokenComment)
	c.Assert(inRange.Value, qt.Equals, "--!syntax_v1")
}
