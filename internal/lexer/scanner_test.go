package lexer_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/lexer"
)

// scanCall is what one call of a scanner's Scan was given.
type scanCall struct {
	Text  string
	Start int
	Head  bool
}

// wholeScanner is a lexer of an invented engine's own: it reads everything
// from the start as one operator token, which the shared rules never do, and
// answers every string and name with a marked copy. It records each Scan.
type wholeScanner struct {
	calls []scanCall
}

func (s *wholeScanner) Scan(text string, start int, head bool) lexer.TokenReader {
	s.calls = append(s.calls, scanCall{Text: text, Start: start, Head: head})
	return &queue{tokens: []lexer.Token{
		{Type: lexer.TokenOperator, Value: text[start:], Start: start, End: len(text)},
		{Type: lexer.TokenEOF, Start: len(text), End: len(text)},
	}}
}

func (*wholeScanner) StringValue(token string) (string, bool) {
	return "string " + token, true
}

func (*wholeScanner) IdentifierValue(token string) (string, bool) {
	return "name " + token, true
}

// queue returns its tokens in order and then its last one again.
type queue struct {
	tokens []lexer.Token
}

func (q *queue) NextToken() lexer.Token {
	token := q.tokens[0]
	if len(q.tokens) > 1 {
		q.tokens = q.tokens[1:]
	}
	return token
}

// A lexer built with a scanner returns the scanner's tokens for the whole
// text, read from the head, and none of the shared rules' tokens.
func TestNewLexerWithOptions_ReadsWithTheScanner(t *testing.T) {
	c := qt.New(t)
	scanner := &wholeScanner{}
	input := "SELECT 'a;b'; SELECT 2"

	l := lexer.NewLexerWithOptions(input, lexer.Options{Scanner: scanner})

	c.Assert(l.NextToken(), qt.Equals, lexer.Token{Type: lexer.TokenOperator, Value: input, Start: 0, End: len(input)})
	c.Assert(l.NextToken(), qt.Equals, lexer.Token{Type: lexer.TokenEOF, Start: len(input), End: len(input)})
	c.Assert(scanner.calls, qt.DeepEquals, []scanCall{{Text: input, Start: 0, Head: true}})
}

// A range of a lexer built with a scanner asks the scanner for the text up to
// the range's end, from its start, and not at the head of the text.
func TestLexer_Range_ReadsWithTheScanner(t *testing.T) {
	c := qt.New(t)
	scanner := &wholeScanner{}
	input := "SELECT 'a;b'; SELECT 2"
	outer := lexer.NewLexerWithOptions(input, lexer.Options{Scanner: scanner})

	token := outer.Range(7, 12).NextToken()

	c.Assert(token, qt.Equals, lexer.Token{Type: lexer.TokenOperator, Value: "'a;b'", Start: 7, End: 12})
	c.Assert(scanner.calls, qt.DeepEquals, []scanCall{
		{Text: input, Start: 0, Head: true},
		{Text: input[:12], Start: 7, Head: false},
	})
}

// StringValue under a scanner is the scanner's answer, which the shared rules
// would not give: they read the doubled quote as one quote.
func TestStringValue_AsksTheScanner(t *testing.T) {
	c := qt.New(t)

	text, ok := lexer.StringValue("'a''b'", lexer.Options{Scanner: &wholeScanner{}})

	c.Assert(ok, qt.IsTrue)
	c.Assert(text, qt.Equals, "string 'a''b'")
}

// Without a scanner the shared rules read the same text: the control for the
// tests above.
func TestNewLexerWithOptions_SharedRulesWithoutAScanner(t *testing.T) {
	c := qt.New(t)
	input := "SELECT 'a;b'; SELECT 2"

	tokens := lexedTokens(input, lexer.Options{StandardStrings: true})
	text, ok := lexer.StringValue("'a''b'", lexer.Options{StandardStrings: true})

	c.Assert(tokens[:5], qt.DeepEquals, []sharedToken{
		{lexer.TokenIdentifier, "SELECT"}, {lexer.TokenWhitespace, " "}, {lexer.TokenString, "'a;b'"},
		{lexer.TokenSemicolon, ";"}, {lexer.TokenWhitespace, " "},
	})
	c.Assert(ok, qt.IsTrue)
	c.Assert(text, qt.Equals, "a'b")
}
