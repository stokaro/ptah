package sqlutil

import "github.com/stokaro/ptah/internal/lexer"

// IsScalarIFExpressionFragment reports whether fragment starts with a MySQL
// scalar IF(...) expression tail rather than a procedural IF ... THEN block.
func IsScalarIFExpressionFragment(fragment string) bool {
	l := lexer.NewLexer(fragment)
	tok := nextNonTriviaToken(l)
	if !tok.MatchOperatorValue("(") {
		return false
	}

	depth := 1
	for depth > 0 {
		tok = l.NextToken()
		switch {
		case tok.Type == lexer.TokenEOF:
			return true
		case tok.MatchOperatorValue("("):
			depth++
		case tok.MatchOperatorValue(")"):
			depth--
		}
	}

	tok = nextNonTriviaToken(l)
	return !tok.MatchIdentifierValue("THEN")
}

func nextNonTriviaToken(l *lexer.Lexer) lexer.Token {
	for {
		tok := l.NextToken()
		if tok.Type != lexer.TokenWhitespace && tok.Type != lexer.TokenComment {
			return tok
		}
	}
}
