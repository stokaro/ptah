// Package compound reads compound-statement boundaries shared by the
// compound routine owner and the permissive SQL function grammar.
package compound

import (
	"ptah.run/internal/lexer"
)

// ScalarIF distinguishes IF(...) expressions from IF ... THEN blocks.
func ScalarIF(fragment string) bool {
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

// TrackKeyword advances block and CASE depths and recognizes END trailers.
func TrackKeyword(keyword string, blockDepth, caseDepth *int, pendingEndTrailer *bool) {
	if *pendingEndTrailer {
		*pendingEndTrailer = false
		if isRoutineEndTrailerKeyword(keyword) {
			return
		}
	}

	switch keyword {
	case "BEGIN", "IF", "LOOP", "REPEAT", "WHILE":
		(*blockDepth)++
	case "CASE":
		(*caseDepth)++
	case "END":
		if *caseDepth > 0 {
			(*caseDepth)--
		} else if *blockDepth > 0 {
			(*blockDepth)--
		}
		*pendingEndTrailer = true
	}
}

func isRoutineEndTrailerKeyword(keyword string) bool {
	switch keyword {
	case "CASE", "IF", "LOOP", "REPEAT", "WHILE":
		return true
	default:
		return false
	}
}
