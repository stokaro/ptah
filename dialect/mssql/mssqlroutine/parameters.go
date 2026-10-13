package mssqlroutine

import (
	"strings"

	"ptah.run/internal/lexer"
)

// NormalizeParameters removes the optional AS after each T-SQL parameter name
// and formats separators as sys.parameters reports them. Parameter names, order,
// type dimensions, quoted text, and defaults are preserved. Comments are ignored.
// The empty list stays empty. The result is deterministic and idempotent.
func NormalizeParameters(parameters string) string {
	var tokens []lexer.Token
	for _, token := range tokenize(parameters) {
		if !isTrivia(token) && token.Type != lexer.TokenEOF {
			tokens = append(tokens, token)
		}
	}
	var result strings.Builder
	depth, start := 0, 0
	var previous lexer.Token
	for i, token := range tokens {
		if i == start+2 && tokens[start].MatchOperatorValue("@") && tokens[start+1].Type == lexer.TokenIdentifier && token.MatchIdentifierValue("AS") && i+1 < len(tokens) && tokens[i+1].Type != lexer.TokenOperator {
			continue
		}
		switch {
		case token.MatchOperatorValue("("):
			depth++
		case token.MatchOperatorValue(")"):
			depth--
		case token.MatchOperatorValue(","):
			if depth == 0 {
				start = i + 1
			}
		}
		if result.Len() > 0 && parameterSpace(previous, token, depth) {
			result.WriteByte(' ')
		}
		result.WriteString(token.Value)
		previous = token
	}
	return result.String()
}

func parameterSpace(previous, current lexer.Token, depth int) bool {
	if current.MatchOperatorValue(",") || current.MatchOperatorValue("(") || current.MatchOperatorValue(")") || current.MatchOperatorValue(".") || previous.MatchOperatorValue("(") || previous.MatchOperatorValue(".") {
		return false
	}
	if previous.MatchOperatorValue(",") {
		return depth == 0
	}
	if previous.MatchOperatorValue("@") || (previous.MatchIdentifierValue("N") && current.Type == lexer.TokenString) {
		return previous.End != current.Start
	}
	return true
}
