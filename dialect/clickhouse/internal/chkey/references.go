package chkey

import (
	"slices"
	"strings"

	"ptah.run/internal/lexer"
)

// PlainColumnReferences returns, unquoted and in order, the names an expression
// uses where only a column can stand: an element of a top-level list or
// tuple, or an argument of a call, written as one bare identifier. Such a name
// is a column the server must find when it adds an index on the expression.
//
// Everything that could be read as something else is left out, so a caller
// may refuse every name returned: a function name, a literal such as 1, NULL
// or nan, a lambda parameter and the names it binds, a type after AS or ::,
// and an identifier with an operator, keyword or member access beside it.
// A misspelled column inside a larger expression, such as `lower(paylod) + 1`,
// is therefore not reported.
func PlainColumnReferences(expression string) []string {
	tokens := significantTokens(expression)
	bound := lambdaParameters(tokens)
	types := typePositions(tokens)
	var names []string
	for i, token := range tokens {
		if !isIdentifier(token) || types[i] || isLiteral(token.Value) || bound[unquote(token.Value)] {
			continue
		}
		if i > 0 && !matchesAny(tokens[i-1], "(", ",") {
			continue
		}
		if i+1 < len(tokens) && !matchesAny(tokens[i+1], ")", ",") {
			continue
		}
		names = append(names, unquote(token.Value))
	}
	return names
}

// lambdaParameters returns the names a lambda binds: an identifier written
// before ->, or each identifier of the parenthesized list written before it.
func lambdaParameters(tokens []lexer.Token) map[string]bool {
	bound := make(map[string]bool)
	for i := range tokens {
		if !arrowAt(tokens, i) || i == 0 {
			continue
		}
		if isIdentifier(tokens[i-1]) {
			bound[unquote(tokens[i-1].Value)] = true
			continue
		}
		if !tokens[i-1].MatchOperatorValue(")") {
			continue
		}
		for j := i - 2; j >= 0 && !tokens[j].MatchOperatorValue("("); j-- {
			if isIdentifier(tokens[j]) {
				bound[unquote(tokens[j].Value)] = true
			}
		}
	}
	return bound
}

// arrowAt reports whether tokens[i:] begins with the lambda arrow, which the
// lexer splits into - and >.
func arrowAt(tokens []lexer.Token, i int) bool {
	return i+1 < len(tokens) && tokens[i].MatchOperatorValue("-") && tokens[i+1].MatchOperatorValue(">")
}

// typePositions marks the tokens of each type written after AS or ::,
// including its parenthesized arguments, as in CAST(x AS Nullable(String)).
func typePositions(tokens []lexer.Token) map[int]bool {
	types := make(map[int]bool)
	for i := range tokens {
		start := -1
		switch {
		case isIdentifier(tokens[i]) && strings.EqualFold(tokens[i].Value, "AS"):
			start = i + 1
		case i+1 < len(tokens) && tokens[i].MatchOperatorValue(":") && tokens[i+1].MatchOperatorValue(":"):
			start = i + 2
		}
		if start < 0 || start >= len(tokens) {
			continue
		}
		types[start] = true
		if start+1 >= len(tokens) || !tokens[start+1].MatchOperatorValue("(") {
			continue
		}
		for j, depth := start+1, 0; j < len(tokens); j++ {
			types[j] = true
			switch {
			case tokens[j].MatchOperatorValue("("):
				depth++
			case tokens[j].MatchOperatorValue(")"):
				depth--
			}
			if depth == 0 {
				break
			}
		}
	}
	return types
}

// isLiteral reports a bare token the lexer calls an identifier that is a
// number or one of ClickHouse's literal keywords.
func isLiteral(value string) bool {
	if value != "" && value[0] >= '0' && value[0] <= '9' {
		return true
	}
	return slices.ContainsFunc([]string{"NULL", "TRUE", "FALSE", "INF", "NAN"}, func(literal string) bool {
		return strings.EqualFold(value, literal)
	})
}

func matchesAny(token lexer.Token, operators ...string) bool {
	return slices.ContainsFunc(operators, token.MatchOperatorValue)
}
