// Package chindexkey reads the key of a ClickHouse data-skipping index: it
// joins an index's key parts into the expression ADD INDEX takes, and decides
// whether a declared key and the one system.data_skipping_indices reports are
// one key. The ClickHouse renderer and the shared schema comparison both ask
// it, so the two cannot disagree about what an index's key is.
package chindexkey

import (
	"strings"

	"ptah.run/core/platform"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
)

// Expression joins an index's key parts into the expression ADD INDEX takes.
// Several parts form one tuple. A single part is used as written unless it is
// itself a comma-separated list, which is how system.data_skipping_indices
// reports the key of `INDEX i (a, b)`: ADD INDEX would read the bare list as
// the end of its expression, so the list becomes a tuple too. Commas inside
// parentheses, brackets, braces or string literals do not count. No parts
// yield an empty string.
func Expression(parts []string) string {
	switch {
	case len(parts) == 0:
		return ""
	case len(parts) == 1 && !topLevelList(parts[0]):
		return parts[0]
	default:
		return "(" + strings.Join(parts, ", ") + ")"
	}
}

// Same reports whether two key part lists are one index key.
//
// ClickHouse stores the key expression in its own formatting, and a
// comparison of the text as written would rebuild an index nobody changed.
// Measured on 26.9.14, system.data_skipping_indices reports `LOWER(a)` as
// `lower(a)`, `(a,b)` and `tuple(a, b)` as `a, b`, `b * 2 + 1` as
// `(b * 2) + 1`, and `cityHash64(a)%10` as `cityHash64(a) % 10`. So both
// sides are read into one form first: spacing and case outside string
// literals are dropped, parentheses that only group are removed, and a key
// that is one tuple() call is read as the list it holds. A change that only
// regroups an expression, or only changes the case of a name, is therefore
// not a change here.
func Same(desired, current []string) bool {
	return keyForm(Expression(desired)) == keyForm(Expression(current))
}

// keyForm reduces a key expression to the form [Same] compares.
func keyForm(expression string) string {
	tokens := significantTokens(expression)
	tokens = withoutGroupingParentheses(tokens)
	if len(tokens) > 3 && tokens[0] == "tuple" && tokens[1] == "(" && closing(tokens, 1) == len(tokens)-1 {
		tokens = tokens[2 : len(tokens)-1]
	}
	return strings.Join(tokens, " ")
}

// significantTokens lists the tokens of expression without whitespace or
// comments, each outside a string literal in lower case.
func significantTokens(expression string) []string {
	lex := lexer.NewLexerWithOptions(expression, dialectlexer.Options(platform.ClickHouse))
	var tokens []string
	for {
		token := lex.NextToken()
		switch token.Type {
		case lexer.TokenEOF:
			return tokens
		case lexer.TokenWhitespace, lexer.TokenComment:
			continue
		case lexer.TokenString:
			tokens = append(tokens, token.Value)
		default:
			tokens = append(tokens, strings.ToLower(token.Value))
		}
	}
}

// withoutGroupingParentheses removes each parenthesis pair that groups rather
// than calls: one that does not follow a name. A call keeps its parentheses,
// so `f(a)` and `f a` stay apart.
func withoutGroupingParentheses(tokens []string) []string {
	kept := make([]string, 0, len(tokens))
	var grouping []bool
	for position, token := range tokens {
		switch token {
		case "(":
			isGroup := position == 0 || !isName(tokens[position-1])
			grouping = append(grouping, isGroup)
			if isGroup {
				continue
			}
		case ")":
			if len(grouping) > 0 {
				isGroup := grouping[len(grouping)-1]
				grouping = grouping[:len(grouping)-1]
				if isGroup {
					continue
				}
			}
		}
		kept = append(kept, token)
	}
	return kept
}

// isName reports whether a token can name a function a parenthesis calls.
func isName(token string) bool {
	if token == "" {
		return false
	}
	first := token[0]
	return first == '_' || first == '`' || first == '"' || (first >= 'a' && first <= 'z')
}

// closing returns the position of the parenthesis that closes the one at
// open, or -1.
func closing(tokens []string, open int) int {
	depth := 0
	for position := open; position < len(tokens); position++ {
		switch tokens[position] {
		case "(":
			depth++
		case ")":
			depth--
			if depth == 0 {
				return position
			}
		}
	}
	return -1
}

func topLevelList(expression string) bool {
	lex := lexer.NewLexerWithOptions(expression, dialectlexer.Options(platform.ClickHouse))
	depth := 0
	for {
		token := lex.NextToken()
		switch {
		case token.Type == lexer.TokenEOF:
			return false
		case token.Type != lexer.TokenOperator:
			continue
		case token.MatchOperatorValue("(") || token.MatchOperatorValue("[") || token.MatchOperatorValue("{"):
			depth++
		case token.MatchOperatorValue(")") || token.MatchOperatorValue("]") || token.MatchOperatorValue("}"):
			depth--
		case depth == 0 && token.MatchOperatorValue(","):
			return true
		}
	}
}
