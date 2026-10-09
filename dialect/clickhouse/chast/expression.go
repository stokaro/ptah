package chast

import (
	"strings"

	"ptah.run/core/platform"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
)

// SkippingIndexExpression joins an index's key parts into the expression
// ADD INDEX takes. Several parts form one tuple. A single part is used as
// written unless it is itself a comma-separated list, which is how
// system.data_skipping_indices reports the key of `INDEX i (a, b)`: ADD INDEX
// would read the bare list as the end of its expression, so the list becomes a
// tuple too. Commas inside parentheses, brackets, braces or string literals do
// not count. No parts yield an empty string.
func SkippingIndexExpression(parts []string) string {
	switch {
	case len(parts) == 0:
		return ""
	case len(parts) == 1 && !topLevelList(parts[0]):
		return parts[0]
	default:
		return "(" + strings.Join(parts, ", ") + ")"
	}
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
