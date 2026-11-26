// Package chindexkey reads the key of a ClickHouse data-skipping index: it
// joins an index's key parts into the expression ADD INDEX takes, and decides
// whether a declared key and the one system.data_skipping_indices reports are
// one key. The ClickHouse renderer and the shared schema comparison both ask
// it, so the two cannot disagree about what an index's key is.
package chindexkey

import (
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/chcolumn"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
	"ptah.run/internal/sqlident"
)

// Written returns parts as they stand in the key ADD INDEX takes: an
// expression as it is written, and a column name quoted as ClickHouse quotes a
// name. Written bare, a column named x-y is the expression x - y, which the
// server builds and reports back as the key, so no later plan sees the
// difference; and a name holding a backtick or a backslash is refused when
// the plan runs. Measured on ClickHouse 24.10 and 26.10, the quoted name
// builds an index on the column, reported as `x-y`. A part with neither stays
// empty, for the check that refuses a key with no parts to report.
func Written(parts []schemamodel.IndexPart) []string {
	written := make([]string, 0, len(parts))
	for _, part := range parts {
		switch expression := strings.TrimSpace(part.Expr); {
		case expression != "":
			written = append(written, expression)
		case part.Name != "":
			written = append(written, sqlident.Quote(platform.ClickHouse, part.Name))
		default:
			written = append(written, "")
		}
	}
	return written
}

// Reported returns the key of an index a catalog describes, written as
// [Written] writes it. The reader reports the server's key as one expression
// part. A catalog projected from a declaration carries the declared parts, or
// only the key columns where the declaration names no parts, and those are
// column names. So a column named x-y compares as the same quoted name on
// both sides, wherever the catalog came from.
func Reported(index catalog.Index) []string {
	parts := make([]schemamodel.IndexPart, 0, max(len(index.Parts), len(index.Columns)))
	for _, part := range index.Parts {
		parts = append(parts, schemamodel.IndexPart{Name: part.Name, Expr: part.Expr})
	}
	if len(index.Parts) == 0 {
		for _, column := range index.Columns {
			parts = append(parts, schemamodel.IndexPart{Name: column})
		}
	}
	return Written(parts)
}

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
// comments, each outside a string literal in lower case. A quoted name is
// first written the way [chcolumn.ComparableIdentifier] compares it, so the
// server's spelling of a name and the declaration's agree.
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
			tokens = append(tokens, chcolumn.ComparableIdentifier(token.Value))
		default:
			tokens = append(tokens, strings.ToLower(chcolumn.ComparableIdentifier(token.Value)))
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
