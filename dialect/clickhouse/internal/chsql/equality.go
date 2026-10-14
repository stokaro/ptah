// Package chsql shares ClickHouse storage expression comparisons and the rules
// of in-place storage changes between the owning comparator, planner, renderer
// and reversal. It does not assert general SQL equivalence; only token spacing,
// outer key-list grouping and the order of table settings are normalized.
package chsql

import (
	"slices"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
)

// SameTable compares current storage a with declared storage b, clause by
// clause: keys without their outer grouping, settings as a set of entries in
// which index_granularity that only a names does not count, and everything
// else by tokens. Two observations compare the same way.
func SameTable(a, b *chschema.ObservedTable) bool { return len(Changed(a, b)) == 0 }

// SameIndex compares type tokens and the exact unsigned granularity. Type
// parameters retain their token boundaries, quoted values, and order.
func SameIndex(a, b *chschema.ObservedIndex) bool {
	return a.Granularity == b.Granularity && SameExpression(a.IndexType, b.IndexType)
}

// Preserve token boundaries, quoted text, identifier case, and order. Removing
// whitespace from the whole expression would equate different string literals.
func expressionTokens(expression string) []string {
	lex := lexer.NewLexerWithOptions(expression, dialectlexer.Options(platform.ClickHouse))
	var tokens []string
	for {
		token := lex.NextToken()
		switch token.Type {
		case lexer.TokenEOF:
			return tokens
		case lexer.TokenWhitespace, lexer.TokenComment:
			continue
		default:
			tokens = append(tokens, token.Value)
		}
	}
}

// SameExpression compares tokens while preserving literals, case, and order.
func SameExpression(a, b string) bool { return slices.Equal(expressionTokens(a), expressionTokens(b)) }

// SameKey also accepts catalog key lists without their outer tuple grouping.
func SameKey(a, b string) bool { return slices.Equal(keyTokens(a), keyTokens(b)) }

func keyTokens(expression string) []string {
	tokens := expressionTokens(expression)
	// system.tables may omit the outer tuple parentheses on a key list. Strip
	// only a balanced pair enclosing the entire expression, never inner groups.
	for enclosesKey(tokens) {
		tokens = tokens[1 : len(tokens)-1]
	}
	if len(tokens) == 3 && strings.EqualFold(tokens[0], "tuple") && tokens[1] == "(" && tokens[2] == ")" {
		return nil
	}
	return tokens
}

func enclosesKey(tokens []string) bool {
	if len(tokens) < 2 || tokens[0] != "(" || tokens[len(tokens)-1] != ")" {
		return false
	}
	depth := 0
	for i, token := range tokens {
		switch token {
		case "(":
			depth++
		case ")":
			depth--
		}
		if depth == 0 {
			return i == len(tokens)-1
		}
	}
	return false
}
