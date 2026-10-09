// Package chsql shares ClickHouse storage expression comparisons between the
// owning comparator, planner, and renderer. It does not assert general SQL
// equivalence; only token spacing and outer key-list grouping are normalized.
package chsql

import (
	"slices"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
)

// SameTable compares all captured storage properties using their clause rules.
func SameTable(a, b *chschema.ObservedTable) bool {
	return SameExpression(a.Engine, b.Engine) && SameKey(a.OrderBy, b.OrderBy) && SameKey(a.PrimaryKey, b.PrimaryKey) &&
		SameKey(a.PartitionBy, b.PartitionBy) && SameExpression(a.SampleBy, b.SampleBy) &&
		SameExpression(a.TTL, b.TTL) && SameExpression(a.Settings, b.Settings)
}

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
