package chcompare

import (
	"slices"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
)

func sameTable(a, b *chschema.ObservedTable) bool {
	return sameExpression(a.Engine, b.Engine) && sameKey(a.OrderBy, b.OrderBy) && sameKey(a.PrimaryKey, b.PrimaryKey) &&
		sameKey(a.PartitionBy, b.PartitionBy) && sameExpression(a.SampleBy, b.SampleBy) &&
		sameExpression(a.TTL, b.TTL) && sameExpression(a.Settings, b.Settings)
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

func sameExpression(a, b string) bool { return slices.Equal(expressionTokens(a), expressionTokens(b)) }

func sameKey(a, b string) bool { return slices.Equal(keyTokens(a), keyTokens(b)) }

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
