package pgtable

import (
	"strconv"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
	"ptah.run/internal/normalize"
	"ptah.run/internal/sqlident"
)

func partitionExpressionKey(expression string) string {
	scan := lexer.NewLexerWithOptions(normalize.Expression(expression), dialectlexer.Options(platform.Postgres))
	var key strings.Builder
	for token := scan.NextToken(); token.Type != lexer.TokenEOF; token = scan.NextToken() {
		if token.Type == lexer.TokenWhitespace || token.Type == lexer.TokenComment {
			continue
		}
		value, kind := token.Value, token.Type
		if strings.HasPrefix(value, `"`) && strings.HasSuffix(value, `"`) {
			value, kind = sqlident.UnquoteDoubleQuoted(value), lexer.TokenIdentifier
		} else if kind == lexer.TokenIdentifier {
			value = strings.Map(func(r rune) rune {
				if r >= 'A' && r <= 'Z' {
					return r + ('a' - 'A')
				}
				return r
			}, value)
		}
		key.WriteString(strconv.Itoa(int(kind)))
		key.WriteByte(':')
		key.WriteString(strconv.Quote(value))
	}
	return key.String()
}

// PartitionComparisonKey encodes a captured key for comparison. It folds SQL
// whitespace and unquoted identifier case, preserving key order, literals,
// column spelling and modifiers. Nil denotes an ordinary table. It performs no
// database normalization and does not establish expression equivalence.
func PartitionComparisonKey(partition *Partition) string {
	if partition == nil {
		return ""
	}
	var key strings.Builder
	key.WriteString(partition.Type)
	for _, part := range partition.Parts {
		for _, value := range []string{part.Name, partitionExpressionKey(part.Expr), partitionExpressionKey(part.Collation), partitionExpressionKey(part.OperatorClass)} {
			key.WriteByte(';')
			key.WriteString(strconv.Quote(value))
		}
	}
	return key.String()
}
