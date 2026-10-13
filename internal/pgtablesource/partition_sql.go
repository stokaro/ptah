package pgtablesource

import (
	"context"
	"fmt"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/core/sqlext"
	"ptah.run/dialect/postgres/pgtable"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
	"ptah.run/internal/normalize"
	"ptah.run/internal/sqlident"
)

// ParsePartitionSQL reads a PostgreSQL partition definition, with or without
// PARTITION BY. It also accepts pg_get_partkeydef output. Column names retain
// quoted spelling; expression SQL and key modifiers remain separate. Invalid
// or trailing syntax returns no model and wraps schemaext.ErrInvalidValue.
func ParsePartitionSQL(text string) (*pgtable.DesiredPartition, error) {
	tokens, err := partitionTokens(text)
	if err != nil {
		return nil, err
	}
	if len(tokens) >= 2 && tokens[0].MatchIdentifierValue("PARTITION") && tokens[1].MatchIdentifierValue("BY") {
		tokens = tokens[2:]
	}
	if len(tokens) < 4 || !tokens[1].MatchOperatorValue("(") || !tokens[len(tokens)-1].MatchOperatorValue(")") {
		return nil, partitionSyntax("expected a partition strategy and parenthesized key")
	}
	value := &pgtable.DesiredPartition{Type: strings.ToUpper(tokens[0].Value)}
	items, err := partitionItems(tokens[2 : len(tokens)-1])
	if err != nil {
		return nil, err
	}
	for _, item := range items {
		part, err := parsePartitionItem(text, item)
		if err != nil {
			return nil, err
		}
		value.Parts = append(value.Parts, part)
	}
	if err := pgtable.ValidateDesiredPartition(value); err != nil {
		return nil, err
	}
	return value, nil
}

func partitionSyntax(reason string) error {
	return fmt.Errorf("%w: PostgreSQL partition: %s", schemaext.ErrInvalidValue, reason)
}

func partitionTokens(text string) ([]lexer.Token, error) {
	scan := lexer.NewLexerWithOptions(text, dialectlexer.Options(platform.Postgres))
	var tokens []lexer.Token
	for token := scan.NextToken(); token.Type != lexer.TokenEOF; token = scan.NextToken() {
		if token.Type == lexer.TokenWhitespace || token.Type == lexer.TokenComment {
			continue
		}
		if token.Type == lexer.TokenUnknown || token.Type == lexer.TokenSemicolon {
			return nil, partitionSyntax("unexpected token " + token.Value)
		}
		tokens = append(tokens, token)
	}
	return tokens, nil
}

func partitionItems(tokens []lexer.Token) ([][]lexer.Token, error) {
	var items [][]lexer.Token
	start, depth := 0, 0
	for i, token := range tokens {
		switch {
		case token.MatchOperatorValue("("):
			depth++
		case token.MatchOperatorValue(")"):
			depth--
			if depth < 0 {
				return nil, partitionSyntax("unbalanced partition key")
			}
		case token.MatchOperatorValue(",") && depth == 0:
			items = append(items, tokens[start:i])
			start = i + 1
		}
	}
	if depth != 0 {
		return nil, partitionSyntax("unbalanced partition key")
	}
	return append(items, tokens[start:]), nil
}

func parsePartitionItem(text string, tokens []lexer.Token) (pgtable.PartitionPart, error) {
	var part pgtable.PartitionPart
	if len(tokens) == 0 {
		return part, partitionSyntax("empty partition key")
	}
	name, end, err := partitionBase(tokens)
	if err != nil {
		return part, err
	}
	part.Name = name
	if end <= 0 {
		return part, partitionSyntax("unbalanced partition expression")
	}
	if part.Name == "" {
		part.Expr = normalize.Expression(strings.TrimSpace(text[tokens[0].Start:tokens[end-1].End]))
	}
	if end < len(tokens) && tokens[end].MatchIdentifierValue("COLLATE") {
		name, after, err := partitionIdentifier(tokens, end+1)
		if err != nil {
			return part, err
		}
		part.Collation, end = name, after
	}
	if end < len(tokens) {
		name, after, err := partitionIdentifier(tokens, end)
		if err != nil {
			return part, err
		}
		part.OperatorClass, end = name, after
	}
	if end != len(tokens) {
		return part, partitionSyntax("unexpected key modifier " + tokens[end].Value)
	}
	return part, nil
}

func partitionClose(tokens []lexer.Token, start int) int {
	depth := 0
	for i := start; i < len(tokens); i++ {
		if tokens[i].MatchOperatorValue("(") {
			depth++
		}
		if tokens[i].MatchOperatorValue(")") {
			depth--
			if depth == 0 {
				return i + 1
			}
		}
	}
	return 0
}

func partitionIdentifier(tokens []lexer.Token, start int) (string, int, error) {
	var names []string
	for i := start; i < len(tokens); i += 2 {
		value := tokens[i].Value
		quoted := strings.HasPrefix(value, `"`) && strings.HasSuffix(value, `"`)
		if !quoted && (tokens[i].Type != lexer.TokenIdentifier || value == "" || value[0] >= '0' && value[0] <= '9') {
			return "", 0, partitionSyntax("expected an identifier, got " + value)
		}
		if partitionName(value) == "" {
			return "", 0, partitionSyntax("empty identifier")
		}
		names = append(names, sqlident.Quote(platform.Postgres, partitionName(value)))
		if i+1 >= len(tokens) || !tokens[i+1].MatchOperatorValue(".") {
			return strings.Join(names, "."), i + 1, nil
		}
	}
	return "", 0, partitionSyntax("incomplete identifier")
}

func partitionName(value string) string {
	if strings.HasPrefix(value, `"`) {
		return sqlident.UnquoteDoubleQuoted(value)
	}
	return strings.Map(func(r rune) rune {
		if r >= 'A' && r <= 'Z' {
			return r + ('a' - 'A')
		}
		return r
	}, value)
}

// partitionSQL reads the whole batch without retaining frontend state.
type partitionSQL struct{}

func (partitionSQL) ParseSQL(ctx context.Context, request sqlext.Request) (sqlext.Result, error) {
	if ctx == nil {
		return sqlext.Result{}, partitionSyntax("SQL requires a context")
	}
	result := sqlext.Result{Values: make([]sqlext.Value, 0, len(request.Claims))}
	for _, claim := range request.Claims {
		if err := ctx.Err(); err != nil {
			return sqlext.Result{}, err
		}
		if claim.Pattern != "partition" || claim.Site != sqlext.TableOption {
			return sqlext.Result{}, partitionSyntax("unexpected SQL claim")
		}
		value, err := ParsePartitionSQL(claim.Text)
		if err != nil {
			return sqlext.Result{}, err
		}
		facets, err := schemaext.NewFacets(value)
		if err != nil {
			return sqlext.Result{}, err
		}
		result.Values = append(result.Values, sqlext.Value{Facets: facets})
	}
	if err := ctx.Err(); err != nil {
		return sqlext.Result{}, err
	}
	return result, nil
}

func partitionBase(tokens []lexer.Token) (string, int, error) {
	if tokens[0].MatchOperatorValue("(") {
		return "", partitionClose(tokens, 0), nil
	}
	_, after, err := partitionIdentifier(tokens, 0)
	if err != nil {
		return "", 0, err
	}
	if after < len(tokens) && tokens[after].MatchOperatorValue("(") {
		return "", partitionClose(tokens, after), nil
	}
	if after != 1 {
		return "", 0, partitionSyntax("a column key cannot be qualified")
	}
	return partitionName(tokens[0].Value), 1, nil
}
