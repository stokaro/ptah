package pgtable

import (
	"fmt"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
	"ptah.run/internal/sqlident"
)

// ParseDeleteColumnList reads a complete parenthesized column list. Names are
// returned without SQL quoting, with unquoted PostgreSQL case folding. Empty
// lists, qualified names, expressions and trailing syntax fail without values.
func ParseDeleteColumnList(text string) ([]string, error) {
	tokens, err := deleteColumnsTokens(text)
	if err != nil {
		return nil, err
	}
	return deleteColumnsFromTokens(tokens)
}
func deleteColumnsFromTokens(tokens []lexer.Token) ([]string, error) {
	if len(tokens) < 3 || !tokens[0].MatchOperatorValue("(") || !tokens[len(tokens)-1].MatchOperatorValue(")") {
		return nil, deleteColumnsSyntax("expected a nonempty parenthesized column list")
	}
	tokens = tokens[1 : len(tokens)-1]
	var columns []string
	for i, token := range tokens {
		if i%2 == 1 {
			if !token.MatchOperatorValue(",") {
				return nil, deleteColumnsSyntax("expected a comma between column names")
			}
			continue
		}
		name, err := deleteColumnName(token)
		if err != nil {
			return nil, err
		}
		columns = append(columns, name)
	}
	if len(tokens)%2 == 0 {
		return nil, deleteColumnsSyntax("expected a column after comma")
	}
	if err := validateDeleteColumns(columns); err != nil {
		return nil, err
	}
	return columns, nil
}
func deleteColumnName(token lexer.Token) (string, error) {
	if strings.HasPrefix(token.Value, `"`) && strings.HasSuffix(token.Value, `"`) {
		return sqlident.UnquoteDoubleQuoted(token.Value), nil
	}
	if token.Type != lexer.TokenIdentifier {
		return "", deleteColumnsSyntax("expected an unqualified column name")
	}
	return strings.Map(func(r rune) rune {
		if r >= 'A' && r <= 'Z' {
			return r + ('a' - 'A')
		}
		return r
	}, token.Value), nil
}
func deleteColumnsTokens(text string) ([]lexer.Token, error) {
	scan := lexer.NewLexerWithOptions(text, dialectlexer.Options(platform.Postgres))
	var tokens []lexer.Token
	for token := scan.NextToken(); token.Type != lexer.TokenEOF; token = scan.NextToken() {
		if token.Type == lexer.TokenWhitespace || token.Type == lexer.TokenComment {
			continue
		}
		if token.Type == lexer.TokenUnknown || token.Type == lexer.TokenSemicolon {
			return nil, deleteColumnsSyntax("unexpected token " + token.Value)
		}
		tokens = append(tokens, token)
	}
	return tokens, nil
}
func deleteColumnsSyntax(reason string) error {
	return fmt.Errorf("%w: PostgreSQL ON DELETE column list: %s", schemaext.ErrInvalidValue, reason)
}

// DeleteColumnsFromDefinition reads PostgreSQL's pg_get_constraintdef output.
// A definition without a delete-action list returns nil. Malformed lists fail;
// quoted identifiers containing action words are not treated as SQL clauses.
// Reading the definition works on servers without pg_constraint.confdelsetcols.
func DeleteColumnsFromDefinition(definition string) ([]string, error) {
	tokens, err := deleteColumnsTokens(definition)
	if err != nil {
		return nil, err
	}
	for i := 0; i+4 < len(tokens); i++ {
		if !tokens[i].MatchIdentifierValue("ON") || !tokens[i+1].MatchIdentifierValue("DELETE") || !tokens[i+2].MatchIdentifierValue("SET") ||
			(!tokens[i+3].MatchIdentifierValue("NULL") && !tokens[i+3].MatchIdentifierValue("DEFAULT")) || !tokens[i+4].MatchOperatorValue("(") {
			continue
		}
		for end := i + 5; end < len(tokens); end++ {
			if tokens[end].MatchOperatorValue(")") {
				return deleteColumnsFromTokens(tokens[i+4 : end+1])
			}
		}
		return nil, deleteColumnsSyntax("unterminated column list in constraint definition")
	}
	return nil, nil
}
