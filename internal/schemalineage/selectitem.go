package schemalineage

import (
	"fmt"
	"slices"

	"go.5x5.cz/ptah/internal/lexer"
)

// splitSelectList divides the select list on top-level commas.
//
// Top-level is the whole point: `coalesce(a, b) AS c` is one item containing a
// comma, and splitting on every comma would read it as two.
func splitSelectList(tokens []lexer.Token) [][]lexer.Token {
	var items [][]lexer.Token
	depth, start := 0, 0
	for i, token := range tokens {
		switch {
		case token.MatchOperatorValue("("):
			depth++
		case token.MatchOperatorValue(")"):
			depth--
		case depth == 0 && token.MatchOperatorValue(","):
			items = append(items, tokens[start:i])
			start = i + 1
		}
	}
	if start < len(tokens) {
		items = append(items, tokens[start:])
	}
	return items
}

// resolveSelectItem turns one select-list entry into the edges it declares.
func resolveSelectItem(
	item []lexer.Token,
	view string,
	source sourceRef,
	materialized bool,
	columns map[string][]string,
) ([]Edge, error) {
	if len(item) == 0 {
		return nil, fmt.Errorf("the select list has an empty entry")
	}

	// A star projection names every column of the source. It resolves when the
	// schema declares that table, and is undecidable when it does not -- the
	// names live in the table, not in the view.
	if star, ok := starProjection(item, source); ok {
		names := columns[lowerName(source.table)]
		if len(names) == 0 {
			return nil, fmt.Errorf(
				"the select list is %s and table %q declares no columns here, so its names are unknown",
				star, source.table)
		}
		edges := make([]Edge, 0, len(names))
		for _, column := range names {
			edges = append(edges, Edge{
				FromTable: source.table, FromColumn: column,
				ToView: view, ToColumn: column, Materialized: materialized,
			})
		}
		return edges, nil
	}

	expression, alias := splitAlias(item)
	if len(expression) == 0 {
		return nil, fmt.Errorf("a select entry has an alias with no expression")
	}

	if column, ok := plainColumn(expression, source); ok {
		output := alias
		if output == "" {
			output = column
		}
		return []Edge{{
			FromTable: source.table, FromColumn: column,
			ToView: view, ToColumn: output, Materialized: materialized,
		}}, nil
	}

	// An expression over columns still carries lineage: every column it names
	// feeds the output. Without an alias there is no output name to attach
	// them to -- the server derives one, by rules this does not reproduce.
	if alias == "" {
		return nil, fmt.Errorf("a computed select entry has no alias, so its output column has no name here")
	}
	referenced := referencedColumns(expression, source)
	if len(referenced) == 0 {
		// A constant feeds no column. That is a resolved answer, not a gap.
		return nil, nil
	}
	edges := make([]Edge, 0, len(referenced))
	for _, column := range referenced {
		edges = append(edges, Edge{
			FromTable: source.table, FromColumn: column,
			ToView: view, ToColumn: alias, Materialized: materialized,
		})
	}
	return edges, nil
}

// starProjection reports whether the entry is `*` or `alias.*`.
func starProjection(item []lexer.Token, source sourceRef) (string, bool) {
	if len(item) == 1 && item[0].MatchOperatorValue("*") {
		return "*", true
	}
	if len(item) == 3 && item[0].Type == lexer.TokenIdentifier &&
		item[1].MatchOperatorValue(".") && item[2].MatchOperatorValue("*") {
		if sameName(unquote(item[0].Value), source.alias) || sameName(unquote(item[0].Value), source.table) {
			return unquote(item[0].Value) + ".*", true
		}
	}
	return "", false
}

// splitAlias separates an entry's expression from its output name.
func splitAlias(item []lexer.Token) ([]lexer.Token, string) {
	if len(item) >= 2 && item[len(item)-2].MatchIdentifierValue("as") &&
		item[len(item)-1].Type == lexer.TokenIdentifier {
		return item[:len(item)-2], unquote(item[len(item)-1].Value)
	}
	// `expr name` without AS is an alias too, but only when the expression is
	// not itself a bare identifier -- `a b` is ambiguous and left alone.
	if len(item) >= 3 && item[len(item)-1].Type == lexer.TokenIdentifier &&
		!item[len(item)-1].MatchIdentifierValue("as") &&
		!item[len(item)-2].MatchOperatorValue(".") {
		return item[:len(item)-1], unquote(item[len(item)-1].Value)
	}
	return item, ""
}

// plainColumn reports the column when the expression is nothing but a column
// reference, optionally qualified by the source's name.
func plainColumn(expression []lexer.Token, source sourceRef) (string, bool) {
	switch len(expression) {
	case 1:
		if expression[0].Type == lexer.TokenIdentifier && !isClauseKeyword(expression[0]) {
			return unquote(expression[0].Value), true
		}
	case 3:
		if expression[0].Type == lexer.TokenIdentifier && expression[1].MatchOperatorValue(".") &&
			expression[2].Type == lexer.TokenIdentifier {
			qualifier := unquote(expression[0].Value)
			if sameName(qualifier, source.alias) || sameName(qualifier, source.table) {
				return unquote(expression[2].Value), true
			}
		}
	}
	return "", false
}

// referencedColumns lists the columns an expression names.
//
// A token is a column when it is an identifier that is not a function name
// (not followed by an open parenthesis), not a qualifier, and not a keyword.
func referencedColumns(expression []lexer.Token, source sourceRef) []string {
	var columns []string
	seen := make(map[string]bool)
	for i, token := range expression {
		if token.Type != lexer.TokenIdentifier || isClauseKeyword(token) {
			continue
		}
		if i+1 < len(expression) && expression[i+1].MatchOperatorValue("(") {
			continue // a function name
		}
		if i+1 < len(expression) && expression[i+1].MatchOperatorValue(".") {
			continue // a qualifier; the part after the dot is the column
		}
		name := unquote(token.Value)
		if i > 0 && expression[i-1].MatchOperatorValue(".") {
			qualifier := unquote(expression[i-2].Value)
			if !sameName(qualifier, source.alias) && !sameName(qualifier, source.table) {
				continue
			}
		}
		if isLiteralKeyword(token) || seen[lowerName(name)] {
			continue
		}
		seen[lowerName(name)] = true
		columns = append(columns, name)
	}
	return columns
}

func isLiteralKeyword(token lexer.Token) bool {
	return slices.ContainsFunc([]string{"null", "true", "false", "case", "when", "then", "else", "end",
		"and", "or", "not", "distinct", "cast", "as", "interval"}, token.MatchIdentifierValue)
}
