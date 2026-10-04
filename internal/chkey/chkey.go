// Package chkey reads which columns a ClickHouse table's primary key uses, from
// the key clauses a declaration states, the way the server reports it in
// system.columns.is_in_primary_key.
//
// The comparison needs both sides to answer that question alike. The reader
// takes the server's flag; a SQL, YAML or annotation declaration states the
// key as an engine clause, PRIMARY KEY or ORDER BY, rather than on a column.
package chkey

import (
	"strings"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
)

// PrimaryKeyColumns returns the columns of table that its declared ClickHouse
// primary key uses, and whether the declaration states a key clause at all.
//
// The primary key is the PRIMARY KEY clause, or the ORDER BY clause when there
// is none, as ClickHouse defines it. A column counts when the clause names it,
// directly or inside an expression: measured on 24.10 and 26.9, ORDER BY
// (toDate(ts), id) marks both ts and id with is_in_primary_key, and PRIMARY KEY
// id ORDER BY (id, n) marks id alone. A name followed by an opening
// parenthesis is a function, not a column.
//
// A table that states neither clause answers false: its key, if any, is the
// column-level one the declaration states on its fields.
func PrimaryKeyColumns(table schemamodel.Table, columns []string) (map[string]bool, bool) {
	overrides := table.Overrides[platform.ClickHouse]
	clause := strings.TrimSpace(overrides["primary_key"])
	if clause == "" {
		clause = strings.TrimSpace(overrides["order_by"])
	}
	if clause == "" {
		return nil, false
	}
	known := make(map[string]bool, len(columns))
	for _, column := range columns {
		known[column] = true
	}
	used := make(map[string]bool)
	tokens := significantTokens(clause)
	for i, token := range tokens {
		if !isIdentifier(token) {
			continue
		}
		if i+1 < len(tokens) && tokens[i+1].MatchOperatorValue("(") {
			continue
		}
		if name := unquote(token.Value); known[name] {
			used[name] = true
		}
	}
	return used, true
}

// significantTokens lexes clause as ClickHouse SQL, without whitespace and
// comments.
func significantTokens(clause string) []lexer.Token {
	lex := lexer.NewLexerWithOptions(clause, dialectlexer.Options(platform.ClickHouse))
	var tokens []lexer.Token
	for {
		token := lex.NextToken()
		switch token.Type {
		case lexer.TokenEOF:
			return tokens
		case lexer.TokenWhitespace, lexer.TokenComment:
			continue
		}
		tokens = append(tokens, token)
	}
}

// isIdentifier reports whether token names something: a bare or backticked
// identifier, or a double-quoted one, which the lexer reads as a string and
// ClickHouse reads as an identifier.
func isIdentifier(token lexer.Token) bool {
	return token.Type == lexer.TokenIdentifier ||
		token.Type == lexer.TokenString && strings.HasPrefix(token.Value, `"`)
}

// unquote removes the backticks or double quotes a ClickHouse identifier may
// carry.
func unquote(name string) string {
	for _, quote := range []string{"`", `"`} {
		if len(name) >= 2 && strings.HasPrefix(name, quote) && strings.HasSuffix(name, quote) {
			return strings.ReplaceAll(name[1:len(name)-1], quote+quote, quote)
		}
	}
	return name
}
