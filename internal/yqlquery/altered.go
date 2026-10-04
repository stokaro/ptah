package yqlquery

import (
	"strings"

	"ptah.run/internal/lexer"
)

// AlteredTable returns the table an ALTER TABLE query changes, as the query
// writes it: a path relative to the database root, or an absolute one.
//
// It answers only where the text alone settles the answer, and reports false
// otherwise: when text is not one query of one ALTER TABLE statement, when the
// table is a named expression (`ALTER TABLE $t ...`), when a quoted name holds
// an escape the server refuses, and when a PRAGMA heads any statement of the
// query, since `PRAGMA TablePathPrefix` changes what a relative name resolves
// to. A quoted name is decoded by [lexer.YQLIdentifierValue], the way the
// server reads it.
func AlteredTable(text string) (string, bool) {
	queries, err := Split(text)
	if err != nil || len(queries) != 1 {
		return "", false
	}
	query := queries[0]
	if query.Kind != Scheme || len(query.Statements) != 1 || setsAPragma(query.Text) {
		return "", false
	}
	tokens := significantTokens(query.Statements[0])
	if len(tokens) < 3 || !tokens[0].MatchIdentifierValue("ALTER") || !tokens[1].MatchIdentifierValue("TABLE") {
		return "", false
	}
	return tableName(tokens[2])
}

// setsAPragma reports whether a statement of text starts with PRAGMA.
func setsAPragma(text string) bool {
	starts := true
	for _, token := range significantTokens(text) {
		if starts && token.MatchIdentifierValue("PRAGMA") {
			return true
		}
		starts = token.Type == lexer.TokenSemicolon
	}
	return false
}

// tableName reads the name a token writes: a bare identifier as it is, and a
// backticked one decoded the way the server reads it.
func tableName(token lexer.Token) (string, bool) {
	value := token.Value
	if token.Type != lexer.TokenIdentifier || value == "" || strings.HasPrefix(value, "$") {
		return "", false
	}
	return lexer.YQLIdentifierValue(value)
}
