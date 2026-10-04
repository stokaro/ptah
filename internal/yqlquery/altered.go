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
// an escape other than an escaped backtick or backslash, and when a PRAGMA
// heads any statement of the query, since `PRAGMA TablePathPrefix` changes
// what a relative name resolves to.
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
// backticked one with its quoting removed.
func tableName(token lexer.Token) (string, bool) {
	value := token.Value
	if token.Type != lexer.TokenIdentifier || value == "" || strings.HasPrefix(value, "$") {
		return "", false
	}
	if !strings.HasPrefix(value, "`") {
		return value, true
	}
	if len(value) < 2 || !strings.HasSuffix(value, "`") {
		return "", false
	}
	return unquoteBackticked(value[1 : len(value)-1])
}

// unquoteBackticked removes the escaping inside a backticked YQL name: a
// doubled backtick and a backslash before a backtick or a backslash each stand
// for the character. Any other backslash escape is a C escape the grammar
// reads, which this does not decode, so the name is not answered.
func unquoteBackticked(inner string) (string, bool) {
	var name strings.Builder
	for i := 0; i < len(inner); i++ {
		switch ch := inner[i]; {
		case ch == '`' && i+1 < len(inner) && inner[i+1] == '`':
			name.WriteByte('`')
			i++
		case ch == '\\' && i+1 < len(inner) && (inner[i+1] == '`' || inner[i+1] == '\\'):
			name.WriteByte(inner[i+1])
			i++
		case ch == '\\' || ch == '`':
			return "", false
		default:
			name.WriteByte(ch)
		}
	}
	return name.String(), true
}
