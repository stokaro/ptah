package atlasschema

import (
	"strings"

	"ptah.run/internal/lexer"
	"ptah.run/internal/sqlreach"
)

// triggerRowReferences returns the token positions where a statement that
// creates a MySQL or MariaDB trigger names the row the trigger fires for: the
// NEW or OLD in front of a column, as in NEW.name. Such a qualifier is not a
// database, so the dev-database rehearsal must neither rewrite it nor refuse
// it (stokaro/ptah#4016).
//
// The servers decide by position, not by spelling. Measured on MySQL 8.4.11
// and MariaDB 11.8.9 inside a trigger body: NEW.b, new.b and `NEW`.b all name
// the row, while INSERT INTO new.audit writes to a database named new and
// new.audit.v is a column of that database's table. So a NEW or OLD qualifier
// is a row reference only where a value is expected: a path of exactly two
// parts that does not stand where a table name is read, which is right after
// FROM, JOIN, INTO, UPDATE or TABLE, or after a comma in a table list one of
// them opened. Everything else stays a qualifier and is checked as one.
//
// It answers nil for any statement that is not CREATE TRIGGER, so NEW and OLD
// outside a trigger body are ordinary database names.
func triggerRowReferences(tokens []lexer.Token) map[int]bool {
	body := triggerBodyStart(tokens)
	if body < 0 {
		return nil
	}
	references := make(map[int]bool)
	// openLists records, per parenthesis depth, whether a table list is open
	// there, so a derived table inside FROM does not close the list around it.
	openLists := make(map[int]bool)
	depth := 0
	expectTable := false
	for i := body; i < len(tokens); i++ {
		token := tokens[i]
		switch {
		case token.MatchOperatorValue("("):
			depth++
			expectTable = false
			continue
		case token.MatchOperatorValue(")"):
			delete(openLists, depth)
			depth = max(depth-1, 0)
			expectTable = false
			continue
		case token.Type == lexer.TokenSemicolon:
			clear(openLists)
			expectTable = false
			continue
		case token.MatchOperatorValue(","):
			expectTable = openLists[depth]
			continue
		case opensTableList(token):
			openLists[depth] = true
			expectTable = true
			continue
		case closesTableList(token):
			delete(openLists, depth)
		}
		if !expectTable && isRowQualifier(tokens, i) {
			references[i] = true
		}
		expectTable = false
	}
	return references
}

// triggerBodyStart returns the index of the first body token of a
// CREATE [OR REPLACE] [DEFINER = user] TRIGGER [IF NOT EXISTS] statement, or
// -1 when the statement creates something else. The body follows FOR EACH ROW.
func triggerBodyStart(tokens []lexer.Token) int {
	if len(tokens) == 0 || !sqlreach.IsKeyword(tokens[0], "CREATE") {
		return -1
	}
	position := 1
	if position+1 < len(tokens) && sqlreach.IsKeyword(tokens[position], "OR") &&
		sqlreach.IsKeyword(tokens[position+1], "REPLACE") {
		position += 2
	}
	if position < len(tokens) && sqlreach.IsKeyword(tokens[position], "DEFINER") {
		// A definer is spelled in several ways (CURRENT_USER, CURRENT_USER(),
		// 'user'@'host'), all of them short; the trigger keyword follows it.
		limit := min(position+8, len(tokens))
		for position < limit && !sqlreach.IsKeyword(tokens[position], "TRIGGER") {
			position++
		}
	}
	if position >= len(tokens) || !sqlreach.IsKeyword(tokens[position], "TRIGGER") {
		return -1
	}
	for i := position + 1; i+2 < len(tokens); i++ {
		if !sqlreach.IsKeyword(tokens[i], "FOR") || !sqlreach.IsKeyword(tokens[i+1], "EACH") ||
			!sqlreach.IsKeyword(tokens[i+2], "ROW") {
			continue
		}
		// A FOLLOWS or PRECEDES clause may come first; its two tokens neither
		// open a table list nor qualify anything, so they are scanned as body.
		return i + 3
	}
	return -1
}

// opensTableList reports a keyword after which the server reads a table name.
func opensTableList(token lexer.Token) bool {
	for _, keyword := range []string{"FROM", "JOIN", "STRAIGHT_JOIN", "INTO", "UPDATE", "TABLE"} {
		if sqlreach.IsKeyword(token, keyword) {
			return true
		}
	}
	return false
}

// closesTableList reports a keyword that ends the table list of its clause.
func closesTableList(token lexer.Token) bool {
	for _, keyword := range []string{
		"WHERE", "SET", "ON", "USING", "GROUP", "HAVING", "ORDER", "LIMIT", "VALUES", "VALUE",
		"SELECT", "UNION", "WINDOW", "RETURNING",
	} {
		if sqlreach.IsKeyword(token, keyword) {
			return true
		}
	}
	return false
}

// isRowQualifier reports whether tokens[i] heads a two-part path NEW.x or
// OLD.x, bare or quoted, that is not the tail of a longer path.
func isRowQualifier(tokens []lexer.Token, i int) bool {
	if tokens[i].Type != lexer.TokenIdentifier {
		return false
	}
	name := unquoteSchemaName(tokens[i])
	if !strings.EqualFold(name, "NEW") && !strings.EqualFold(name, "OLD") {
		return false
	}
	if i+2 >= len(tokens) || !tokens[i+1].MatchOperatorValue(".") || tokens[i+2].Type != lexer.TokenIdentifier {
		return false
	}
	if i > 0 && tokens[i-1].MatchOperatorValue(".") {
		return false
	}
	return i+3 >= len(tokens) || !tokens[i+3].MatchOperatorValue(".")
}
