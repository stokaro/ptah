package atlasschema

import (
	"slices"
	"strings"

	"ptah.run/internal/lexer"
	"ptah.run/internal/sqlreach"
)

// columnQualifiers returns the token positions, inside the body of a view or a
// trigger, of a qualifier that names something the body binds rather than a
// database: the u in u.id after FROM users u, and the NEW or OLD in front of a
// trigger's row column. The dev-database rehearsal must neither rewrite nor
// refuse such a qualifier (stokaro/ptah#4016, stokaro/ptah#4027).
//
// The servers decide by position. Measured on MySQL 8.4.11 and MariaDB 11.8.9:
// a two-part path in an expression is a table's column and never a table in
// another database, so SELECT other.t FROM t fails with ERROR 1054 although a
// database other holds a table t, and an alias named like a database shadows
// it. Other positions do reach another database, and every one of them stays a
// qualifier here: a table name after FROM, JOIN, INTO, UPDATE, TABLE or CALL,
// or in the comma list one of them opened; the routine in other.f(); the
// sequence in NEXTVAL(other.s) and NEXT VALUE FOR other.s; and the head of a
// three-part path such as other.t.v.
//
// Only a name the body binds is exempted -- a table or alias it reads, NEW and
// OLD in a trigger -- so a position this scan misreads costs a refusal, never a
// rehearsal outside the dev database. A statement that is not CREATE VIEW,
// ALTER VIEW or CREATE TRIGGER answers nil, and so does the head of one: CREATE
// VIEW u.v names a database u whatever the body binds.
func columnQualifiers(tokens []lexer.Token) map[int]bool {
	body, trigger := bodyStart(tokens)
	if body < 0 {
		return nil
	}
	tablePositions, bound := readTables(tokens, body)
	if trigger {
		bound["new"] = true
		bound["old"] = true
	}
	qualifiers := make(map[int]bool)
	for i := body; i < len(tokens); i++ {
		if !tablePositions[i] && isColumnPath(tokens, i) && bound[boundKey(tokens[i])] {
			qualifiers[i] = true
		}
	}
	return qualifiers
}

// bodyStart returns the index of the first body token of a view or a trigger
// definition, and whether it is a trigger, or -1. A view body follows the AS
// after VIEW; a trigger body follows FOR EACH ROW and an optional FOLLOWS or
// PRECEDES clause, whose two tokens neither read a table nor qualify anything.
func bodyStart(tokens []lexer.Token) (int, bool) {
	if len(tokens) == 0 || (!sqlreach.IsKeyword(tokens[0], "CREATE") && !sqlreach.IsKeyword(tokens[0], "ALTER")) {
		return -1, false
	}
	// The object kind follows a short header: OR REPLACE, ALGORITHM = x,
	// DEFINER = user, SQL SECURITY x.
	for kind := 1; kind < min(len(tokens), 16); kind++ {
		if sqlreach.IsKeyword(tokens[kind], "VIEW") {
			return indexAfter(tokens, kind+1, "AS"), false
		}
		if sqlreach.IsKeyword(tokens[kind], "TRIGGER") && sqlreach.IsKeyword(tokens[0], "CREATE") {
			return indexAfter(tokens, kind+1, "FOR", "EACH", "ROW"), true
		}
		if isOtherObjectKind(tokens[kind]) {
			return -1, false
		}
	}
	return -1, false
}

// indexAfter returns the index after the first occurrence of keywords at or
// after start, or -1.
func indexAfter(tokens []lexer.Token, start int, keywords ...string) int {
	for i := start; i+len(keywords) <= len(tokens); i++ {
		matched := true
		for offset, keyword := range keywords {
			if !sqlreach.IsKeyword(tokens[i+offset], keyword) {
				matched = false
				break
			}
		}
		if matched {
			return i + len(keywords)
		}
	}
	return -1
}

func isOtherObjectKind(token lexer.Token) bool {
	return slices.ContainsFunc([]string{"TABLE", "PROCEDURE", "FUNCTION", "EVENT", "INDEX", "SCHEMA", "DATABASE"},
		func(keyword string) bool { return sqlreach.IsKeyword(token, keyword) })
}

// readTables scans a body for the token positions where a name is read as a
// table, a routine or a sequence, and the names the body binds: each table it
// reads, under its own name and its alias, and each derived table's alias.
func readTables(tokens []lexer.Token, body int) (positions map[int]bool, bound map[string]bool) {
	positions = make(map[int]bool)
	bound = make(map[string]bool)
	// openLists records, per parenthesis depth, whether a table list is open
	// there, so a derived table inside FROM does not close the list around it.
	openLists := make(map[int]bool)
	derived := make(map[int]bool)
	depth := 0
	expectTable := false
	for i := body; i < len(tokens); i++ {
		token := tokens[i]
		switch {
		case token.MatchOperatorValue("("):
			derived[depth+1] = expectTable
			if i > 0 && isSequenceFunction(tokens[i-1]) {
				markPath(tokens, i+1, positions)
			}
			depth++
			expectTable = false
			continue
		case token.MatchOperatorValue(")"):
			closedDerived := derived[depth]
			delete(derived, depth)
			delete(openLists, depth)
			depth = max(depth-1, 0)
			expectTable = false
			if closedDerived {
				i = bindAlias(tokens, i+1, bound) - 1
			}
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
		case sqlreach.IsKeyword(token, "FOR") && i > 0 && sqlreach.IsKeyword(tokens[i-1], "VALUE"):
			markPath(tokens, i+1, positions)
			continue
		case closesTableList(token):
			delete(openLists, depth)
		}
		if expectTable && isSchemaNameToken(token) {
			end := markPath(tokens, i, positions)
			bound[boundKey(tokens[end-1])] = true
			i = bindAlias(tokens, end, bound) - 1
		}
		expectTable = false
	}
	return positions, bound
}

// markPath records every part of the dotted path starting at start as a
// position where a name is read, and returns the index after it.
func markPath(tokens []lexer.Token, start int, positions map[int]bool) int {
	end := start
	for end < len(tokens) && isSchemaNameToken(tokens[end]) {
		positions[end] = true
		if end+2 >= len(tokens) || !tokens[end+1].MatchOperatorValue(".") {
			return end + 1
		}
		end += 2
	}
	return end
}

// bindAlias records the alias at start, written with or without AS, and
// returns the index after it.
func bindAlias(tokens []lexer.Token, start int, bound map[string]bool) int {
	if start < len(tokens) && sqlreach.IsKeyword(tokens[start], "AS") {
		start++
	}
	if start < len(tokens) && tokens[start].Type == lexer.TokenIdentifier && !isClauseKeyword(tokens[start]) {
		bound[boundKey(tokens[start])] = true
		return start + 1
	}
	return start
}

// isColumnPath reports whether tokens[i] heads a two-part path x.y or x.*
// that is neither the tail of a longer path, the head of a three-part one, nor
// a routine call.
func isColumnPath(tokens []lexer.Token, i int) bool {
	if tokens[i].Type != lexer.TokenIdentifier || i+2 >= len(tokens) || !tokens[i+1].MatchOperatorValue(".") {
		return false
	}
	if tokens[i+2].Type != lexer.TokenIdentifier && !tokens[i+2].MatchOperatorValue("*") {
		return false
	}
	if i > 0 && tokens[i-1].MatchOperatorValue(".") {
		return false
	}
	return i+3 >= len(tokens) || (!tokens[i+3].MatchOperatorValue(".") && !tokens[i+3].MatchOperatorValue("("))
}

// boundKey folds a name for the one question asked of it: did the body bind
// it? A false match exempts a two-part path in an expression, which is a
// column wherever it stands.
func boundKey(token lexer.Token) string {
	return strings.ToLower(unquoteSchemaName(token))
}

// opensTableList reports a keyword after which the server reads a table or,
// for CALL, a routine.
func opensTableList(token lexer.Token) bool {
	return slices.ContainsFunc([]string{"FROM", "JOIN", "STRAIGHT_JOIN", "INTO", "UPDATE", "TABLE", "CALL"},
		func(keyword string) bool { return sqlreach.IsKeyword(token, keyword) })
}

// closesTableList reports a keyword that ends the table list of its clause.
func closesTableList(token lexer.Token) bool {
	return slices.ContainsFunc([]string{
		"WHERE", "SET", "ON", "USING", "GROUP", "HAVING", "ORDER", "LIMIT", "VALUES", "VALUE",
		"SELECT", "UNION", "WINDOW", "RETURNING",
	}, func(keyword string) bool { return sqlreach.IsKeyword(token, keyword) })
}

// isClauseKeyword reports a keyword that can follow a table reference and is
// therefore not its alias.
func isClauseKeyword(token lexer.Token) bool {
	return closesTableList(token) || opensTableList(token) || slices.ContainsFunc([]string{
		"INNER", "LEFT", "RIGHT", "CROSS", "NATURAL", "FULL", "OUTER", "FOR", "LOCK", "PARTITION",
		"USE", "FORCE", "IGNORE", "EXCEPT", "INTERSECT", "WITH", "AS", "END", "THEN", "ELSE",
	}, func(keyword string) bool { return sqlreach.IsKeyword(token, keyword) })
}

// isSequenceFunction reports a MariaDB function whose argument is a sequence.
func isSequenceFunction(token lexer.Token) bool {
	return slices.ContainsFunc([]string{"NEXTVAL", "SETVAL", "LASTVAL"},
		func(keyword string) bool { return sqlreach.IsKeyword(token, keyword) })
}
