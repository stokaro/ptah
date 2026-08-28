package schemalineage

import (
	"strings"

	"go.5x5.cz/ptah/internal/lexer"
)

// writeTargetRef is the table a statement writes and the columns it names.
//
// An empty Columns slice means the statement names the table alone, which is
// the whole table rather than an unknown column.
type writeTargetRef struct {
	statement string
	table     string
	columns   []string
}

// writes renders the target as the rows a caller reports.
func (t writeTargetRef) writes(routine, kind string) []RoutineWrite {
	if len(t.columns) == 0 {
		return []RoutineWrite{{
			Table: t.table, ByRoutine: routine, Kind: kind, Statement: t.statement,
		}}
	}
	rows := make([]RoutineWrite, 0, len(t.columns))
	for _, column := range t.columns {
		rows = append(rows, RoutineWrite{
			Table: t.table, Column: column, ByRoutine: routine, Kind: kind, Statement: t.statement,
		})
	}
	return rows
}

// writeTarget reads the table and columns a writing statement names.
//
// It reports false for anything it cannot read rather than returning a partial
// target: a write attributed to the wrong table is worse than one reported as
// unresolved, which is the rule the view half already follows.
func writeTarget(leading string, tokens []lexer.Token) (writeTargetRef, bool) {
	switch leading {
	case "INSERT":
		return insertTarget(tokens)
	case "UPDATE":
		return updateTarget(tokens)
	case "DELETE":
		return deleteTarget(tokens)
	case "TRUNCATE":
		return truncateTarget(tokens)
	default:
		return writeTargetRef{}, false
	}
}

// insertTarget reads `INSERT INTO table [(columns)]`.
func insertTarget(tokens []lexer.Token) (writeTargetRef, bool) {
	if len(tokens) < 3 || !equalFold(tokens[1].Value, "INTO") {
		return writeTargetRef{}, false
	}
	table, next, err := qualifiedName(tokens[2:])
	if err != nil {
		return writeTargetRef{}, false
	}
	target := writeTargetRef{statement: "insert", table: table}
	rest := tokens[2+next:]
	if len(rest) == 0 || rest[0].Value != "(" {
		return target, true
	}
	columns, ok := parenthesizedNames(rest)
	if !ok {
		return writeTargetRef{}, false
	}
	target.columns = columns
	return target, true
}

// updateTarget reads `UPDATE table SET column = ...`.
func updateTarget(tokens []lexer.Token) (writeTargetRef, bool) {
	if len(tokens) < 2 {
		return writeTargetRef{}, false
	}
	table, next, err := qualifiedName(tokens[1:])
	if err != nil {
		return writeTargetRef{}, false
	}
	rest := tokens[1+next:]
	setIdx := indexOfKeyword(rest, "SET")
	if setIdx < 0 {
		return writeTargetRef{}, false
	}
	columns, ok := assignedColumns(rest[setIdx+1:])
	if !ok {
		return writeTargetRef{}, false
	}
	return writeTargetRef{statement: "update", table: table, columns: columns}, true
}

// deleteTarget reads `DELETE FROM table`.
func deleteTarget(tokens []lexer.Token) (writeTargetRef, bool) {
	if len(tokens) < 3 || !equalFold(tokens[1].Value, "FROM") {
		return writeTargetRef{}, false
	}
	table, _, err := qualifiedName(tokens[2:])
	if err != nil {
		return writeTargetRef{}, false
	}
	return writeTargetRef{statement: "delete", table: table}, true
}

// truncateTarget reads `TRUNCATE [TABLE] table`, for one table only.
//
// TRUNCATE accepts a list, and a list is refused rather than half-read: the
// second name would be silently dropped, which is a table reported as untouched
// while the statement empties it.
func truncateTarget(tokens []lexer.Token) (writeTargetRef, bool) {
	rest := tokens[1:]
	if len(rest) > 0 && equalFold(rest[0].Value, "TABLE") {
		rest = rest[1:]
	}
	if len(rest) == 0 {
		return writeTargetRef{}, false
	}
	table, next, err := qualifiedName(rest)
	if err != nil {
		return writeTargetRef{}, false
	}
	if hasMoreNames(rest[next:]) {
		return writeTargetRef{}, false
	}
	return writeTargetRef{statement: "truncate", table: table}, true
}

// hasMoreNames reports whether a comma follows, which means a name list.
func hasMoreNames(tokens []lexer.Token) bool {
	return len(tokens) > 0 && tokens[0].Value == ","
}

// parenthesizedNames reads `( a, b, c )` starting at the opening parenthesis.
func parenthesizedNames(tokens []lexer.Token) ([]string, bool) {
	var names []string
	expectName := true
	for index := 1; index < len(tokens); index++ {
		token := tokens[index]
		if token.Value == ")" {
			return names, !expectName
		}
		if token.Value == "," {
			expectName = true
			continue
		}
		if !expectName || token.Type != lexer.TokenIdentifier {
			return nil, false
		}
		names = append(names, unquote(token.Value))
		expectName = false
	}
	return nil, false
}

// assignedColumns reads the column names a SET clause assigns to.
//
// The parenthesized form, `SET (a, b) = (...)`, is refused: reading its names
// as ordinary columns would work, and reading the right-hand tuple as more of
// them would not, so the whole statement goes unresolved instead.
func assignedColumns(tokens []lexer.Token) ([]string, bool) {
	if len(tokens) == 0 || tokens[0].Value == "(" {
		return nil, false
	}
	var names []string
	depth := 0
	expectName := true
	for index := range tokens {
		token := tokens[index]
		depth = trackDepth(token, depth)
		if depth > 0 {
			continue
		}
		if endsSetClause(token, depth) {
			break
		}
		if token.Value == "," {
			expectName = true
			continue
		}
		if expectName && token.Type == lexer.TokenIdentifier {
			names = append(names, unquote(token.Value))
			expectName = false
		}
	}
	return names, len(names) > 0
}

// trackDepth follows parenthesis nesting so a function call in an assigned
// expression does not look like a column list.
func trackDepth(token lexer.Token, depth int) int {
	if token.Value == "(" {
		return depth + 1
	}
	if token.Value == ")" {
		return depth - 1
	}
	return depth
}

// endsSetClause reports whether a top-level token ends the assignments.
func endsSetClause(token lexer.Token, depth int) bool {
	if depth != 0 || token.Type != lexer.TokenIdentifier {
		return false
	}
	switch strings.ToUpper(unquote(token.Value)) {
	case "WHERE", "FROM", "RETURNING":
		return true
	default:
		return false
	}
}

// indexOfKeyword finds a top-level keyword, ignoring one inside parentheses.
func indexOfKeyword(tokens []lexer.Token, keyword string) int {
	depth := 0
	for index, token := range tokens {
		depth = trackDepth(token, depth)
		if depth == 0 && token.Type == lexer.TokenIdentifier && equalFold(token.Value, keyword) {
			return index
		}
	}
	return -1
}

// equalFold compares a token's text with a keyword, ignoring quoting and case.
func equalFold(value, keyword string) bool {
	return strings.EqualFold(unquote(value), keyword)
}
