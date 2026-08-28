package schemalineage

import (
	"go.5x5.cz/ptah/internal/lexer"
)

// readSourceRef is the one table a statement reads, the word naming what kind
// of read it is, and the token positions that are written rather than read.
type readSourceRef struct {
	table     string
	statement string
	// written holds the indexes of assignment targets. In `UPDATE t SET c = 1`
	// the column c is written and not read, and the two roles are told apart by
	// position rather than by name -- `SET c = c + 1` has both.
	written map[int]bool
}

// readSource resolves what a statement reads, or reports that it could not.
//
// It reports false for anything with more than one table in scope. That is the
// same line the view half draws, and for the same reason: an unqualified column
// cannot be attributed when two tables could own it.
func readSource(tokens []lexer.Token) (readSourceRef, bool) {
	switch lowerRoutineWord(tokens) {
	case "SELECT", "PERFORM":
		return selectReadSource(tokens)
	case "UPDATE":
		return updateReadSource(tokens)
	case "DELETE":
		return deleteReadSource(tokens)
	default:
		return readSourceRef{}, false
	}
}

// selectReadSource resolves a SELECT or PERFORM over one source.
func selectReadSource(tokens []lexer.Token) (readSourceRef, bool) {
	_, fromStart, err := selectAndFrom(tokens)
	if err != nil {
		return readSourceRef{}, false
	}
	ref, err := singleSource(tokens, fromStart)
	if err != nil {
		return readSourceRef{}, false
	}
	return readSourceRef{table: ref.table, statement: "select"}, true
}

// updateReadSource resolves `UPDATE t SET ...` and marks the assigned columns.
//
// A statement with a FROM clause is refused: `UPDATE t SET c = u.v FROM u`
// brings a second table into scope, and an unqualified column stops being
// attributable to either.
func updateReadSource(tokens []lexer.Token) (readSourceRef, bool) {
	if len(tokens) < 2 {
		return readSourceRef{}, false
	}
	table, next, err := qualifiedName(tokens[1:])
	if err != nil {
		return readSourceRef{}, false
	}
	rest := tokens[1+next:]
	setIdx := indexOfKeyword(rest, "SET")
	if setIdx < 0 || indexOfKeyword(rest, "FROM") >= 0 {
		return readSourceRef{}, false
	}
	offset := 1 + next + setIdx + 1
	targets := assignedColumnPositions(rest[setIdx+1:])
	written := make(map[int]bool, len(targets))
	for _, position := range targets {
		written[offset+position] = true
	}
	return readSourceRef{table: table, statement: "update", written: written}, true
}

// deleteReadSource resolves `DELETE FROM t`.
//
// A USING clause is refused for the reason an UPDATE's FROM is: it puts a
// second table in scope.
func deleteReadSource(tokens []lexer.Token) (readSourceRef, bool) {
	if len(tokens) < 3 || !equalFold(tokens[1].Value, "FROM") {
		return readSourceRef{}, false
	}
	table, _, err := qualifiedName(tokens[2:])
	if err != nil {
		return readSourceRef{}, false
	}
	if indexOfKeyword(tokens, "USING") >= 0 || indexOfKeyword(tokens, "JOIN") >= 0 {
		return readSourceRef{}, false
	}
	return readSourceRef{table: table, statement: "delete"}, true
}

// assignedColumnPositions returns the token indexes of the columns a SET clause
// assigns to, relative to the tokens it is given.
//
// It walks the clause the way assignedColumns does, and returns positions
// instead of names because that is what separates the two roles a column can
// have in one statement: in `UPDATE t SET c = c + 1` the first c is written and
// the second is read.
func assignedColumnPositions(tokens []lexer.Token) []int {
	var positions []int
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
			positions = append(positions, index)
			expectName = false
		}
	}
	return positions
}
