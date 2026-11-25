package mysqlreplay

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/internal/devclean/replay"
	"ptah.run/internal/lexer"
)

// The readers here name the tables and other objects a statement writes,
// where the shared reader of [replay.MutationTargets] cannot: the spellings
// of an INSERT, a REPLACE and a DELETE they know, the tables an ALTER TABLE or
// a CREATE TABLE reaches besides its own, the tables a view writes through,
// and the stored functions and sequences a statement runs or advances in
// another database. Each spelling below was measured on MySQL 8.4.13 or
// MariaDB 11.8.9, or both, writing a table of another database.

// isKeyword reports whether token is an unquoted word among words, in either
// case. A quoted name is never a keyword, so `into`.t names a database.
func isKeyword(token lexer.Token, words ...string) bool {
	return slices.ContainsFunc(words, token.MatchIdentifierValue)
}

// isOperatorAt reports whether the token at index is the operator op.
func isOperatorAt(tokens []lexer.Token, index int, op string) bool {
	token := replay.TokenAt(tokens, index)
	return token.MatchOperatorValue(op)
}

// qualifiedNameAt returns the dotted name that begins at index as a list of
// one target, or nil where no name begins there.
func qualifiedNameAt(tokens []lexer.Token, index int) [][]string {
	name, _ := replay.QualifiedIdentifierAt(tokens, index)
	if len(name) == 0 {
		return nil
	}
	return [][]string{name}
}

// mysqlInsertTargets reads the table an INSERT or REPLACE writes, past its
// modifiers and an INTO the servers take as optional, as in INSERT
// LOW_PRIORITY IGNORE other.t VALUES (1) or REPLACE other.t VALUES (1).
func mysqlInsertTargets(tokens []lexer.Token) [][]string {
	index := 1
	for index < len(tokens) &&
		isKeyword(tokens[index], "LOW_PRIORITY", "DELAYED", "HIGH_PRIORITY", "IGNORE", "INTO", "TABLE") {
		index++
	}
	return qualifiedNameAt(tokens, index)
}

// mysqlDeleteTargets reads every table a DELETE names: the tables of its
// delete list and of its FROM and USING references, past the modifiers
// LOW_PRIORITY, QUICK and IGNORE. A multi-table DELETE names its targets by
// alias, as DELETE FROM x USING other.t AS x and DELETE t, o FROM t JOIN
// other.t AS o ON ... do, so a table the references name counts as written
// whether or not the delete list names it. A table the statement only reads
// in another database is refused with them, which costs a migration little: a
// dev server seldom holds that database.
//
// RETURNING ends the list after the FROM of a DELETE that has no delete list
// before it, where MariaDB reads it as its RETURNING clause: DELETE FROM t
// RETURNING t.id, t.name. Anywhere else it is an alias. MySQL has no such
// clause, and measured on MySQL 8.4.13 both DELETE x FROM t AS returning,
// other.t AS x and DELETE FROM x USING t returning, other.t AS x delete from
// other.t. MariaDB 11.8.9 refuses both, so reading on past the word refuses
// nothing it runs. The rule reads the words rather than the dialect, so it
// holds on whichever server a URL reaches; a mysql URL can reach MariaDB.
// MariaDB's HISTORY is not skipped as a modifier: to MySQL it is a name, and
// DELETE history FROM t AS returning, other.t AS history names a delete list.
func mysqlDeleteTargets(tokens []lexer.Token) [][]string {
	stops := []string{"FROM", "USING", "WHERE", "ORDER", "LIMIT"}
	listStops := stops
	first := 1
	for first < len(tokens) && isKeyword(tokens[first], "LOW_PRIORITY", "QUICK", "IGNORE") {
		first++
	}
	if isKeyword(replay.TokenAt(tokens, first), "FROM") {
		listStops = append(slices.Clone(stops), "RETURNING")
	}
	var targets [][]string
	for index := first; index < len(tokens); {
		if isKeyword(tokens[index], "FROM", "USING") {
			index++
		}
		list, next := mysqlTableReferenceTargets(tokens, index, listStops...)
		targets = append(targets, list...)
		if next >= len(tokens) || !isKeyword(tokens[next], "FROM", "USING") {
			return targets
		}
		index = next
		listStops = stops
	}
	return targets
}

// mysqlTableMaintenanceTargets reads every table an OPTIMIZE, ANALYZE or
// REPAIR TABLE names, past NO_WRITE_TO_BINLOG or LOCAL. OPTIMIZE rebuilds a
// table and REPAIR can drop rows a MyISAM table cannot read.
func mysqlTableMaintenanceTargets(tokens []lexer.Token) [][]string {
	index := 1
	for index < len(tokens) && isKeyword(tokens[index], "NO_WRITE_TO_BINLOG", "LOCAL") {
		index++
	}
	if index >= len(tokens) || !isKeyword(tokens[index], "TABLE", "TABLES") {
		return nil
	}
	return replay.CommaSeparatedQualifiedTargets(tokens, index+1, "")
}

// mysqlTableReach reads the tables an ALTER TABLE or CREATE TABLE writes
// besides its own, from start on: the destination of a RENAME, written with
// TO, AS or neither, as in ALTER TABLE t RENAME other.t2; the table an
// EXCHANGE PARTITION ... WITH TABLE swaps rows with; the tables of a MERGE
// table's UNION, which an INSERT into the MERGE table writes; and the table a
// MariaDB CONVERT PARTITION p TO TABLE creates from a partition's rows, or
// CONVERT TABLE moves into a partition, taking it out of its database.
// RENAME, UNION and CONVERT are reserved words and WITH TABLE ends an
// EXCHANGE, so none of them stands for a name inside a column list.
func mysqlTableReach(tokens []lexer.Token, start int) [][]string {
	var targets [][]string
	for index := start; index < len(tokens); index++ {
		switch {
		case isKeyword(tokens[index], "RENAME"):
			targets = append(targets, renameDestination(tokens, index+1)...)
		case isKeyword(tokens[index], "WITH") && isKeyword(replay.TokenAt(tokens, index+1), "TABLE"):
			targets = append(targets, qualifiedNameAt(tokens, index+2)...)
		case isKeyword(tokens[index], "CONVERT") && isKeyword(replay.TokenAt(tokens, index+1), "TABLE"):
			targets = append(targets, qualifiedNameAt(tokens, index+2)...)
		case isKeyword(tokens[index], "CONVERT") && isKeyword(replay.TokenAt(tokens, index+1), "PARTITION") &&
			replay.TokenSequenceAt(tokens, index+3, "TO", "TABLE"):
			targets = append(targets, qualifiedNameAt(tokens, index+5)...)
		case isKeyword(tokens[index], "UNION"):
			targets = append(targets, unionTables(tokens, index+1)...)
		}
	}
	return targets
}

// likeSource reads the table a CREATE TABLE ... LIKE copies, written LIKE src
// or (LIKE src) after the new table's name at index. The copy takes the
// source's whole definition: a MERGE table's UNION, or a FEDERATED, SPIDER or
// CONNECT table's connection. So CREATE TABLE m LIKE other.mrg followed by
// INSERT INTO m VALUES (77) writes a table of other.
func likeSource(tokens []lexer.Token, index int) [][]string {
	if isOperatorAt(tokens, index, "(") {
		index++
	}
	if !isKeyword(replay.TokenAt(tokens, index), "LIKE") {
		return nil
	}
	return qualifiedNameAt(tokens, index+1)
}

// referenceModifierEnd returns the index past an index hint or a MariaDB FOR
// SYSTEM_TIME clause that begins at index, after a table of a list of table
// references, and false where neither begins there:
//
//   - the words of an index hint before its list: USE, IGNORE or FORCE, then
//     INDEX or KEY, then FOR JOIN, FOR ORDER BY or FOR GROUP BY if it is
//     written. A reader passes over the list in parentheses after them as it
//     passes over any other, and hints follow one another without a comma;
//   - FOR SYSTEM_TIME and the word after it: ALL, AS OF, BETWEEN or FROM. The
//     expressions of AS OF, BETWEEN ... AND and FROM ... TO hold no word that
//     ends a list, so a reader goes on through them.
//
// Their words FOR, ORDER, GROUP and FROM end a list of references anywhere
// else. A reader that stopped at them would miss the references after the
// hint, as in FROM t FORCE INDEX FOR ORDER BY (PRIMARY), other.t AS o.
func referenceModifierEnd(tokens []lexer.Token, index int) (int, bool) {
	at := func(offset int) lexer.Token { return replay.TokenAt(tokens, index+offset) }
	switch {
	case isKeyword(at(0), "USE", "IGNORE", "FORCE") && isKeyword(at(1), "INDEX", "KEY"):
		end := index + 2
		switch {
		case isKeyword(at(2), "FOR") && isKeyword(at(3), "JOIN"):
			end += 2
		case isKeyword(at(2), "FOR") && isKeyword(at(3), "ORDER", "GROUP") && isKeyword(at(4), "BY"):
			end += 3
		}
		return end, true
	case isKeyword(at(0), "FOR") && isKeyword(at(1), "SYSTEM_TIME"):
		switch {
		case isKeyword(at(2), "ALL", "BETWEEN", "FROM"):
			return index + 3, true
		case isKeyword(at(2), "AS") && isKeyword(at(3), "OF"):
			return index + 4, true
		default:
			return index + 2, true
		}
	default:
		return index, false
	}
}

// renameDestination reads the new name of an ALTER TABLE ... RENAME clause
// whose words after RENAME begin at index. RENAME COLUMN, INDEX or KEY reads
// as a rename to a table of that name in the dev database, which the check
// passes.
func renameDestination(tokens []lexer.Token, index int) [][]string {
	if index < len(tokens) && isKeyword(tokens[index], "TO", "AS") {
		index++
	}
	return qualifiedNameAt(tokens, index)
}

// unionTables reads the list of a MERGE table's UNION [=] (t1, t2, ...) whose
// words after UNION begin at index. A UNION that a query writes, followed by
// a SELECT, ALL or DISTINCT or by a query in parentheses, names no tables.
func unionTables(tokens []lexer.Token, index int) [][]string {
	if index < len(tokens) && tokens[index].MatchOperatorValue("=") {
		index++
	}
	if index >= len(tokens) || !tokens[index].MatchOperatorValue("(") ||
		isKeyword(replay.TokenAt(tokens, index+1), "SELECT", "WITH", "VALUES", "TABLE") {
		return nil
	}
	var tables [][]string
	for index++; index < len(tokens) && !tokens[index].MatchOperatorValue(")"); index++ {
		if name, next := replay.QualifiedIdentifierAt(tokens, index); len(name) > 0 {
			tables = append(tables, name)
			index = next - 1
		}
	}
	return tables
}

// queryFrame is one level of parentheses in a query: query reports a level
// whose FROM and JOIN name tables, the statement itself or a subquery, as
// against the arguments of a function such as EXTRACT(YEAR FROM d); list
// reports a level inside a FROM list, where a comma begins another table.
type queryFrame struct {
	query, list bool
}

// mysqlQueryTables reads the tables a query names from start on: after FROM,
// a JOIN, a comma of a FROM list or the TABLE statement, in the query and in
// each subquery it holds. A write through a view lands in the tables its
// query names, so CREATE VIEW vw AS SELECT * FROM other.t followed by INSERT
// INTO vw VALUES (42) writes other.t. An index hint or a FOR SYSTEM_TIME
// clause after a table of a FROM list is read past whole
// ([referenceModifierEnd]), so the list goes on after it.
func mysqlQueryTables(tokens []lexer.Token, start int) [][]string {
	var tables [][]string
	frames := []queryFrame{{query: true}}
	expectTable := false
	for index := start; index < len(tokens); index++ {
		token := tokens[index]
		frame := &frames[len(frames)-1]
		if frame.query && frame.list && !expectTable {
			if end, ok := referenceModifierEnd(tokens, index); ok {
				index = end - 1
				continue
			}
		}
		switch {
		case token.MatchOperatorValue("("):
			next := replay.TokenAt(tokens, index+1)
			switch {
			case isKeyword(next, "SELECT", "WITH", "VALUES", "TABLE"):
				frames = append(frames, queryFrame{query: true})
			case expectTable:
				// A join in parentheses: the table it begins with comes next.
				frames = append(frames, queryFrame{query: true, list: true})
				continue
			default:
				frames = append(frames, queryFrame{})
			}
			expectTable = false
		case token.MatchOperatorValue(")"):
			if len(frames) > 1 {
				frames = frames[:len(frames)-1]
			}
			expectTable = false
		case !frame.query:
		case isKeyword(token, "FROM", "JOIN", "STRAIGHT_JOIN"):
			frame.list = true
			expectTable = true
		case isKeyword(token, "TABLE"):
			expectTable = true
		case frame.list && token.MatchOperatorValue(","):
			expectTable = true
		case isKeyword(token, "WHERE", "GROUP", "HAVING", "ORDER", "LIMIT", "UNION", "EXCEPT",
			"INTERSECT", "WINDOW", "INTO", "FOR", "LOCK"):
			frame.list = false
			expectTable = false
		case expectTable:
			if name, next := replay.QualifiedIdentifierAt(tokens, index); len(name) > 0 {
				tables = append(tables, name)
				index = next - 1
			}
			expectTable = false
		}
	}
	return tables
}

// rejectMySQLViewReach refuses a CREATE or ALTER VIEW whose query names a
// table of another database than database, and names the view and the table:
// an INSERT, UPDATE, DELETE or REPLACE through the view writes that table. A
// table the query only reads, in a WHERE subquery, is refused with them.
func rejectMySQLViewReach(dialect, database string, tokens []lexer.Token) error {
	if !isKeyword(tokens[0], "CREATE", "ALTER") {
		return nil
	}
	kindIndex := replay.StatementObjectKindIndex(tokens)
	if kindIndex == replay.MutationTargetNotFound || !isKeyword(tokens[kindIndex], "VIEW") {
		return nil
	}
	view, next := replay.QualifiedIdentifierAt(tokens, replay.ObjectNameIndex(tokens, kindIndex))
	for _, table := range mysqlQueryTables(tokens, next) {
		if len(table) < 2 || table[0] == database {
			continue
		}
		return replay.Refuse(dialect, fmt.Sprintf("view %q over cross-database table %q",
			strings.Join(view, "."), strings.Join(table, ".")))
	}
	return nil
}

// mysqlRoutineReach reads the stored functions and sequences of another
// database that a statement runs or advances, anywhere in it:
//
//   - a function named by its database and followed by its arguments, as in
//     SELECT other.f(), DO other.f() or SET @x = other.f(). A stored function
//     writes what its body writes, in the function's database or any other;
//   - a MariaDB sequence given to NEXTVAL or SETVAL, named after NEXT VALUE
//     FOR, or written seq.NEXTVAL as Oracle mode does, each of which advances
//     or sets the sequence.
//
// A name followed by parentheses after REFERENCES is the parent table of a
// foreign key with its columns, which the statement reads. Anywhere else a
// qualified name with parentheses is a call, or the statement's own target
// with its columns, which [mysqlMutationTargets] names first.
func mysqlRoutineReach(tokens []lexer.Token) [][]string {
	var targets [][]string
	for index := 0; index < len(tokens); index++ {
		switch {
		case isKeyword(tokens[index], "NEXTVAL", "SETVAL") &&
			isOperatorAt(tokens, index+1, "("):
			targets = append(targets, qualifiedNameAt(tokens, index+2)...)
			continue
		case replay.TokenSequenceAt(tokens, index, "NEXT", "VALUE", "FOR"):
			targets = append(targets, qualifiedNameAt(tokens, index+3)...)
			continue
		}
		name, next := replay.QualifiedIdentifierAt(tokens, index)
		switch {
		case len(name) == 3 && isKeyword(tokens[next-1], "NEXTVAL"):
			targets = append(targets, name[:2])
		case len(name) >= 2 && isOperatorAt(tokens, next, "(") &&
			(index == 0 || !isKeyword(tokens[index-1], "REFERENCES")):
			targets = append(targets, name)
		}
		if len(name) > 0 {
			index = next - 1
		}
	}
	return targets
}
