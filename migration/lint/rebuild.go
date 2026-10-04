package lint

import (
	"slices"
	"strings"

	"ptah.run/internal/yqlddl"
)

// A table rebuild replaces a table with a copy that holds its rows: a new
// table is created, every row of the old one is copied into it, and the copy
// takes the old table's name while the old table is dropped. Ptah's planners
// write one where an engine cannot change a table in place -- SQLite's
// rebuild for what its ALTER TABLE cannot express, and YDB's
// --allow-table-rebuild plan for a key, type or NOT NULL change -- in one of
// two orders, each step a statement of its own:
//
//	SQLite:  CREATE TABLE s; INSERT INTO s ... SELECT ... FROM t; DROP TABLE t; ALTER TABLE s RENAME TO t
//	YDB:     CREATE TABLE s; INSERT INTO s ... SELECT ... FROM t; ALTER TABLE t RENAME TO r;
//	         ALTER TABLE s RENAME TO t; DROP TABLE r
//
// The rows of t are in the copy, and the copy is t once the file has run, so
// the DROP TABLE loses no row (DS101), and the name deployed code reads comes
// back before the file ends (BC101, BC103). A DROP TABLE outside that shape is
// a drop, and is reported as one.
//
// The shape is matched strictly, as consecutive statements: a write to t
// between the copy and the drop would be lost, a copy with a WHERE, a join,
// DISTINCT or anything after its FROM leaves rows behind, and a copy into a
// table other than the one created just before it is not a rebuild. A column
// the copy leaves out is lost too, which the text alone cannot see: where the
// old table's columns are known -- from the directory's own history on YDB,
// from the dev database elsewhere -- a copy that leaves one out is not a
// rebuild, and DS101 reports the drop.
//
// Only the native surface recognizes it: the compatibility surface reports
// what the analyzer it is compatible with reports, which was not measured for
// this shape.

// rebuildSteps are the statements of a file that belong to a table rebuild.
type rebuildSteps struct {
	// drops are the DROP TABLE statements that retire a replaced table whose
	// rows a rebuild copied.
	drops map[int]bool
	// renames are the ALTER TABLE ... RENAME TO statements that move a
	// replaced table aside or move its copy into place.
	renames map[int]bool
}

// fileRebuilds finds the table rebuilds of an up migration on the native
// surface.
func fileRebuilds(file *File) rebuildSteps {
	steps := rebuildSteps{drops: make(map[int]bool), renames: make(map[int]bool)}
	if !file.IsUp || file.compatibility == CompatibilityProfileAtlas {
		return steps
	}
	var ydbState *ydbSchema
	if ydbRun(file.Target) {
		ydbState = file.ydbBefore.clone()
	}
	for i := range file.Statements {
		if rebuild, ok := matchRebuild(file, i, ydbState); ok {
			steps.drops[rebuild.drop] = true
			for _, index := range rebuild.renames {
				steps.renames[index] = true
			}
		}
		if ydbState != nil {
			ydbState.apply(yqlddl.Read(file.Statements[i].SQL))
		}
	}
	return steps
}

// rebuildMatch is one rebuild: the statement that drops the replaced table and
// the renames that swap the copy in.
type rebuildMatch struct {
	drop    int
	renames []int
}

// matchRebuild reports whether the statements from index on are a rebuild,
// in either order. ydbState is the YDB schema before the statement at index,
// nil on every other dialect.
func matchRebuild(file *File, index int, ydbState *ydbSchema) (rebuildMatch, bool) {
	statements := file.Statements
	if index+3 >= len(statements) {
		return rebuildMatch{}, false
	}
	scratch := createdTableRef(statements[index].Words)
	copied, ok := tableCopyOf(&statements[index+1])
	if scratch == "" || !ok || !sameTableRef(copied.into, scratch) || sameTableRef(copied.from, scratch) {
		return rebuildMatch{}, false
	}
	old := copied.from
	if len(lostColumns(file, old, copied, ydbState)) > 0 {
		return rebuildMatch{}, false
	}
	// SQLite's order: the old table is dropped, then the copy takes its name.
	if dropped, ok := singleDroppedTable(&statements[index+2]); ok && sameTableRef(dropped, old) {
		if from, to, ok := tableRenameOf(&statements[index+3]); ok && sameTableRef(from, scratch) && sameTableRef(to, old) {
			return rebuildMatch{drop: index + 2, renames: []int{index + 3}}, true
		}
	}
	// YDB's order: the old table is moved aside, the copy takes its name, and
	// the old table is dropped under the name it was moved to.
	if index+4 >= len(statements) {
		return rebuildMatch{}, false
	}
	aside, replaced, ok := tableRenameOf(&statements[index+2])
	if !ok || !sameTableRef(aside, old) {
		return rebuildMatch{}, false
	}
	from, to, ok := tableRenameOf(&statements[index+3])
	if !ok || !sameTableRef(from, scratch) || !sameTableRef(to, old) {
		return rebuildMatch{}, false
	}
	if dropped, ok := singleDroppedTable(&statements[index+4]); !ok || !sameTableRef(dropped, replaced) {
		return rebuildMatch{}, false
	}
	return rebuildMatch{drop: index + 4, renames: []int{index + 2, index + 3}}, true
}

// tableCopy is an INSERT INTO ... SELECT ... FROM that copies every row of one
// table.
type tableCopy struct {
	into, from string
	// columns are the columns the INSERT names, folded; empty when it names
	// none.
	columns []string
	// star reports a SELECT * with no column list, which copies every column.
	star bool
}

// tableCopyOf reads a statement that copies every row of one table into
// another: `INSERT INTO s [(columns)] SELECT ... FROM t`, with nothing after
// the table, so no WHERE, join, grouping or limit leaves a row behind, and no
// DISTINCT folds two rows into one.
func tableCopyOf(stmt *Statement) (tableCopy, bool) {
	w := stmt.Words
	if !hasWordPrefix(w, "INSERT", "INTO") {
		return tableCopy{}, false
	}
	into, j := tableRefAt(w, stmt.sourceWords, 2)
	if into.normalized == "" {
		return tableCopy{}, false
	}
	copied := tableCopy{into: into.normalized}
	copied.columns, j = copyColumns(w, j)
	if j >= len(w) || w[j] != "SELECT" || (j+1 < len(w) && (w[j+1] == "DISTINCT" || w[j+1] == "ALL")) {
		return tableCopy{}, false
	}
	copied.star = len(copied.columns) == 0 && j+2 < len(w) && w[j+1] == "*" && w[j+2] == "FROM"
	from, ok := copiedTable(stmt, j+1)
	if !ok {
		return tableCopy{}, false
	}
	copied.from = from
	return copied, true
}

// copyColumns reads the column list an INSERT names at w[j], folded, and
// returns the index past it. A statement that names none returns none.
func copyColumns(w []string, j int) ([]string, int) {
	if j >= len(w) || w[j] != "(" {
		return nil, j
	}
	var columns []string
	for j++; j < len(w) && w[j] != ")"; j++ {
		if w[j] != "," {
			columns = append(columns, normalizeIdent(w[j]))
		}
	}
	return columns, j + 1
}

// copiedTable reads the table the SELECT from w[j] on reads: the one after
// its FROM, outside any parentheses, with nothing after it.
func copiedTable(stmt *Statement, j int) (string, bool) {
	w := stmt.Words
	depth := 0
	for ; j < len(w); j++ {
		switch {
		case w[j] == "(":
			depth++
		case w[j] == ")":
			depth--
		case depth == 0 && w[j] == "FROM":
			from, next := tableRefAt(w, stmt.sourceWords, j+1)
			return from.normalized, from.normalized != "" && next == len(w)
		}
	}
	return "", false
}

// singleDroppedTable reads a DROP TABLE of one table.
func singleDroppedTable(stmt *Statement) (string, bool) {
	if !hasWordPrefix(stmt.Words, "DROP", "TABLE") {
		return "", false
	}
	tables, complete := droppedTablesNotCreated(stmt.Words, stmt.sourceWords, nil)
	if !complete || len(tables) != 1 {
		return "", false
	}
	return tables[0].normalized, true
}

// tableRenameOf reads `ALTER TABLE from RENAME TO to`, the one clause of its
// statement.
func tableRenameOf(stmt *Statement) (from, to string, ok bool) {
	w := stmt.Words
	if !hasWordPrefix(w, "ALTER", "TABLE") {
		return "", "", false
	}
	table, j := tableRefAt(w, stmt.sourceWords, skipIfExists(w, 2))
	if table.normalized == "" || j+1 >= len(w) || w[j] != "RENAME" || w[j+1] != "TO" {
		return "", "", false
	}
	target, next := tableRefAt(w, stmt.sourceWords, j+2)
	if target.normalized == "" || next != len(w) {
		return "", "", false
	}
	return table.normalized, target.normalized, true
}

// sameTableRef reports whether two folded references name one table: equal,
// or equal in their last component when one of them names no schema, the
// comparison DS101's own exemption makes.
func sameTableRef(a, b string) bool {
	return refersToCreated(map[string]bool{b: true}, a)
}

// lostColumns returns the columns of the copied table that the copy leaves
// out, where its columns are known; none where they are not.
func lostColumns(file *File, old string, copied tableCopy, ydbState *ydbSchema) []string {
	known, ok := knownColumns(file, old, ydbState)
	if !ok || copied.star {
		return nil
	}
	var lost []string
	for _, column := range known {
		if !slices.Contains(copied.columns, normalizeIdent(column)) {
			lost = append(lost, column)
		}
	}
	return lost
}

// knownColumns returns the columns a table has before the file runs: on YDB
// from the directory's own history, elsewhere from the schema state the run
// read from the dev database. It reports false when neither knows the table.
func knownColumns(file *File, table string, ydbState *ydbSchema) ([]string, bool) {
	if ydbState != nil {
		for name, known := range ydbState.tables {
			if strings.EqualFold(name, table) && known.columnsKnown {
				return known.columns, true
			}
		}
		return nil, false
	}
	columns := file.baseline.tableColumns(table)
	if len(columns) == 0 {
		return nil, false
	}
	names := make([]string, 0, len(columns))
	for _, column := range columns {
		names = append(names, column.Name)
	}
	return names, true
}

// rebuildBaselineSubjects are the rebuild drops whose copy the dev database
// would check against the old table's columns. YDB has no dev database, and
// reads the directory's history instead.
func rebuildBaselineSubjects(file *File) []int {
	if ydbRun(file.Target) {
		return nil
	}
	steps := fileRebuilds(file)
	subjects := make([]int, 0, len(steps.drops))
	for index := range steps.drops {
		subjects = append(subjects, index)
	}
	slices.Sort(subjects)
	return subjects
}
