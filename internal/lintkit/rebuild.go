package lintkit

import (
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"

	"ptah.run/core/lintext"
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
// DISTINCT or anything after its FROM but an alias leaves rows behind, and a
// copy into a table other than the one created just before it is not a
// rebuild. The statements a YDB rebuild writes beside its steps are skipped,
// because each writes no row and moves no table (see [rebuildAside]).
//
// A copy that keeps every row still loses a column whose values it does not
// write into the column of the same name. The INSERT pairs its columns with
// the SELECT list by position, so the item at each place has to carry the old
// column the INSERT names there (see [tableCopy.carried]). NULL, a constant,
// another column or an expression in that place loses every value of the
// column. SQLite's planner writes each item as the column itself, or as
// IFNULL(column, default) for a column it makes NOT NULL with a default, and
// both carry it. SELECT * with no column list copies every column.
//
// Which columns the copy has to carry depends on the old table, which the
// text alone does not show. Where its columns are known, a copy that loses
// one, or leaves one out entirely, is not a rebuild, and DS101 reports the
// drop. Where they are not known, any column the copy writes from something
// other than itself may be one of them, so that copy is not a rebuild either;
// a column it leaves out cannot be seen there. The YDB provider recognizes
// the rebuilds its own plans write, whose items convert a column through CAST
// and Unwrap, and a host reads its answer instead of this one.

// RebuildSteps are the statements of a file that belong to a table rebuild.
type RebuildSteps struct {
	// Drops are the DROP TABLE statements that retire a replaced table whose
	// rows a rebuild copied.
	Drops map[int]bool
	// Renames are the ALTER TABLE ... RENAME TO statements that move a
	// replaced table aside or move its copy into place.
	Renames map[int]bool
}

// Columns answers what a rebuild matcher knows of a table's columns just
// before the statement at index runs, folded or as written, in the order the
// table declares them, and whether it knows the table at all. Indexes arrive
// in increasing order.
type Columns func(index int, table string) ([]string, bool)

// Rebuilds finds the table rebuilds of a file's statements whose copy loses
// no column. columns says what the old table holds; nil knows no table, and
// a copy is then a rebuild only when it writes each column it names from
// the old column of the same name.
func Rebuilds(statements []lintext.Statement, columns Columns) RebuildSteps {
	steps := RebuildSteps{Drops: make(map[int]bool), Renames: make(map[int]bool)}
	for i := range statements {
		rebuild, copied, ok := matchRebuild(statements, i)
		if !ok || copyLoses(i, copied, columns) {
			continue
		}
		steps.Drops[rebuild.drop] = true
		for _, index := range rebuild.renames {
			steps.Renames[index] = true
		}
	}
	return steps
}

// RebuildCandidates returns, in order, the DROP TABLE statements that end the
// steps of a table rebuild, whatever its copy writes: the drops the old
// table's columns decide, which [Rebuilds] keeps or reports.
func RebuildCandidates(statements []lintext.Statement) []int {
	var drops []int
	for i := range statements {
		if rebuild, _, ok := matchRebuild(statements, i); ok {
			drops = append(drops, rebuild.drop)
		}
	}
	slices.Sort(drops)
	return drops
}

// RebuildChange describes a table replacement whose old columns are known.
type RebuildChange struct {
	Create      int
	LostColumns []string
	Renames     []int
}

// RebuildChanges reports complete table replacements with known old columns.
// The key is the DROP TABLE statement index. LostColumns are the old columns
// whose values the copy does not write into the column of the same name, in
// the order columns reports them. An unknown old table is omitted because the
// copy alone cannot establish whether it loses a column.
func RebuildChanges(statements []lintext.Statement, columns Columns) map[int]RebuildChange {
	changes := make(map[int]RebuildChange)
	if columns == nil {
		return changes
	}
	for i := range statements {
		match, copied, ok := matchRebuild(statements, i)
		if !ok {
			continue
		}
		known, ok := columns(i, copied.from)
		if !ok {
			continue
		}
		changes[match.drop] = RebuildChange{
			Create:      i,
			LostColumns: copied.lost(known),
			Renames:     match.renames,
		}
	}
	return changes
}

// atlasReplacementPrefix is the name prefix the community binary's SQLite
// analyzer reads a replacement table by. A copy under any other name is not
// folded there.
const atlasReplacementPrefix = "new_"

// AtlasReplacement is a SQLite table replacement in the shape the community
// binary folds into one table change: `CREATE TABLE new_t`, any one statement,
// `DROP TABLE t`, then `ALTER TABLE new_t RENAME TO t`, each a statement of its
// own and in that order.
type AtlasReplacement struct {
	// Create, Drop and Rename are statement indexes.
	Create int
	Drop   int
	Rename int
	// Table is the replaced table as the DROP TABLE spells it.
	Table string
}

// AtlasReplacements finds the table replacements the community binary's
// SQLite analyzer folds, in order. Two cannot overlap: no statement of one
// can begin another. baseline is the schema state before the statements run.
//
// The shape is that analyzer's, not [Rebuilds]': it keys on the `new_` prefix
// as spelled, which [Rebuilds] does not, and it does not read the statement
// between the CREATE and the DROP, where [Rebuilds] requires a full copy. A
// match says what the summary counts, never whether a row was lost.
func AtlasReplacements(statements []lintext.Statement, baseline State) []AtlasReplacement {
	var replacements []AtlasReplacement
	for create := range statements {
		if replacement, ok := atlasReplacementAt(statements, create, baseline); ok {
			replacements = append(replacements, replacement)
		}
	}
	return replacements
}

// atlasReplacementAt matches the replacement whose CREATE TABLE is the
// statement at create.
//
// The analyzer compares table names exactly, as the server spells them: the
// copy as its CREATE TABLE writes it, the old table as the baseline reports
// it, and the copy's new name as the rename writes it. A server resolves a
// name in any case, so `ALTER TABLE NEW_USERS RENAME TO users` still renames
// new_users, but `RENAME TO USERS` leaves a table the analyzer does not
// recognize as users.
func atlasReplacementAt(statements []lintext.Statement, create int, baseline State) (AtlasReplacement, bool) {
	replacement := AtlasReplacement{Create: create, Drop: create + 2, Rename: create + 3}
	if replacement.Rename >= len(statements) {
		return AtlasReplacement{}, false
	}
	scratch := CreatedTable(statements[create].Words, statements[create].SourceWords)
	table, ok := strings.CutPrefix(spelledName(scratch), atlasReplacementPrefix)
	if !ok || table == "" {
		return AtlasReplacement{}, false
	}
	dropped, ok := singleDroppedTable(&statements[replacement.Drop])
	if !ok || serverSpelling(dropped, baseline) != table {
		return AtlasReplacement{}, false
	}
	from, to, ok := tableRenameOf(&statements[replacement.Rename])
	if !ok || !sameTableRef(from.Normalized, scratch.Normalized) || spelledName(to) != table {
		return AtlasReplacement{}, false
	}
	replacement.Table = dropped.Name
	return replacement, true
}

// spelledName is a table reference's name as written, past any schema and
// without its quotes.
func spelledName(ref TableReference) string {
	unqualified := ref.Name[strings.LastIndex(ref.Name, ".")+1:]
	return strings.Trim(unqualified, "`\"[]")
}

// serverSpelling is the name of the table ref names as the baseline reports
// it, and as ref writes it where the baseline does not know the table.
func serverSpelling(ref TableReference, baseline State) string {
	if columns := baseline.TableColumns(ref.Normalized); len(columns) > 0 {
		return columns[0].Table
	}
	return spelledName(ref)
}

// rebuildMatch is one rebuild: the statement that drops the replaced table and
// the renames that swap the copy in.
type rebuildMatch struct {
	drop    int
	renames []int
}

// matchRebuild reports whether the statements from index on have the steps
// of a rebuild, in either order, and returns its copy. Whether the copy loses
// a column is the caller's question.
func matchRebuild(statements []lintext.Statement, index int) (rebuildMatch, tableCopy, bool) {
	scratch := CreatedTableRef(statements[index].Words)
	if scratch == "" {
		return rebuildMatch{}, tableCopy{}, false
	}
	steps := rebuildSteps(statements, index)
	if len(steps) < 3 {
		return rebuildMatch{}, tableCopy{}, false
	}
	copied, ok := tableCopyOf(&statements[steps[0]])
	if !ok || !sameTableRef(copied.into, scratch) || sameTableRef(copied.from, scratch) {
		return rebuildMatch{}, tableCopy{}, false
	}
	if len(copied.columns) == 0 {
		// An INSERT that names no column fills the scratch table's columns in
		// the order its CREATE TABLE declares them.
		copied.columns = createdColumns(&statements[index])
	}
	match, ok := sqliteSwap(statements, steps[1:], scratch, copied.from)
	if !ok {
		match, ok = ydbSwap(statements, steps[1:], scratch, copied.from)
	}
	return match, copied, ok
}

// rebuildSteps are the indexes of the four statements after the one at
// index, past what a YDB rebuild writes beside its steps, or fewer at the
// end of the file.
func rebuildSteps(statements []lintext.Statement, index int) []int {
	var steps []int
	for i := index + 1; i < len(statements) && len(steps) < 4; i++ {
		if !rebuildAside(&statements[i]) {
			steps = append(steps, i)
		}
	}
	return steps
}

// sqliteSwap matches SQLite's order at steps: the old table is dropped, then
// the copy takes its name.
func sqliteSwap(statements []lintext.Statement, steps []int, scratch, old string) (rebuildMatch, bool) {
	dropped, ok := singleDroppedTable(&statements[steps[0]])
	if !ok || !sameTableRef(dropped.Normalized, old) {
		return rebuildMatch{}, false
	}
	from, to, ok := tableRenameOf(&statements[steps[1]])
	if !ok || !sameTableRef(from.Normalized, scratch) || !sameTableRef(to.Normalized, old) {
		return rebuildMatch{}, false
	}
	return rebuildMatch{drop: steps[0], renames: []int{steps[1]}}, true
}

// ydbSwap matches YDB's order at steps: the old table is moved aside, the
// copy takes its name, and the old table is dropped under the name it was
// moved to.
func ydbSwap(statements []lintext.Statement, steps []int, scratch, old string) (rebuildMatch, bool) {
	if len(steps) < 3 {
		return rebuildMatch{}, false
	}
	aside, replaced, ok := tableRenameOf(&statements[steps[0]])
	if !ok || !sameTableRef(aside.Normalized, old) {
		return rebuildMatch{}, false
	}
	from, to, ok := tableRenameOf(&statements[steps[1]])
	if !ok || !sameTableRef(from.Normalized, scratch) || !sameTableRef(to.Normalized, old) {
		return rebuildMatch{}, false
	}
	if dropped, ok := singleDroppedTable(&statements[steps[2]]); !ok || !sameTableRef(dropped.Normalized, replaced.Normalized) {
		return rebuildMatch{}, false
	}
	return rebuildMatch{drop: steps[2], renames: []int{steps[0], steps[1]}}, true
}

// rebuildAside reports a statement a YDB rebuild writes beside its steps,
// each of which writes no row and moves no table: an index setting of the
// copy, after it is created, a changefeed dropped from the old table before
// it moves, since YDB moves no table that carries one, a changefeed added to
// the copy once it holds the name, and a change of a changefeed's topic,
// which adds its consumers. The YDB provider writes each as a statement of
// its own.
func rebuildAside(stmt *lintext.Statement) bool {
	w := stmt.Words
	if HasWordPrefix(w, "ALTER", "TOPIC") {
		return true
	}
	if !HasWordPrefix(w, "ALTER", "TABLE") {
		return false
	}
	starts := ClauseStarts(w)
	if len(starts) == 0 {
		return false
	}
	for _, i := range starts {
		if i+1 >= len(w) {
			return false
		}
		changefeed := (w[i] == "ADD" || w[i] == "DROP") && w[i+1] == "CHANGEFEED"
		if !changefeed && (w[i] != "ALTER" || w[i+1] != "INDEX") {
			return false
		}
	}
	return true
}

// tableCopy is an INSERT INTO ... SELECT ... FROM that copies every row of one
// table.
type tableCopy struct {
	into, from string
	// alias is the name the SELECT gives the copied table, folded; empty when
	// it gives none.
	alias string
	// columns are the columns the INSERT fills, folded, in order: those it
	// names, or, where it names none, those the scratch table's CREATE TABLE
	// declares. Empty where neither can be read.
	columns []string
	// carries holds, for each item of the SELECT list, the column of the
	// copied table, folded, whose every value the item writes, and "" for an
	// item that writes anything else.
	carries []string
	// star reports a SELECT * with no column list, which writes the old
	// table's columns into the scratch table's by position.
	star bool
}

// tableCopyOf reads a statement that copies every row of one table into
// another: `INSERT INTO s [(columns)] SELECT ... FROM t [[AS] alias]`, with
// nothing after the table but its alias, so no WHERE, join, grouping or limit
// leaves a row behind, and no DISTINCT folds two rows into one.
func tableCopyOf(stmt *lintext.Statement) (tableCopy, bool) {
	w := stmt.Words
	if !HasWordPrefix(w, "INSERT", "INTO") {
		return tableCopy{}, false
	}
	into, j := TableRefAt(w, stmt.SourceWords, 2)
	if into.Normalized == "" {
		return tableCopy{}, false
	}
	copied := tableCopy{into: into.Normalized}
	copied.columns, j = copyColumns(w, j)
	if j >= len(w) || w[j] != "SELECT" || (j+1 < len(w) && (w[j+1] == "DISTINCT" || w[j+1] == "ALL")) {
		return tableCopy{}, false
	}
	copied.star = len(copied.columns) == 0 && j+2 < len(w) && w[j+1] == "*" && w[j+2] == "FROM"
	from, ok := copiedTable(stmt, j+1)
	if !ok {
		return tableCopy{}, false
	}
	copied.from, copied.alias = from.table, from.alias
	for _, item := range selectItems(w, j+1, from.at) {
		copied.carries = append(copied.carries, copied.carried(stmt, item[0], item[1]))
	}
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
			columns = append(columns, NormalizeIdent(w[j]))
		}
	}
	return columns, j + 1
}

// createdColumns reads the columns a CREATE TABLE statement declares, folded,
// in order: the first word of each element of its body that opens no table
// constraint. It returns none where the body cannot be read that way: no body,
// as in CREATE TABLE ... AS SELECT, or an element that starts with no name.
func createdColumns(stmt *lintext.Statement) []string {
	w := stmt.Words
	_, open := createdTableAt(w, stmt.SourceWords)
	if open == 0 || open >= len(w) || w[open] != "(" {
		return nil
	}
	var columns []string
	depth := 0
	for k := open; k < len(w); k++ {
		switch w[k] {
		case "(":
			depth++
			if depth > 1 {
				continue
			}
		case ")":
			depth--
			if depth == 0 {
				return columns
			}
			continue
		case ",":
			if depth > 1 {
				continue
			}
		default:
			continue
		}
		// k opens the body or ends an element at depth one: an element starts
		// after it.
		column, ok := tableElementColumn(stmt, k+1)
		if !ok {
			return nil
		}
		if column != "" {
			columns = append(columns, column)
		}
	}
	return nil
}

// tableElementColumn reads the CREATE TABLE element at w[k]: the column it
// declares, folded, or "" for a table constraint. It reports false for an
// element that starts with no name.
func tableElementColumn(stmt *lintext.Statement, k int) (string, bool) {
	w := stmt.Words
	if k >= len(w) {
		return "", false
	}
	switch w[k] {
	case "CONSTRAINT", "PRIMARY", "FOREIGN", "CHECK", "UNIQUE", "EXCLUDE",
		"PERIOD", "INDEX", "KEY", "FULLTEXT", "SPATIAL":
		return "", true
	}
	column, _, ok := IdentifierAt(w, stmt.SourceWords, k)
	return column.Normalized, ok
}

// copySource is the table a copy's SELECT reads, folded, the alias it gives
// it, and where its FROM is.
type copySource struct {
	table, alias string
	at           int
}

// copiedTable reads the table the SELECT from w[j] on reads: the one after
// its FROM, outside any parentheses, with nothing after it but an alias.
func copiedTable(stmt *lintext.Statement, j int) (copySource, bool) {
	w := stmt.Words
	depth := 0
	for ; j < len(w); j++ {
		switch {
		case w[j] == "(":
			depth++
		case w[j] == ")":
			depth--
		case depth == 0 && w[j] == "FROM":
			from, next := TableRefAt(w, stmt.SourceWords, j+1)
			alias, end := tableAlias(stmt, next)
			return copySource{table: from.Normalized, alias: alias, at: j}, from.Normalized != "" && end == len(w)
		}
	}
	return copySource{}, false
}

// tableAlias reads the alias `[AS] name` at w[j], folded, and returns it and
// the index past it, or no alias and j where there is none. ClickHouse's
// FINAL is not an alias: it folds the rows a key holds twice into one.
func tableAlias(stmt *lintext.Statement, j int) (string, int) {
	w := stmt.Words
	k := j
	if k < len(w) && w[k] == "AS" {
		k++
	}
	if k == j && k < len(w) && (w[k] == "FINAL" || notAnAlias(w[k])) {
		return "", j
	}
	alias, next, ok := IdentifierAt(w, stmt.SourceWords, k)
	if !ok {
		return "", j
	}
	return alias.Normalized, next
}

// notAnAlias reports whether a bare word, read where an alias may stand, is an
// operator or a clause instead: `label ISNULL` is the expression
// `label IS NULL`, which writes 0 or 1 where the value was, and `users WHERE`
// opens a clause. A quoted word is always a name.
func notAnAlias(word string) bool {
	switch word {
	case "ISNULL", "NOTNULL", "COLLATE", "ESCAPE",
		"WHERE", "GROUP", "HAVING", "WINDOW", "ORDER", "LIMIT", "OFFSET", "FETCH",
		"UNION", "EXCEPT", "INTERSECT", "RETURNING",
		"NATURAL", "JOIN", "LEFT", "RIGHT", "INNER", "OUTER", "CROSS", "FULL", "ON", "USING":
		return true
	}
	return false
}

// selectItems splits the SELECT list w[j:end] at its top-level commas, as
// [start, end) index pairs.
func selectItems(w []string, j, end int) [][2]int {
	var items [][2]int
	depth := 0
	for k, start := j, j; k <= end; k++ {
		switch {
		case k == end || (depth == 0 && w[k] == ","):
			items = append(items, [2]int{start, k})
			start = k + 1
		case w[k] == "(":
			depth++
		case w[k] == ")":
			depth--
		}
	}
	return items
}

// carried reads the item w[start:end] of the copy's SELECT list and returns
// the column of the copied table, folded, whose every value it writes: the
// column itself, bare or qualified by the table or its alias, or the column
// passed through IFNULL or COALESCE, which replace a NULL and nothing else.
// The item's own alias does not matter, since the INSERT pairs items with its
// columns by position. Any other item returns "": NULL, a constant, an
// expression, and a CAST too, which can turn a value it cannot convert into
// another one.
//
// A name compares as the copy's column list compares with the old table's
// columns, without its quotes and in one case. A table holding two columns
// whose names differ only in case, which PostgreSQL allows, is read as
// holding one.
func (c tableCopy) carried(stmt *lintext.Statement, start, end int) string {
	// The item's words alone, so that nothing read here runs into the next
	// item or past the FROM.
	item := stmt.Words[:end]
	var column string
	var next int
	if start+1 < end && (item[start] == "IFNULL" || item[start] == "COALESCE") && item[start+1] == "(" {
		column, next = c.firstArgument(item, stmt.SourceWords, start+1)
	} else {
		column, next = c.columnAt(item, stmt.SourceWords, start)
	}
	if column == "" || !itemAlias(item, stmt.SourceWords, next) {
		return ""
	}
	return column
}

// firstArgument reads a column of the copied table as the first argument of
// the call whose parenthesis is w[open], and returns it and the index past
// the call.
func (c tableCopy) firstArgument(w, sourceWords []string, open int) (string, int) {
	column, next := c.columnAt(w, sourceWords, open+1)
	closing := closingParen(w, open)
	if column == "" || closing < 0 || next > closing || (next < closing && w[next] != ",") {
		return "", open
	}
	return column, closing + 1
}

// closingParen returns the index of the parenthesis that closes w[open], or
// -1 where none does.
func closingParen(w []string, open int) int {
	depth := 0
	for k := open; k < len(w); k++ {
		switch w[k] {
		case "(":
			depth++
		case ")":
			depth--
			if depth == 0 {
				return k
			}
		}
	}
	return -1
}

// columnAt reads a column of the copied table at w[k], bare or qualified by
// the table or its alias, and returns it, folded, and the index past it; ""
// where the words there are anything else.
func (c tableCopy) columnAt(w, sourceWords []string, k int) (string, int) {
	var parts []string
	for {
		if k >= len(w) || !columnWord(w[k]) {
			return "", k
		}
		part, next, ok := IdentifierAt(w, sourceWords, k)
		if !ok {
			return "", k
		}
		parts = append(parts, part.Normalized)
		k = next
		if k >= len(w) || w[k] != "." {
			break
		}
		k++
	}
	if qualifier := strings.Join(parts[:len(parts)-1], "."); qualifier != "" && !c.names(qualifier) {
		return "", k
	}
	return parts[len(parts)-1], k
}

// columnWord reports whether a word in a SELECT list can name a column: not
// a number, and not a bare word the SQL standard reads as a value, such as
// NULL, CURRENT_TIMESTAMP or USER. A quoted word always can.
func columnWord(word string) bool {
	switch word {
	case "NULL", "TRUE", "FALSE",
		"CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "LOCALTIME", "LOCALTIMESTAMP",
		"USER", "CURRENT_USER", "SESSION_USER", "SYSTEM_USER", "CURRENT_ROLE":
		return false
	}
	r, _ := utf8.DecodeRuneInString(word)
	return !unicode.IsDigit(r)
}

// names reports whether a column's qualifier names the copied table: its
// alias where the SELECT gives it one, and the table otherwise.
func (c tableCopy) names(qualifier string) bool {
	if c.alias != "" {
		return qualifier == c.alias
	}
	return sameTableRef(qualifier, c.from)
}

// itemAlias reports whether the words of an item from w[k] on are nothing or
// the item's alias: `AS name` or a bare name that is no operator.
func itemAlias(w, sourceWords []string, k int) bool {
	if k >= len(w) {
		return k == len(w)
	}
	if w[k] == "AS" {
		k++
	} else if notAnAlias(w[k]) {
		return false
	}
	_, next, ok := IdentifierAt(w, sourceWords, k)
	return ok && next == len(w)
}

// kept returns the columns the INSERT names, folded, that receive every value
// of the copied table's column of the same name. A SELECT list of another
// length than the column list keeps none, since the statement fails.
func (c tableCopy) kept() []string {
	if len(c.carries) != len(c.columns) {
		return nil
	}
	var kept []string
	for i, column := range c.columns {
		if c.carries[i] == column {
			kept = append(kept, column)
		}
	}
	return kept
}

// starKept returns the columns a SELECT * copy keeps: those the scratch table
// declares in the place where the old table, as known spells it, holds the
// column of the same name, since the copy pairs them by position. A scratch
// table of another width keeps none, since the statement fails.
func (c tableCopy) starKept(known []string) []string {
	if len(known) != len(c.columns) {
		return nil
	}
	var kept []string
	for i, column := range c.columns {
		if NormalizeIdent(known[i]) == column {
			kept = append(kept, column)
		}
	}
	return kept
}

// lost returns the old table's columns, as known spells them, whose values
// the copy does not write into the column of the same name.
func (c tableCopy) lost(known []string) []string {
	kept := c.kept()
	if c.star {
		kept = c.starKept(known)
	}
	var lost []string
	for _, column := range known {
		if !slices.Contains(kept, NormalizeIdent(column)) {
			lost = append(lost, column)
		}
	}
	return lost
}

// copyLoses reports whether the copy may leave a value of the old table
// behind. Where the old table's columns are known, that is a column the copy
// loses. Where they are not, it is any column the copy fills from something
// other than itself, since it may be one the old table holds, and a copy
// whose columns cannot be read. A SELECT * is one of those: its item names no
// column, and only the old table's order says which of its columns lands
// where.
func copyLoses(index int, copied tableCopy, columns Columns) bool {
	if columns != nil {
		if known, ok := columns(index, copied.from); ok {
			return len(copied.lost(known)) > 0
		}
	}
	return len(copied.columns) == 0 || len(copied.kept()) < len(copied.columns)
}

// singleDroppedTable reads a DROP TABLE of one table.
func singleDroppedTable(stmt *lintext.Statement) (TableReference, bool) {
	if !HasWordPrefix(stmt.Words, "DROP", "TABLE") {
		return TableReference{}, false
	}
	tables, complete := DroppedTablesNotCreated(stmt.Words, stmt.SourceWords, nil)
	if !complete || len(tables) != 1 {
		return TableReference{}, false
	}
	return tables[0], true
}

// tableRenameOf reads `ALTER TABLE from RENAME TO to`, the one clause of its
// statement.
func tableRenameOf(stmt *lintext.Statement) (from, to TableReference, ok bool) {
	w := stmt.Words
	if !HasWordPrefix(w, "ALTER", "TABLE") {
		return TableReference{}, TableReference{}, false
	}
	table, j := TableRefAt(w, stmt.SourceWords, SkipIfExists(w, 2))
	if table.Normalized == "" || j+1 >= len(w) || w[j] != "RENAME" || w[j+1] != "TO" {
		return TableReference{}, TableReference{}, false
	}
	target, next := TableRefAt(w, stmt.SourceWords, j+2)
	if target.Normalized == "" || next != len(w) {
		return TableReference{}, TableReference{}, false
	}
	return table, target, true
}

// sameTableRef reports whether two folded references name one table: equal,
// or equal in their last component when one of them names no schema, the
// comparison DS101's own exemption makes.
func sameTableRef(a, b string) bool {
	return RefersToCreated(map[string]bool{b: true}, a)
}
