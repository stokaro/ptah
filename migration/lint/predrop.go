package lint

import (
	"slices"

	"ptah.run/internal/lintkit"
)

// The drop rules report a drop for the data it destroys and the name it
// retires. A drop of something the migration itself brought into being, or of
// a table that was never there, destroys and retires nothing, and the
// community binary, which compares the schema a file starts from with the
// schema it ends with, reports nothing for it. The predicates here are shared
// by the rule that speaks for the data and the rule that speaks for the
// deployed code, so the two cannot disagree on which drops count.

// ifExistsTableDrops are the indexes of the DROP TABLE IF EXISTS statements of
// an up file that the file's starting state may show to drop nothing: those
// naming a table [mayBeAbsent] holds open. For the rest the answer does not
// depend on the state, so a run without one is not missing anything there.
func ifExistsTableDrops(file *File) []int {
	if !file.IsUp {
		return nil
	}
	var drops []int
	for i := range file.Statements {
		stmt := &file.Statements[i]
		if !dropsTableIfExists(stmt.Words) {
			continue
		}
		tables, _ := lintkit.DroppedTablesNotCreated(stmt.Words, stmt.sourceWords, nil)
		if slices.ContainsFunc(tables, func(table lintkit.TableReference) bool {
			return mayBeAbsent(file, i, table.Normalized)
		}) {
			drops = append(drops, i)
		}
	}
	return drops
}

// dropsTableIfExists reports a DROP TABLE IF EXISTS statement.
func dropsTableIfExists(w []string) bool {
	return lintkit.HasWordPrefix(w, "DROP", "TABLE", "IF", "EXISTS")
}

// withoutAbsentTables returns the tables the DROP TABLE at index drops that
// may exist when it runs, leaving out each one [skipsAbsentTable] reports.
func withoutAbsentTables(file *File, index int, tables []lintkit.TableReference) []lintkit.TableReference {
	return slices.DeleteFunc(slices.Clone(tables), func(table lintkit.TableReference) bool {
		return skipsAbsentTable(file, index, table.Normalized)
	})
}

// withoutNoopChanges returns the changes of the statement at index without
// those the replay does not make: the drop of a table [skipsAbsentTable]
// reports, and the creation a CREATE TABLE IF NOT EXISTS skips because the
// starting state holds the table ([skipsPresentTable]). The community binary
// counts the changes the replay makes to the dev database, and a skipped
// statement makes none.
func withoutNoopChanges(file *File, index int, changes []SchemaChange) []SchemaChange {
	return slices.DeleteFunc(changes, func(change SchemaChange) bool {
		ref := lintkit.NormalizeReference(change.Object)
		return skipsAbsentTable(file, index, ref) || skipsPresentTable(file, index, ref)
	})
}

// skipsPresentTable reports whether the statement at index is a CREATE TABLE
// IF NOT EXISTS of the table ref names that the starting state holds, with
// nothing before it in the file that can have moved the name ([unseenBefore]).
func skipsPresentTable(file *File, index int, ref string) bool {
	w := file.Statements[index].Words
	return lintkit.CreatesTableIfNotExists(w) && lintkit.CreatedTableRef(w) == ref &&
		unseenBefore(file, index, ref) && file.baseline.TableColumns(ref) != nil
}

// skipsAbsentTable reports whether the statement at index is a DROP TABLE IF
// EXISTS that skips the table ref names: [mayBeAbsent] holds the answer open,
// and the file's starting state was read and holds no table the reference
// names. The rules and the change count read this one predicate, so a drop
// they leave out of a finding is also left out of the count.
func skipsAbsentTable(file *File, index int, ref string) bool {
	return mayBeAbsent(file, index, ref) && file.baseline.LacksTable(ref)
}

// mayBeAbsent reports whether the statement at index is a DROP TABLE IF EXISTS
// whose table only the starting state can show to be absent ([unseenBefore]).
func mayBeAbsent(file *File, index int, ref string) bool {
	return dropsTableIfExists(file.Statements[index].Words) && unseenBefore(file, index, ref)
}

// unseenBefore reports whether nothing before the statement at index can have
// given the table ref names a table, so the starting state alone says whether
// it exists there. An earlier statement that names it may have created it or
// renamed another table to it. An earlier statement whose effect its words do
// not show, such as a DO block, a CALL, a prepared statement or a SET that
// moves the search path, may have done either out of sight.
func unseenBefore(file *File, index int, ref string) bool {
	return !opaqueBefore(file, index) && !namedBefore(file, index, lastComponent(ref))
}

// newTable returns the folded reference of the table the statement at index
// brings into being, or "": the rules that exempt what a migration created
// read this one answer. A plain CREATE TABLE does. A CREATE TABLE IF NOT EXISTS
// may find the table there already, holding rows, so it counts only where the
// starting state was read and holds no such table, and nothing before it can
// have given the name one ([unseenBefore]), which is what the community binary
// sees when it replays the file. Without the state it does not count, and
// [ifNotExistsCreates] names the rules that would have read it.
func newTable(file *File, index int) string {
	w := file.Statements[index].Words
	ref := lintkit.CreatedTableRef(w)
	if ref == "" || !lintkit.CreatesTableIfNotExists(w) {
		return ref
	}
	if unseenBefore(file, index, ref) && file.baseline.LacksTable(ref) {
		return ref
	}
	return ""
}

// ifNotExistsCreates are the CREATE TABLE IF NOT EXISTS statements of an up file
// whose newness only the starting state decides ([newTable]) and whose table a
// later statement names, so a rule that exempts the file's own tables may
// answer differently with the state than without it.
func ifNotExistsCreates(file *File) []int {
	if !file.IsUp {
		return nil
	}
	var creates []int
	for i := range file.Statements {
		w := file.Statements[i].Words
		ref := lintkit.CreatedTableRef(w)
		if ref == "" || !lintkit.CreatesTableIfNotExists(w) || !unseenBefore(file, i, ref) || !namedAfter(file, i, lastComponent(ref)) {
			continue
		}
		creates = append(creates, i)
	}
	return creates
}

// namedAfter reports whether a statement after index carries name as a word.
func namedAfter(file *File, index int, name string) bool {
	for i := index + 1; i < len(file.Statements); i++ {
		if slices.ContainsFunc(file.Statements[i].Words, func(word string) bool { return lintkit.NormalizeIdent(word) == name }) {
			return true
		}
	}
	return false
}

// unionSubjects is the sorted statement indexes any of sources names, once
// each.
func unionSubjects(file *File, sources ...func(*File) []int) []int {
	var subjects []int
	for _, source := range sources {
		subjects = append(subjects, source(file)...)
	}
	slices.Sort(subjects)
	return slices.Compact(subjects)
}

// plainStatements are the leading words of statements whose effect on which
// tables exist their own words show.
var plainStatements = []string{"CREATE", "ALTER", "DROP", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "COMMENT", "GRANT", "REVOKE"}

// opaqueBefore reports whether a statement before index starts with a word
// outside [plainStatements].
func opaqueBefore(file *File, index int) bool {
	for i := range index {
		words := file.Statements[i].Words
		if len(words) == 0 || !slices.Contains(plainStatements, words[0]) {
			return true
		}
	}
	return false
}

// namedBefore reports whether a statement before index carries name as a word.
func namedBefore(file *File, index int, name string) bool {
	for i := range index {
		for _, word := range file.Statements[i].Words {
			if lintkit.NormalizeIdent(word) == name {
				return true
			}
		}
	}
	return false
}

// lastComponent is a folded reference without its qualifiers.
func lastComponent(ref string) string {
	for i := len(ref) - 1; i >= 0; i-- {
		if ref[i] == '.' {
			return ref[i+1:]
		}
	}
	return ref
}

// tableDropBaselineSubjects are the statements DS101 reads the starting state
// for: the drops that end a table rebuild, whose copy the old columns check,
// the DROP TABLE IF EXISTS statements, whose table the state may show to be
// absent, and the CREATE TABLE IF NOT EXISTS statements, whose table it may
// show to be new.
func tableDropBaselineSubjects(file *File) []int {
	return unionSubjects(file, rebuildBaselineSubjects, ifExistsTableDrops, ifNotExistsCreates)
}

// newColumnDrops returns, for each ALTER TABLE statement of an up file that
// drops columns, the folded names of those that hold no value from before the
// file ran: a column of a table the file created, and a column an earlier
// ALTER TABLE ... ADD of the file added. No deployed version of the
// application ever read either.
//
// A column added with IF NOT EXISTS may have been there already, so it is not
// one. A statement that renames, a RENAME or a MySQL CHANGE clause, may give a
// table or a column a name that held data before, so the tables it names stop
// counting as the file's own.
func newColumnDrops(file *File) map[int]map[string]bool {
	drops := make(map[int]map[string]bool)
	if !file.IsUp {
		return drops
	}
	created := make(map[string]bool)
	// added holds, for each folded column name, the tables an earlier ADD
	// gave it to.
	added := make(map[string]map[string]bool)
	for i := range file.Statements {
		stmt := &file.Statements[i]
		if ref := newTable(file, i); ref != "" {
			created[ref] = true
			continue
		}
		if renames(stmt.Words) {
			forgetNamedTables(stmt.Words, created, added)
			continue
		}
		if !lintkit.IsAlterTable(stmt.Words) {
			continue
		}
		owner := lintkit.AlterTableReference(stmt.Words, stmt.sourceWords).Normalized
		if owner == "" {
			continue
		}
		for _, subject := range droppedColumnSubjects(stmt.Words, stmt.sourceWords) {
			column := lintkit.NormalizeIdent(subject.Name)
			if lintkit.RefersToCreated(created, owner) || lintkit.RefersToCreated(added[column], owner) {
				if drops[i] == nil {
					drops[i] = make(map[string]bool)
				}
				drops[i][column] = true
			}
			delete(added[column], owner)
		}
		for _, column := range addedColumns(stmt.Words) {
			if added[column] == nil {
				added[column] = make(map[string]bool)
			}
			added[column][owner] = true
		}
	}
	return drops
}

// renames reports a statement that may move a name: a RENAME, or a MySQL
// CHANGE clause, which renames a column without the word.
func renames(w []string) bool {
	return slices.Contains(w, "RENAME") || slices.Contains(w, "CHANGE")
}

// forgetNamedTables stops counting as the file's own every table a renaming
// statement names.
func forgetNamedTables(w []string, created map[string]bool, added map[string]map[string]bool) {
	named := make(map[string]bool, len(w))
	for _, word := range w {
		named[lintkit.NormalizeIdent(word)] = true
	}
	for ref := range created {
		if named[lastComponent(ref)] {
			delete(created, ref)
		}
	}
	for _, tables := range added {
		for ref := range tables {
			if named[lastComponent(ref)] {
				delete(tables, ref)
			}
		}
	}
}

// addedColumns returns the folded names of the columns an ALTER TABLE adds,
// leaving out a column added with IF NOT EXISTS.
func addedColumns(w []string) []string {
	var columns []string
	for _, i := range lintkit.ClauseStarts(w) {
		start, _, ok := lintkit.AddColumnClause(w, i)
		if !ok || slices.Contains(w[i:start], "EXISTS") {
			continue
		}
		columns = append(columns, lintkit.NormalizeIdent(w[start-1]))
	}
	return columns
}

// withoutNewColumns returns the dropped columns that are not the file's own.
func withoutNewColumns(subjects []Subject, own map[string]bool) []Subject {
	if len(own) == 0 {
		return subjects
	}
	return slices.DeleteFunc(slices.Clone(subjects), func(subject Subject) bool {
		return own[lintkit.NormalizeIdent(subject.Name)]
	})
}
