package lint

import (
	"slices"
	"strings"

	"ptah.run/core/lintext"
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

// changesNothing reports a change the replay does not make, because the
// statement is written IF EXISTS or IF NOT EXISTS and the starting state shows
// the engine skips it: the drop of a table [skipsAbsentTable] reports, the
// creation a CREATE TABLE IF NOT EXISTS skips because the starting state holds
// the table ([skipsPresentTable]), the same two answers for an index and for a
// column ([skipsIndex], [skipsColumn]), the drop of a type, a view, a
// materialized view, a trigger or a sequence that is not there
// ([skipsAbsentObject]), and an enum label added IF NOT EXISTS that the type
// already has ([skipsEnumLabel]). The community binary counts the changes the
// replay makes to the dev database, and a skipped statement makes none.
func changesNothing(file *File, change countedChange) bool {
	index, ref := change.StatementIndex, lintkit.NormalizeReference(change.Object)
	switch change.on {
	case kindTable:
		return skipsAbsentTable(file, index, ref) || skipsPresentTable(file, index, ref)
	case kindIndex:
		return skipsIndex(file, change, ref)
	case kindColumn:
		return skipsColumn(file, change, ref)
	case kindType:
		return skipsAbsentObject(file, change, ref) || skipsEnumLabel(file, change, ref)
	case kindView, kindMaterializedView, kindTrigger, kindSequence:
		return skipsAbsentObject(file, change, ref)
	default:
		return false
	}
}

// stateDecidedStatements are the statements of an up file holding a change
// only the starting state decides ([stateMayDecide]), and for the community
// count also a type drop the state may show the binary does not see
// ([typeDropStateDecides]). A count that skips what such a statement skips
// asks for that state.
func stateDecidedStatements(file *File, changes []countedChange, count ChangeCount) []int {
	var decided []int
	for _, change := range changes {
		if stateMayDecide(file, change) || count == ChangeCountAtlasCommunity && typeDropStateDecides(file, change) {
			decided = append(decided, change.StatementIndex)
		}
	}
	return slices.Compact(decided)
}

// typeDropStateDecides reports the drop of a type, written IF EXISTS or not,
// that the starting state can show to be an enum or not: the dialect's read
// lists types in full and nothing before it in the file names the type
// ([unseenBefore]). [dropsNonEnumType] answers it.
func typeDropStateDecides(file *File, change countedChange) bool {
	return change.on == kindType && change.Kind == SchemaChangeDrop &&
		listsObjects(file.dialect, lintext.BaselineType) &&
		unseenBefore(file, change.StatementIndex, lintkit.NormalizeReference(change.Object))
}

// dropsNonEnumType reports a type drop [typeDropStateDecides] lets the
// starting state decide, of a type the state holds as anything but an enum: a
// composite or a range type.
func dropsNonEnumType(file *File, change countedChange) bool {
	if !typeDropStateDecides(file, change) {
		return false
	}
	lookup, ok := stateRef(file, lintkit.NormalizeReference(change.Object))
	if !ok {
		return false
	}
	typed, held := file.baseline.Object(lintext.BaselineType, lookup)
	return held && !typed.Enum
}

// stateMayDecide reports a change whose making only the starting state
// decides, which is the set [changesNothing] can answer for: an IF EXISTS drop
// or an IF NOT EXISTS creation of a table, an index or a column, an IF EXISTS
// drop of an object of a kind the dialect's read lists in full, and an enum
// label added IF NOT EXISTS, each with nothing before it in the file that can
// have touched the object ([unseenBefore]).
func stateMayDecide(file *File, change countedChange) bool {
	index, ref := change.StatementIndex, lintkit.NormalizeReference(change.Object)
	w := file.Statements[index].Words
	switch change.on {
	case kindTable:
		return mayBeAbsent(file, index, ref) ||
			lintkit.CreatesTableIfNotExists(w) && lintkit.CreatedTableRef(w) == ref && unseenBefore(file, index, ref)
	case kindIndex, kindColumn:
		return change.guarded && unseenWithTable(file, index, ref, change.parent)
	default:
		kind, ok := change.on.baselineKind()
		if !ok || !change.guarded || !listsObjects(file.dialect, kind) {
			return false
		}
		switch change.Kind {
		case SchemaChangeDrop:
			return unseenWithTable(file, index, ref, change.parent)
		case SchemaChangeModify:
			return change.on == kindType && unseenBefore(file, index, ref)
		default:
			return false
		}
	}
}

// skipsStatement reports a statement of an up file that makes a change inside
// the scope and whose every such change the starting state shows the engine
// skips ([changesNothing]), so the statement changes nothing at all.
func skipsStatement(file *File, index int) bool {
	skipped := false
	for _, change := range file.changed {
		if change.StatementIndex != index {
			continue
		}
		if !changesNothing(file, change) {
			return false
		}
		skipped = true
	}
	return skipped
}

// skipsAbsentObject reports an IF EXISTS drop of a type, a view, a
// materialized view, a trigger or a sequence that the engine skips, with
// nothing before it in the file that names the object or the table a trigger is
// dropped from ([unseenWithTable]): the starting state lists objects of that
// kind in full and holds none the reference names. A trigger dropped ON a
// table, as PostgreSQL writes it, is looked for on that table.
func skipsAbsentObject(file *File, change countedChange, ref string) bool {
	kind, ok := change.on.baselineKind()
	if !ok || change.Kind != SchemaChangeDrop || !change.guarded ||
		!unseenWithTable(file, change.StatementIndex, ref, change.parent) {
		return false
	}
	if change.parent == "" {
		lookup, ok := stateRef(file, ref)
		return ok && file.baseline.LacksObject(kind, lookup, "")
	}
	table, ok := stateRef(file, change.parent)
	return ok && file.baseline.LacksObject(kind, ref, table)
}

// skipsEnumLabel reports an ALTER TYPE ... ADD VALUE IF NOT EXISTS that the
// engine skips, with nothing before it in the file that names the type
// ([unseenBefore]): the starting state holds the type as an enum that already
// has the label.
func skipsEnumLabel(file *File, change countedChange, ref string) bool {
	if change.Kind != SchemaChangeModify || !change.guarded || !unseenBefore(file, change.StatementIndex, ref) {
		return false
	}
	stmt := &file.Statements[change.StatementIndex]
	value, ok := lintkit.AlterEnumValue(stmt.Words, stmt.sourceWords)
	if !ok || !value.Adds {
		return false
	}
	lookup, ok := stateRef(file, ref)
	if !ok {
		return false
	}
	labels, isEnum := file.baseline.EnumValues(lookup)
	return isEnum && slices.Contains(labels, value.Label)
}

// skipsIndex reports an index change written IF EXISTS or IF NOT EXISTS that
// the engine skips, with nothing before it in the file that can have moved the
// name or the table ([unseenWithTable]). A drop skips an index the starting
// state holds under no table, or, where the statement names its table, as
// MySQL's DROP INDEX ... ON does, one that table does not hold. A creation
// skips an index its table already holds under that name.
func skipsIndex(file *File, change countedChange, ref string) bool {
	if !change.guarded || !unseenWithTable(file, change.StatementIndex, ref, change.parent) {
		return false
	}
	name := lastComponent(ref)
	switch change.Kind {
	case SchemaChangeDrop:
		if change.parent == "" {
			lookup, ok := stateRef(file, ref)
			return ok && file.baseline.LacksIndex(lookup)
		}
		table, ok := stateRef(file, change.parent)
		if !ok || file.baseline.TableColumns(table) == nil {
			return false
		}
		_, held := file.baseline.IndexNamed(table, name)
		return !held
	case SchemaChangeAdd:
		table, ok := stateRef(file, change.parent)
		if !ok {
			return false
		}
		_, held := file.baseline.IndexNamed(table, name)
		return held
	default:
		return false
	}
}

// skipsColumn reports a column change written IF EXISTS or IF NOT EXISTS that
// the engine skips: an ADD COLUMN IF NOT EXISTS of a column the starting state
// holds, or a DROP COLUMN IF EXISTS of one a table the state holds does not
// have, with nothing before it in the file that names the column or the table
// ([unseenBefore]).
func skipsColumn(file *File, change countedChange, ref string) bool {
	if !change.guarded || !unseenWithTable(file, change.StatementIndex, ref, change.parent) {
		return false
	}
	table, ok := stateRef(file, change.parent)
	if !ok {
		return false
	}
	_, held := file.baseline.Column(table, ref)
	switch change.Kind {
	case SchemaChangeAdd:
		return held
	case SchemaChangeDrop:
		return !held && file.baseline.TableColumns(table) != nil
	default:
		return false
	}
}

// unseenWithTable reports whether nothing before the statement at index can
// have moved the object ref names or the table it belongs to
// ([unseenBefore]). A statement may name no table, as a PostgreSQL DROP INDEX
// does; then only the object's own name is read.
func unseenWithTable(file *File, index int, ref, table string) bool {
	return unseenBefore(file, index, ref) && (table == "" || unseenBefore(file, index, table))
}

// stateRef returns the reference the starting state answers for ref under,
// and false where the state cannot answer for it as written. An unqualified
// reference, or one against a state whose rows name their schema, is looked up
// as it is. A state that names no schema was read from one schema, or from an
// engine without them, so a qualifier can be dropped only where it names the
// schema an unqualified name resolves to ([namesRunSchema]): any other may
// name a schema the state never read, a MySQL database, or a file attached to
// SQLite, and the answer there would be a guess.
func stateRef(file *File, ref string) (string, bool) {
	dot := strings.LastIndex(ref, ".")
	if dot < 0 || file.baseline.NamesSchemas() {
		return ref, true
	}
	if namesRunSchema(file, lastComponent(ref[:dot])) {
		return ref[dot+1:], true
	}
	return "", false
}

// namesRunSchema reports whether qualifier names the schema an unqualified
// name resolves to in the run: the schema the run reviews, or, where the run
// reviews no one schema, the one namespace of an engine that has no other
// ([soleSchema]), SQLite's main. A run that reviews every schema of an engine
// with several cannot say which of them an unqualified name lands in. SQLite's
// temp is a namespace of its own, which this does not name.
func namesRunSchema(file *File, qualifier string) bool {
	if !file.reviewed.unrestricted() {
		return file.reviewed.allowsSchema(qualifier)
	}
	sole := soleSchema(file.dialect)
	return sole != "" && strings.EqualFold(qualifier, sole)
}

// runReference is the folded reference ref without a qualifier that
// [namesRunSchema] reports, which names the same object as no qualifier.
func runReference(file *File, ref string) string {
	if dot := strings.LastIndex(ref, "."); dot >= 0 && namesRunSchema(file, lastComponent(ref[:dot])) {
		return ref[dot+1:]
	}
	return ref
}

// skipsPresentTable reports whether the statement at index is a CREATE TABLE
// IF NOT EXISTS of the table ref names that the starting state holds, with
// nothing before it in the file that can have moved the name ([unseenBefore]).
func skipsPresentTable(file *File, index int, ref string) bool {
	w := file.Statements[index].Words
	if !lintkit.CreatesTableIfNotExists(w) || lintkit.CreatedTableRef(w) != ref || !unseenBefore(file, index, ref) {
		return false
	}
	lookup, ok := stateRef(file, ref)
	return ok && file.baseline.TableColumns(lookup) != nil
}

// skipsAbsentTable reports whether the statement at index is a DROP TABLE IF
// EXISTS that skips the table ref names: [mayBeAbsent] holds the answer open,
// and the file's starting state was read and holds no table the reference
// names. The rules and the change count read this one predicate, so a drop
// they leave out of a finding is also left out of the count.
func skipsAbsentTable(file *File, index int, ref string) bool {
	lookup, ok := stateRef(file, ref)
	return ok && mayBeAbsent(file, index, ref) && file.baseline.LacksTable(lookup)
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
	if lookup, ok := stateRef(file, ref); ok && unseenBefore(file, index, ref) && file.baseline.LacksTable(lookup) {
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
// outside [plainStatements], or is one of them that may change tables it does
// not name ([hidesItsEffect]).
func opaqueBefore(file *File, index int) bool {
	for i := range index {
		words := file.Statements[i].Words
		if len(words) == 0 || !slices.Contains(plainStatements, words[0]) || hidesItsEffect(words) {
			return true
		}
	}
	return false
}

// hidesItsEffect reports a statement that leads with a plain word yet may
// drop or move a table, an index or a column without naming it: a DROP or an
// ALTER of a schema, of a database or of everything a role owns, and any
// statement that cascades, since a cascade drops what depends on the object
// it names, a column of a dropped type among it.
func hidesItsEffect(words []string) bool {
	if len(words) > 1 && (words[0] == "DROP" || words[0] == "ALTER") {
		switch strings.ToUpper(words[1]) {
		case "SCHEMA", "DATABASE", "OWNED":
			return true
		}
	}
	return slices.ContainsFunc(words, func(word string) bool { return strings.EqualFold(word, "CASCADE") })
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
// file ran: a column of a table the file created, a column an earlier ALTER
// TABLE ... ADD of the file added, and a column a DROP COLUMN IF EXISTS drops
// that the starting state shows the table does not have ([skipsColumn]). No
// deployed version of the application ever read any of them.
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
		absent := absentColumnDrops(file, i)
		for _, subject := range droppedColumnSubjects(stmt.Words, stmt.sourceWords) {
			column := lintkit.NormalizeIdent(subject.Name)
			if absent[column] || lintkit.RefersToCreated(created, owner) || lintkit.RefersToCreated(added[column], owner) {
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

// absentColumnDrops returns the folded names of the columns the ALTER TABLE at
// index drops IF EXISTS that the starting state shows the table does not have
// ([skipsColumn]).
func absentColumnDrops(file *File, index int) map[string]bool {
	absent := make(map[string]bool)
	for _, change := range file.changed {
		if change.StatementIndex == index && change.on == kindColumn && change.Kind == SchemaChangeDrop &&
			changesNothing(file, change) {
			absent[lintkit.NormalizeReference(change.Object)] = true
		}
	}
	return absent
}

// guardedColumnDrops are the ALTER TABLE statements of an up file holding a
// DROP COLUMN IF EXISTS whose column only the starting state can show to be
// absent ([stateMayDecide]).
func guardedColumnDrops(file *File) []int {
	var subjects []int
	for _, change := range file.changed {
		if change.on == kindColumn && change.Kind == SchemaChangeDrop && stateMayDecide(file, change) {
			subjects = append(subjects, change.StatementIndex)
		}
	}
	return slices.Compact(subjects)
}

// columnDropBaselineSubjects are the statements DS102 and BC104 read the
// starting state for: the CREATE TABLE IF NOT EXISTS statements, whose table it
// may show to be new, and the DROP COLUMN IF EXISTS statements, whose column it
// may show to be absent.
func columnDropBaselineSubjects(file *File) []int {
	return unionSubjects(file, ifNotExistsCreates, guardedColumnDrops)
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
