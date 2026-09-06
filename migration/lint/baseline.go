package lint

import (
	"slices"
	"strings"
)

// BaselineColumn is one column of the schema state a migration version starts
// from, as read from the dev database the run validates against.
//
// Reading SQL text is enough to see that `ALTER TABLE users RENAME COLUMN id TO
// oid` retires the name `id`. It is not enough to see what the new name
// introduces: the retired column's type, nullability and default live in an
// earlier migration file or in the base schema, never in the rename statement.
// A caller that replays the directory on a dev database can read them there and
// hand them over here, and only then can the add side of a rename be reported.
//
// Callers ask [Analysis.BaselineVersions] which versions are worth reading, so a
// directory with no renames costs no introspection at all.
type BaselineColumn struct {
	// Version is the migration version whose starting state this column belongs
	// to: the state read BEFORE that version is applied, not after.
	Version int64
	// Schema is the schema of the owning table, as the server spells it.
	Schema string
	// Table is the owning table, as the server spells it.
	Table string
	// Name is the column name, as the server spells it.
	Name string
	// DataType is the column type spelled the way the compatibility surface
	// prints it. Empty when the caller could not establish that spelling, which
	// leaves the diagnostic reported under Ptah's own wording rather than
	// inventing an Atlas sentence for a type nobody measured.
	DataType string
	// ColumnType is the type exactly as the server spells it, member lists
	// included: MySQL's information_schema COLUMN_TYPE, `enum('a','b')` or
	// `set('x','y')`. It is separate from DataType because DataType is a
	// normalized spelling for one family of diagnostics and is empty for
	// everything that spelling does not cover, which included every ENUM and
	// SET column; the rules that compare member lists read this field and
	// find nothing when it is empty (stokaro/ptah#2942).
	ColumnType string
	// Charset is the column's character set as the server names it, empty
	// for a column whose type carries none. TableCharset is the default
	// character set of the owning table, which is what a MODIFY that names
	// no character set gives the column (stokaro/ptah#2942).
	Charset      string
	TableCharset string
	// NotNull reports whether the column rejects NULL.
	NotNull bool
	// HasDefault reports whether the column carries a DEFAULT expression.
	HasDefault bool
}

// BaselineDependent is one object that reads a column in the schema state a
// version starts from.
//
// It is the answer to "what breaks if this column goes", carried into the
// linter rather than derived there: resolving it needs the view and routine
// bodies of the replayed schema, and the linter reads migration text.
//
// Kind names what does the reading -- a view, a materialized view, a function
// or a procedure -- because the operator's next move differs by kind and a bare
// name does not say which.
type BaselineDependent struct {
	// Version is the migration version whose starting state this belongs to:
	// the state read BEFORE that version is applied.
	Version int64
	// Schema and Table are the owning table, as the server spells them.
	Schema string
	Table  string
	// Column is the column that is read.
	Column string
	// Dependent is the object that reads it.
	Dependent string
	// Kind is what the dependent is.
	Kind string
}

// baselineColumns is one version's starting state, indexed for lookup by the
// source-spelled references the linter reads out of SQL.
//
// The zero value resolves nothing, which is what every run without a dev
// database gets and what keeps a rename reported exactly as it was before this
// existed.
type baselineColumns struct {
	byRef map[string][]BaselineColumn
	// byTable holds every column of a table under the same reference
	// spellings byRef uses, in catalog order, for the rules whose subject is
	// the whole table.
	byTable map[string][]BaselineColumn
	// schemaless reports that no column in this state names a schema. A reader
	// scoped to one schema does not repeat that schema's name on every table, so
	// a migration writing `ALTER TABLE public.users` has a qualifier the state
	// cannot match literally. Nothing in the state can contradict that qualifier
	// either, which is what makes dropping it safe here and not once the state
	// carries several schemas.
	schemaless bool
}

// newBaselineIndex groups baseline columns by the version whose starting state
// they describe.
func newBaselineIndex(columns []BaselineColumn) map[int64]baselineColumns {
	if len(columns) == 0 {
		return nil
	}
	index := make(map[int64]baselineColumns)
	for _, column := range columns {
		state, ok := index[column.Version]
		if !ok {
			state = baselineColumns{
				byRef:      make(map[string][]BaselineColumn),
				byTable:    make(map[string][]BaselineColumn),
				schemaless: true,
			}
		}
		if column.Schema != "" {
			state.schemaless = false
		}
		for _, key := range baselineKeys(column) {
			state.byRef[key] = append(state.byRef[key], column)
		}
		for _, key := range baselineTableKeys(column) {
			state.byTable[key] = append(state.byTable[key], column)
		}
		index[column.Version] = state
	}
	return index
}

// baselineKeys are the reference spellings that resolve to one column.
//
// A migration writes `ALTER TABLE users` or `ALTER TABLE public.users` for the
// same table, so both the qualified and the bare form are indexed. Identifiers
// fold to the same case the rest of the linter folds them to, because the
// alternative -- missing a column whose file spells it differently from the
// catalog -- silently drops a diagnostic.
func baselineKeys(column BaselineColumn) []string {
	name := normalizeIdent(column.Name)
	table := normalizeIdent(column.Table)
	qualified := normalizeIdent(column.Schema) + "." + table
	if column.Schema == "" {
		return []string{table + "\x00" + name}
	}
	return []string{
		table + "\x00" + name,
		qualified + "\x00" + name,
	}
}

// baselineTableKeys are the reference spellings that resolve to one table,
// the same two forms [baselineKeys] indexes a column under.
func baselineTableKeys(column BaselineColumn) []string {
	table := normalizeIdent(column.Table)
	if column.Schema == "" {
		return []string{table}
	}
	return []string{table, normalizeIdent(column.Schema) + "." + table}
}

// tableColumns returns every baseline column of one table, in catalog order,
// resolving the reference the way [baselineColumns.column] does and failing
// closed on the same ambiguity: a bare name two schemas carry resolves to
// neither.
func (b baselineColumns) tableColumns(tableRef string) []BaselineColumn {
	if len(b.byTable) == 0 || tableRef == "" {
		return nil
	}
	if columns := b.exactTable(tableRef); columns != nil {
		return columns
	}
	if !b.schemaless {
		return nil
	}
	dot := strings.LastIndex(tableRef, ".")
	if dot < 0 {
		return nil
	}
	return b.exactTable(tableRef[dot+1:])
}

func (b baselineColumns) exactTable(tableRef string) []BaselineColumn {
	columns := b.byTable[tableRef]
	if len(columns) == 0 {
		return nil
	}
	owner := columns[0].Schema
	for _, column := range columns[1:] {
		if column.Schema != owner {
			return nil
		}
	}
	return columns
}

// column returns the baseline state of one column of one table.
//
// tableRef and columnName are the linter's normalized identifier forms. A
// reference that resolves to more than one column -- a bare table name carried
// by several schemas -- is not a match: naming the wrong table's column is worse
// than saying nothing, so ambiguity fails closed.
// empty reports that this state resolves nothing, which is what a version the
// run never read from a dev database gets.
func (b baselineColumns) empty() bool {
	return len(b.byRef) == 0
}

func (b baselineColumns) column(tableRef, columnName string) (BaselineColumn, bool) {
	if len(b.byRef) == 0 || tableRef == "" || columnName == "" {
		return BaselineColumn{}, false
	}
	if match, ok := b.exact(tableRef, columnName); ok {
		return match, ok
	}
	if !b.schemaless {
		return BaselineColumn{}, false
	}
	// Comparing last components when one side is unqualified is the same match
	// [refersToCreated] makes, and it inherits the same limit: the normalized
	// form of `"tenant.data"` and of `tenant.data` are one string.
	dot := strings.LastIndex(tableRef, ".")
	if dot < 0 {
		return BaselineColumn{}, false
	}
	return b.exact(tableRef[dot+1:], columnName)
}

func (b baselineColumns) exact(tableRef, columnName string) (BaselineColumn, bool) {
	matches := b.byRef[tableRef+"\x00"+columnName]
	if len(matches) != 1 {
		return BaselineColumn{}, false
	}
	return matches[0], true
}

// baselineVersions returns the versions whose starting schema state a rule
// asked for, sorted ascending.
//
// The list is assembled from the rules rather than from a check written here.
// It used to be "the versions carrying a column rename", which was true because
// exactly one rule needed the state -- and would have gone quietly wrong for the
// second one, whose files nobody would have read and whose findings would
// therefore never have fired (stokaro/ptah#1632). Now a rule declares
// [InputBaselineSchema] and names the files it wants through
// [Rule.BaselineSubject], and both this list and the rule's own check go through
// that one predicate.
//
// A file no enabled rule asks about is not listed, so a directory with nothing
// to resolve costs no round trip.
func baselineVersions(files []File, opts Options, rules []Rule) []int64 {
	var versions []int64
	for i := range files {
		file := &files[i]
		if !baselineRequested(file, opts, rules) {
			continue
		}
		versions = append(versions, file.Version)
	}
	slices.Sort(versions)
	return slices.Compact(versions)
}

// baselineRequested reports whether any enabled rule wants this file's starting
// schema state.
func baselineRequested(file *File, opts Options, rules []Rule) bool {
	if !file.Selected || !file.IsUp || file.Version == 0 {
		return false
	}
	for _, rule := range rules {
		if ruleWantsBaseline(rule, file, opts) {
			return true
		}
	}
	return false
}

// ruleWantsBaseline reports whether rule asks for this file's starting schema
// state, for a statement the run actually reviews.
//
// The reviewed-schema filter is applied here rather than by each rule: a rename
// in a schema outside the scope produces no finding, so reading the database
// for it would be a round trip spent to learn nothing and reporting it as
// unresolved would be a warning about work the operator excluded on purpose.
func ruleWantsBaseline(rule Rule, file *File, opts Options) bool {
	if rule.Input != InputBaselineSchema || rule.BaselineSubjects == nil {
		return false
	}
	if !ruleRunsOnFile(rule, file, opts) {
		return false
	}
	for _, index := range rule.BaselineSubjects(file) {
		if !file.scopeExcluded[index] {
			return true
		}
	}
	return false
}

// unmetInputs returns every rule that asked for a file's starting schema state
// on a run that supplied none.
//
// This is the reason the input is declared at all. A rule that needs the
// replayed schema and does not get it does not fail -- it resolves nothing and
// reports less, while the run exits 0, which the issue that asked for this
// called the hardest kind of gap to notice from CI. Saying so costs one line of
// output and turns silence into a fact a reader can act on.
func unmetInputs(files []File, opts Options, rules []Rule) []UnmetInput {
	var unmet []UnmetInput
	for i := range files {
		file := &files[i]
		if !file.Selected || file.Version == 0 {
			continue
		}
		for _, rule := range rules {
			if !ruleInputUnmet(rule, file, opts) {
				continue
			}
			unmet = append(unmet, UnmetInput{
				Rule:    rule.Code,
				Input:   rule.Input,
				File:    file.Path,
				Version: file.Version,
			})
		}
	}
	return unmet
}

// ruleInputUnmet reports whether a rule asked this file for an input the run
// did not supply.
func ruleInputUnmet(rule Rule, file *File, opts Options) bool {
	switch rule.Input {
	case InputBaselineSchema:
		return file.IsUp && file.baseline.empty() && ruleWantsBaseline(rule, file, opts)
	case InputRoutineBody:
		return ruleWantsRoutineBody(rule, file, opts)
	case InputStatementText:
		return false
	default:
		return false
	}
}

// ruleWantsRoutineBody reports whether a rule that reads a routine body was
// handed a file whose routine bodies could not be parsed.
//
// Parsing a body needs a dialect. Without one the body is not parsed, the rule
// finds nothing, and the run exits 0 -- the exact failure RuleInput exists to
// prevent, which is why this is a third input rather than a detail of the
// second (stokaro/ptah#2357).
//
// A file that defines no routine is not a gap: the rule had nothing to ask for.
func ruleWantsRoutineBody(rule Rule, file *File, opts Options) bool {
	if strings.TrimSpace(opts.Dialect) != "" {
		return false
	}
	if !ruleRunsOnFile(rule, file, opts) {
		return false
	}
	for i := range file.Statements {
		if looksLikeRoutineDefinition(file.Statements[i].SQL) {
			return true
		}
	}
	return false
}

// UnmetInput reports one rule that asked for an analyzer input the run did not
// supply, on one file.
type UnmetInput struct {
	// Rule is the code of the rule that asked.
	Rule string
	// Input is what it asked for.
	Input RuleInput
	// File is the migration file whose analysis was thinner for the absence.
	File string
	// Version is that file's migration version.
	Version int64
}

// normalizeBaselineColumns drops entries no lookup can use, so an index never
// carries rows that can only produce a false ambiguity.
func normalizeBaselineColumns(columns []BaselineColumn) []BaselineColumn {
	kept := make([]BaselineColumn, 0, len(columns))
	for _, column := range columns {
		if strings.TrimSpace(column.Table) == "" || strings.TrimSpace(column.Name) == "" {
			continue
		}
		kept = append(kept, column)
	}
	return kept
}

// baselineDependents is one version's readers, indexed by the column they read.
//
// The zero value resolves nothing, which is what every run without a dev
// database gets and what keeps a drop reported exactly as it was before this
// existed.
type baselineDependents struct {
	byRef map[string][]BaselineDependent
	// schemaless carries the same fact, and licenses the same relaxation, that
	// it does on [baselineColumns].
	schemaless bool
}

// newBaselineDependentIndex groups dependents by the version whose starting
// state they describe.
func newBaselineDependentIndex(dependents []BaselineDependent) map[int64]baselineDependents {
	if len(dependents) == 0 {
		return nil
	}
	index := make(map[int64]baselineDependents)
	for _, dependent := range dependents {
		state, ok := index[dependent.Version]
		if !ok {
			state = baselineDependents{byRef: make(map[string][]BaselineDependent), schemaless: true}
		}
		if strings.TrimSpace(dependent.Schema) != "" {
			state.schemaless = false
		}
		for _, key := range baselineDependentKeys(dependent) {
			state.byRef[key] = append(state.byRef[key], dependent)
		}
		index[dependent.Version] = state
	}
	return index
}

// baselineDependentKeys are the spellings a migration might use for the column,
// in the forms [baselineKeys] builds for the same reason.
func baselineDependentKeys(dependent BaselineDependent) []string {
	name := normalizeIdent(dependent.Column)
	table := normalizeIdent(dependent.Table)
	if dependent.Schema == "" {
		return []string{table + "\x00" + name}
	}
	return []string{
		table + "\x00" + name,
		normalizeIdent(dependent.Schema) + "." + table + "\x00" + name,
	}
}

// readers returns everything that reads one column of one table.
//
// Unlike [baselineColumns.column] an ambiguous reference is not fatal here: two
// tables of that name in different schemas both read by something means the
// answer "something reads a column of this name" is still true, and the finding
// names each reader with the table it read. Reporting nothing would be the
// silence this rule exists to remove.
func (b baselineDependents) readers(tableRef, columnName string) []BaselineDependent {
	if len(b.byRef) == 0 || tableRef == "" || columnName == "" {
		return nil
	}
	if found := b.byRef[tableRef+"\x00"+columnName]; len(found) > 0 {
		return found
	}
	if !b.schemaless {
		return nil
	}
	dot := strings.LastIndex(tableRef, ".")
	if dot < 0 {
		return nil
	}
	return b.byRef[tableRef[dot+1:]+"\x00"+columnName]
}
