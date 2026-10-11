package lint

import (
	"slices"
	"strings"

	"ptah.run/core/lintext"
	"ptah.run/internal/lintscan"
)

// baselineColumns is one version's starting state, indexed for lookup by the
// source-spelled references the linter reads out of SQL.
//
// The zero value resolves nothing, which is what every run without a dev
// database gets and what keeps a rename reported exactly as it was before this
// existed.
type baselineColumns struct {
	byRef map[string][]lintext.BaselineColumn
	// byTable holds every column of a table under the same reference
	// spellings byRef uses, in catalog order, for the rules whose subject is
	// the whole table.
	byTable map[string][]lintext.BaselineColumn
	// byTableIndexes holds every index of a table under those spellings, in
	// catalog order.
	byTableIndexes map[string][]lintext.BaselineIndex
	// hypertables holds every TimescaleDB hypertable under the same reference
	// spellings, one entry per schema that carries the name.
	hypertables map[string][]lintext.BaselineHypertable
	// schemaless reports that no column in this state names a schema. A reader
	// scoped to one schema does not repeat that schema's name on every table, so
	// a migration writing `ALTER TABLE public.users` has a qualifier the state
	// cannot match literally. Nothing in the state can contradict that qualifier
	// either, which is what makes dropping it safe here and not once the state
	// carries several schemas.
	schemaless bool
}

// newBaselineIndex groups baseline columns, indexes and hypertables by the
// version whose starting state they describe.
func newBaselineIndex(
	columns []lintext.BaselineColumn,
	indexes []lintext.BaselineIndex,
	hypertables []lintext.BaselineHypertable,
) map[int64]baselineColumns {
	if len(columns) == 0 && len(indexes) == 0 && len(hypertables) == 0 {
		return nil
	}
	index := make(map[int64]baselineColumns)
	stateOf := func(version int64) baselineColumns {
		state, ok := index[version]
		if !ok {
			state = baselineColumns{
				byRef:          make(map[string][]lintext.BaselineColumn),
				byTable:        make(map[string][]lintext.BaselineColumn),
				byTableIndexes: make(map[string][]lintext.BaselineIndex),
				hypertables:    make(map[string][]lintext.BaselineHypertable),
				schemaless:     true,
			}
		}
		return state
	}
	for _, column := range columns {
		state := stateOf(column.Version)
		if column.Schema != "" {
			state.schemaless = false
		}
		for _, key := range baselineKeys(column) {
			state.byRef[key] = append(state.byRef[key], column)
		}
		for _, key := range baselineTableKeys(column.Schema, column.Table) {
			state.byTable[key] = append(state.byTable[key], column)
		}
		index[column.Version] = state
	}
	for _, idx := range indexes {
		state := stateOf(idx.Version)
		if idx.Schema != "" {
			state.schemaless = false
		}
		for _, key := range baselineTableKeys(idx.Schema, idx.Table) {
			state.byTableIndexes[key] = append(state.byTableIndexes[key], idx)
		}
		index[idx.Version] = state
	}
	for _, hypertable := range hypertables {
		state := stateOf(hypertable.Version)
		if hypertable.Schema != "" {
			state.schemaless = false
		}
		for _, key := range baselineTableKeys(hypertable.Schema, hypertable.Table) {
			state.hypertables[key] = append(state.hypertables[key], hypertable)
		}
		index[hypertable.Version] = state
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
func baselineKeys(column lintext.BaselineColumn) []string {
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
func baselineTableKeys(schema, tableName string) []string {
	table := normalizeIdent(tableName)
	if schema == "" {
		return []string{table}
	}
	return []string{table, normalizeIdent(schema) + "." + table}
}

// tableColumns returns every baseline column of one table, in catalog order,
// resolving the reference the way [baselineColumns.column] does and failing
// closed on the same ambiguity: a bare name two schemas carry resolves to
// neither.
func (b baselineColumns) tableColumns(tableRef string) []lintext.BaselineColumn {
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

func (b baselineColumns) exactTable(tableRef string) []lintext.BaselineColumn {
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

// tableIndexes returns every baseline index of one table, in catalog order,
// resolving the reference the way [baselineColumns.tableColumns] does and
// failing closed on the same ambiguity.
func (b baselineColumns) tableIndexes(tableRef string) []lintext.BaselineIndex {
	if len(b.byTableIndexes) == 0 || tableRef == "" {
		return nil
	}
	if indexes := b.exactTableIndexes(tableRef); indexes != nil {
		return indexes
	}
	if !b.schemaless {
		return nil
	}
	dot := strings.LastIndex(tableRef, ".")
	if dot < 0 {
		return nil
	}
	return b.exactTableIndexes(tableRef[dot+1:])
}

// hypertable reports whether tableRef names a TimescaleDB hypertable in this
// state.
//
// The table is placed through [baselineColumns.tableColumns], so a reference
// resolves here exactly when it resolves for every other lookup, and fails
// closed on the same ambiguity: a bare name two schemas carry is neither
// schema's table. The rule that asks then keeps its ordinary-table advice
// rather than claiming a hypertable it cannot place.
func (b baselineColumns) hypertable(tableRef string) bool {
	columns := b.tableColumns(tableRef)
	if len(columns) == 0 {
		return false
	}
	owner := columns[0]
	for _, hypertable := range b.hypertables[normalizeIdent(owner.Table)] {
		if hypertable.Schema == owner.Schema {
			return true
		}
	}
	return false
}

// partitioned reports whether tableRef names a PostgreSQL partitioned table in
// this state. The table is placed through [baselineColumns.tableColumns], so it
// fails closed on the same ambiguity every other lookup does.
func (b baselineColumns) partitioned(tableRef string) bool {
	columns := b.tableColumns(tableRef)
	return len(columns) > 0 && columns[0].TablePartitioned
}

func (b baselineColumns) exactTableIndexes(tableRef string) []lintext.BaselineIndex {
	indexes := b.byTableIndexes[tableRef]
	if len(indexes) == 0 {
		return nil
	}
	owner := indexes[0].Schema
	for _, idx := range indexes[1:] {
		if idx.Schema != owner {
			return nil
		}
	}
	return indexes
}

// columnIndexes returns the baseline indexes with a key part on one column
// of one table, prefix parts included: an index that reads any part of the
// value is rebuilt or copied with it. columnName is the linter's normalized
// form.
func (b baselineColumns) columnIndexes(tableRef, columnName string) []lintext.BaselineIndex {
	var found []lintext.BaselineIndex
	for _, idx := range b.tableIndexes(tableRef) {
		for _, part := range idx.Parts {
			if normalizeIdent(part.Column) == columnName {
				found = append(found, idx)
				break
			}
		}
	}
	return found
}

// indexNamed returns the baseline index of one table with the given name,
// compared in the linter's normalized form.
func (b baselineColumns) indexNamed(tableRef, name string) (lintext.BaselineIndex, bool) {
	for _, idx := range b.tableIndexes(tableRef) {
		if normalizeIdent(idx.Name) == name {
			return idx, true
		}
	}
	return lintext.BaselineIndex{}, false
}

// wholeKeyColumns returns the normalized columns an index keys, or nil when
// the key is not a plain column list: a part that is an expression or a
// prefix, or a part the reader could not name, keys something other than the
// column values, so the index says nothing about them.
func wholeKeyColumns(idx lintext.BaselineIndex) []string {
	if idx.Incomplete || len(idx.Parts) == 0 {
		return nil
	}
	columns := make([]string, 0, len(idx.Parts))
	for _, part := range idx.Parts {
		if part.Column == "" || part.Prefix > 0 {
			return nil
		}
		columns = append(columns, normalizeIdent(part.Column))
	}
	return columns
}

// uniqueCovering returns a baseline index that already proves the given
// columns hold no duplicate: a unique index or primary key over every row,
// keyed by whole columns, each of which is one of the given columns. A key
// over a subset proves the superset unique too. columns are normalized.
func (b baselineColumns) uniqueCovering(tableRef string, columns []string) (lintext.BaselineIndex, bool) {
	for _, idx := range b.tableIndexes(tableRef) {
		if !idx.Unique || idx.Partial {
			continue
		}
		key := wholeKeyColumns(idx)
		if len(key) == 0 || !subsetOf(key, columns) {
			continue
		}
		return idx, true
	}
	return lintext.BaselineIndex{}, false
}

// sameColumnSet reports whether two column lists name the same columns,
// in any order.
func sameColumnSet(a, b []string) bool {
	return len(a) == len(b) && subsetOf(a, b) && subsetOf(b, a)
}

func subsetOf(inner, outer []string) bool {
	for _, name := range inner {
		if !slices.Contains(outer, name) {
			return false
		}
	}
	return true
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

func (b baselineColumns) column(tableRef, columnName string) (lintext.BaselineColumn, bool) {
	if len(b.byRef) == 0 || tableRef == "" || columnName == "" {
		return lintext.BaselineColumn{}, false
	}
	if match, ok := b.exact(tableRef, columnName); ok {
		return match, ok
	}
	if !b.schemaless {
		return lintext.BaselineColumn{}, false
	}
	// Comparing last components when one side is unqualified is the same match
	// [refersToCreated] makes, and it inherits the same limit: the normalized
	// form of `"tenant.data"` and of `tenant.data` are one string.
	dot := strings.LastIndex(tableRef, ".")
	if dot < 0 {
		return lintext.BaselineColumn{}, false
	}
	return b.exact(tableRef[dot+1:], columnName)
}

func (b baselineColumns) exact(tableRef, columnName string) (lintext.BaselineColumn, bool) {
	matches := b.byRef[tableRef+"\x00"+columnName]
	if len(matches) != 1 {
		return lintext.BaselineColumn{}, false
	}
	return matches[0], true
}

// baselineVersions returns the versions whose starting schema state a rule
// asked for, sorted ascending.
//
// The list is assembled from the rules rather than from a check written here.
// "The versions carrying a column rename" is true only while exactly one rule
// needs the state, and goes quietly wrong for the second: its files are never
// read, its findings can never fire, and nothing says so (stokaro/ptah#1632). A
// rule declares [InputBaselineSchema] and names the files it wants through
// [Rule.BaselineSubject], and both this list and the rule's own check go
// through that one predicate.
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
	if !rule.Input.readsBaseline() || rule.BaselineSubjects == nil {
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
	case InputBaselineSchema, InputBaselineRefinement:
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
		if lintscan.MayDefineRoutine(file.Statements[i].SQL) {
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
func normalizeBaselineColumns(columns []lintext.BaselineColumn) []lintext.BaselineColumn {
	var kept []lintext.BaselineColumn
	for _, column := range columns {
		if strings.TrimSpace(column.Table) == "" || strings.TrimSpace(column.Name) == "" {
			continue
		}
		kept = append(kept, column)
	}
	return kept
}

// normalizeBaselineHypertables drops the entries no lookup can use, for the
// same reason [normalizeBaselineColumns] does.
func normalizeBaselineHypertables(hypertables []lintext.BaselineHypertable) []lintext.BaselineHypertable {
	var kept []lintext.BaselineHypertable
	for _, hypertable := range hypertables {
		if strings.TrimSpace(hypertable.Table) == "" {
			continue
		}
		kept = append(kept, hypertable)
	}
	return kept
}

// normalizeBaselineIndexes drops the entries no lookup can use, for the same
// reason [normalizeBaselineColumns] does.
func normalizeBaselineIndexes(indexes []lintext.BaselineIndex) []lintext.BaselineIndex {
	var kept []lintext.BaselineIndex
	for _, idx := range indexes {
		if strings.TrimSpace(idx.Table) == "" || strings.TrimSpace(idx.Name) == "" {
			continue
		}
		kept = append(kept, idx)
	}
	return kept
}

// baselineDependents is one version's readers, indexed by the column they read.
//
// The zero value resolves nothing, which is what every run without a dev
// database gets and what keeps a drop reported exactly as it was before this
// existed.
type baselineDependents struct {
	byRef map[string][]lintext.BaselineDependent
	// schemaless carries the same fact, and licenses the same relaxation, that
	// it does on [baselineColumns].
	schemaless bool
}

// newBaselineDependentIndex groups dependents by the version whose starting
// state they describe.
func newBaselineDependentIndex(dependents []lintext.BaselineDependent) map[int64]baselineDependents {
	if len(dependents) == 0 {
		return nil
	}
	index := make(map[int64]baselineDependents)
	for _, dependent := range dependents {
		state, ok := index[dependent.Version]
		if !ok {
			state = baselineDependents{byRef: make(map[string][]lintext.BaselineDependent), schemaless: true}
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
func baselineDependentKeys(dependent lintext.BaselineDependent) []string {
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
func (b baselineDependents) readers(tableRef, columnName string) []lintext.BaselineDependent {
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
