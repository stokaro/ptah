package lintkit

import (
	"slices"
	"strings"

	"ptah.run/core/lintext"
)

// State is one version's starting state, indexed for lookup by the
// source-spelled references the linter reads out of SQL.
//
// The zero value resolves nothing, which is what every run without a dev
// database gets and what keeps a rename reported exactly as it was before this
// existed.
type State struct {
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

// NewStates indexes a baseline's columns, indexes and hypertables by the
// version whose starting state each describes. A row no lookup can use -- a
// column or an index without a table or a name, a hypertable without a
// table -- is left out, so it cannot make a reference look ambiguous. The
// dependents are not indexed here. A version with no rows has the zero State.
func NewStates(baseline lintext.Baseline) map[int64]State {
	columns := normalizeColumns(baseline.Columns)
	indexes := normalizeIndexes(baseline.Indexes)
	hypertables := normalizeHypertables(baseline.Hypertables)
	if len(columns) == 0 && len(indexes) == 0 && len(hypertables) == 0 {
		return nil
	}
	index := make(map[int64]State)
	stateOf := func(version int64) State {
		state, ok := index[version]
		if !ok {
			state = State{
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
	name := NormalizeIdent(column.Name)
	table := NormalizeIdent(column.Table)
	qualified := NormalizeIdent(column.Schema) + "." + table
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
	table := NormalizeIdent(tableName)
	if schema == "" {
		return []string{table}
	}
	return []string{table, NormalizeIdent(schema) + "." + table}
}

// TableColumns returns every baseline column of one table, in catalog order,
// resolving the reference the way [State.Column] does and failing
// closed on the same ambiguity: a bare name two schemas carry resolves to
// neither.
func (b State) TableColumns(tableRef string) []lintext.BaselineColumn {
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

// exactTable is the columns indexed under exactly tableRef, nil when they
// belong to more than one schema.
func (b State) exactTable(tableRef string) []lintext.BaselineColumn {
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

// TableIndexes returns every baseline index of one table, in catalog order,
// resolving the reference the way [State.TableColumns] does and
// failing closed on the same ambiguity.
func (b State) TableIndexes(tableRef string) []lintext.BaselineIndex {
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

// Hypertable reports whether tableRef names a TimescaleDB hypertable in this
// state.
//
// The table is placed through [State.TableColumns], so a reference
// resolves here exactly when it resolves for every other lookup, and fails
// closed on the same ambiguity: a bare name two schemas carry is neither
// schema's table. The rule that asks then keeps its ordinary-table advice
// rather than claiming a hypertable it cannot place.
func (b State) Hypertable(tableRef string) bool {
	columns := b.TableColumns(tableRef)
	if len(columns) == 0 {
		return false
	}
	owner := columns[0]
	for _, hypertable := range b.hypertables[NormalizeIdent(owner.Table)] {
		if hypertable.Schema == owner.Schema {
			return true
		}
	}
	return false
}

// Partitioned reports whether tableRef names a PostgreSQL partitioned table in
// this state. The table is placed through [State.TableColumns], so it
// fails closed on the same ambiguity every other lookup does.
func (b State) Partitioned(tableRef string) bool {
	columns := b.TableColumns(tableRef)
	return len(columns) > 0 && columns[0].TablePartitioned
}

// exactTableIndexes is the indexes indexed under exactly tableRef, nil when
// they belong to more than one schema.
func (b State) exactTableIndexes(tableRef string) []lintext.BaselineIndex {
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

// ColumnIndexes returns the baseline indexes with a key part on one column
// of one table, prefix parts included: an index that reads any part of the
// value is rebuilt or copied with it. columnName is the linter's normalized
// form.
func (b State) ColumnIndexes(tableRef, columnName string) []lintext.BaselineIndex {
	var found []lintext.BaselineIndex
	for _, idx := range b.TableIndexes(tableRef) {
		for _, part := range idx.Parts {
			if NormalizeIdent(part.Column) == columnName {
				found = append(found, idx)
				break
			}
		}
	}
	return found
}

// IndexNamed returns the baseline index of one table with the given name,
// compared in the linter's normalized form.
func (b State) IndexNamed(tableRef, name string) (lintext.BaselineIndex, bool) {
	for _, idx := range b.TableIndexes(tableRef) {
		if NormalizeIdent(idx.Name) == name {
			return idx, true
		}
	}
	return lintext.BaselineIndex{}, false
}

// WholeKeyColumns returns the normalized columns an index keys, or nil when
// the key is not a plain column list: a part that is an expression or a
// prefix, or a part the reader could not name, keys something other than the
// column values, so the index says nothing about them.
func WholeKeyColumns(idx lintext.BaselineIndex) []string {
	if idx.Incomplete || len(idx.Parts) == 0 {
		return nil
	}
	columns := make([]string, 0, len(idx.Parts))
	for _, part := range idx.Parts {
		if part.Column == "" || part.Prefix > 0 {
			return nil
		}
		columns = append(columns, NormalizeIdent(part.Column))
	}
	return columns
}

// UniqueCovering returns a baseline index that already proves the given
// columns hold no duplicate: a unique index or primary key over every row,
// keyed by whole columns, each of which is one of the given columns. A key
// over a subset proves the superset unique too. columns are normalized.
func (b State) UniqueCovering(tableRef string, columns []string) (lintext.BaselineIndex, bool) {
	for _, idx := range b.TableIndexes(tableRef) {
		if !idx.Unique || idx.Partial {
			continue
		}
		key := WholeKeyColumns(idx)
		if len(key) == 0 || !SubsetOf(key, columns) {
			continue
		}
		return idx, true
	}
	return lintext.BaselineIndex{}, false
}

// SameColumnSet reports whether two column lists name the same columns,
// in any order.
func SameColumnSet(a, b []string) bool {
	return len(a) == len(b) && SubsetOf(a, b) && SubsetOf(b, a)
}

// SubsetOf reports whether every name of inner is in outer.
func SubsetOf(inner, outer []string) bool {
	for _, name := range inner {
		if !slices.Contains(outer, name) {
			return false
		}
	}
	return true
}

// Empty reports that this state resolves nothing, which is what a version
// the run never read from a dev database gets.
func (b State) Empty() bool {
	return len(b.byRef) == 0
}

// Column returns the baseline state of one column of one table.
//
// tableRef and columnName are the linter's normalized identifier forms. A
// reference that resolves to more than one column -- a bare table name carried
// by several schemas -- is not a match: naming the wrong table's column is worse
// than saying nothing, so ambiguity fails closed.
func (b State) Column(tableRef, columnName string) (lintext.BaselineColumn, bool) {
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
	// [RefersToCreated] makes, and it inherits the same limit: the normalized
	// form of `"tenant.data"` and of `tenant.data` are one string.
	dot := strings.LastIndex(tableRef, ".")
	if dot < 0 {
		return lintext.BaselineColumn{}, false
	}
	return b.exact(tableRef[dot+1:], columnName)
}

// exact is the one column indexed under exactly tableRef and columnName.
func (b State) exact(tableRef, columnName string) (lintext.BaselineColumn, bool) {
	matches := b.byRef[tableRef+"\x00"+columnName]
	if len(matches) != 1 {
		return lintext.BaselineColumn{}, false
	}
	return matches[0], true
}

// normalizeColumns drops entries no lookup can use, so an index never
// carries rows that can only produce a false ambiguity.
func normalizeColumns(columns []lintext.BaselineColumn) []lintext.BaselineColumn {
	var kept []lintext.BaselineColumn
	for _, column := range columns {
		if strings.TrimSpace(column.Table) == "" || strings.TrimSpace(column.Name) == "" {
			continue
		}
		kept = append(kept, column)
	}
	return kept
}

// normalizeHypertables drops the entries no lookup can use, for the
// same reason [normalizeColumns] does.
func normalizeHypertables(hypertables []lintext.BaselineHypertable) []lintext.BaselineHypertable {
	var kept []lintext.BaselineHypertable
	for _, hypertable := range hypertables {
		if strings.TrimSpace(hypertable.Table) == "" {
			continue
		}
		kept = append(kept, hypertable)
	}
	return kept
}

// normalizeIndexes drops the entries no lookup can use, for the same
// reason [normalizeColumns] does.
func normalizeIndexes(indexes []lintext.BaselineIndex) []lintext.BaselineIndex {
	var kept []lintext.BaselineIndex
	for _, idx := range indexes {
		if strings.TrimSpace(idx.Table) == "" || strings.TrimSpace(idx.Name) == "" {
			continue
		}
		kept = append(kept, idx)
	}
	return kept
}
