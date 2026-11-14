package lintkit

import (
	"slices"
	"strings"

	"ptah.run/core/lintext"
)

// State is one version's starting state, indexed for lookup by the
// source-spelled references the linter reads out of SQL.
//
// The zero value is a state nobody read, which is what every run without a dev
// database gets: it resolves nothing and answers no question about absence. A
// state that was read and holds nothing is not the zero value; see
// [NewStates].
type State struct {
	// read reports that the run read this state, so an object it does not
	// hold is absent rather than unknown.
	read  bool
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
	// byIndexName holds every index under the spellings of its own name,
	// bare and qualified by its schema, for the lookups that name an index
	// without its table.
	byIndexName map[string][]lintext.BaselineIndex
	// byObject holds every object that stands apart from a table, under its
	// kind and the spellings of its name, and a trigger under its table's
	// spellings and its name as well.
	byObject map[objectKey][]lintext.BaselineObject
	// listed are the kinds of object the read lists in full, so an object of
	// one of them the state does not hold is absent.
	listed map[lintext.BaselineObjectKind]bool
	// schemas holds every schema a row of the state names, in the linter's
	// normalized form.
	schemas map[string]bool
	// schemaless reports that no row in this state names a schema. A reader
	// scoped to one schema does not repeat that schema's name on every table, so
	// a migration writing `ALTER TABLE public.users` has a qualifier the state
	// cannot match literally. Nothing in the state can contradict that qualifier
	// either, which is what makes dropping it safe here and not once the state
	// carries several schemas.
	schemaless bool
}

// objectKey is where [State] files a [lintext.BaselineObject]: its kind and
// one spelling of its name, or of its table and its name for a trigger.
type objectKey struct {
	kind lintext.BaselineObjectKind
	ref  string
}

// NewStates indexes a baseline's columns, indexes, hypertables and objects by
// the version whose starting state each describes. A row no lookup can use --
// a column or an index without a table or a name, a hypertable without a
// table, an object without a kind or a name -- is left out, so it cannot make
// a reference look ambiguous. The dependents are not indexed here.
//
// A version [lintext.Baseline.Reads] lists that has no row is a state that was
// read and holds nothing: the version starts from an empty schema. A version
// with rows was read whether or not it is listed, though it lists no kind of
// object in full unless its read says so. Any other version has the zero
// State.
func NewStates(baseline lintext.Baseline) map[int64]State {
	columns := normalizeColumns(baseline.Columns)
	indexes := normalizeIndexes(baseline.Indexes)
	hypertables := normalizeHypertables(baseline.Hypertables)
	objects := normalizeObjects(baseline.Objects)
	if len(columns) == 0 && len(indexes) == 0 && len(hypertables) == 0 && len(objects) == 0 && len(baseline.Reads) == 0 {
		return nil
	}
	index := make(map[int64]State)
	stateOf := func(version int64) State {
		state, ok := index[version]
		if !ok {
			state = State{
				read:           true,
				byRef:          make(map[string][]lintext.BaselineColumn),
				byTable:        make(map[string][]lintext.BaselineColumn),
				byTableIndexes: make(map[string][]lintext.BaselineIndex),
				hypertables:    make(map[string][]lintext.BaselineHypertable),
				byIndexName:    make(map[string][]lintext.BaselineIndex),
				byObject:       make(map[objectKey][]lintext.BaselineObject),
				listed:         make(map[lintext.BaselineObjectKind]bool),
				schemas:        make(map[string]bool),
				schemaless:     true,
			}
		}
		return state
	}
	// nameSchema records the schema a row names, which is what the state
	// takes a qualified reference to that schema as read from.
	nameSchema := func(state *State, schema string) {
		if schema != "" {
			state.schemaless = false
			state.schemas[NormalizeIdent(schema)] = true
		}
	}
	for _, read := range baseline.Reads {
		state := stateOf(read.Version)
		for _, kind := range read.Listed {
			state.listed[kind] = true
		}
		index[read.Version] = state
	}
	for _, column := range columns {
		state := stateOf(column.Version)
		nameSchema(&state, column.Schema)
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
		nameSchema(&state, idx.Schema)
		for _, key := range baselineTableKeys(idx.Schema, idx.Table) {
			state.byTableIndexes[key] = append(state.byTableIndexes[key], idx)
		}
		for _, key := range baselineTableKeys(idx.Schema, idx.Name) {
			state.byIndexName[key] = append(state.byIndexName[key], idx)
		}
		index[idx.Version] = state
	}
	for _, hypertable := range hypertables {
		state := stateOf(hypertable.Version)
		nameSchema(&state, hypertable.Schema)
		for _, key := range baselineTableKeys(hypertable.Schema, hypertable.Table) {
			state.hypertables[key] = append(state.hypertables[key], hypertable)
		}
		index[hypertable.Version] = state
	}
	for _, object := range objects {
		state := stateOf(object.Version)
		nameSchema(&state, object.Schema)
		for _, key := range objectKeys(object) {
			state.byObject[key] = append(state.byObject[key], object)
		}
		index[object.Version] = state
	}
	return index
}

// objectKeys are the places [State] files one object under: the spellings of
// its name, and for a trigger on a table the spellings of that table joined to
// its name, since PostgreSQL names a trigger only within its table.
func objectKeys(object lintext.BaselineObject) []objectKey {
	var keys []objectKey
	for _, ref := range baselineTableKeys(object.Schema, object.Name) {
		keys = append(keys, objectKey{kind: object.Kind, ref: ref})
	}
	if object.Table == "" {
		return keys
	}
	name := NormalizeIdent(object.Name)
	for _, table := range baselineTableKeys(object.Schema, object.Table) {
		keys = append(keys, objectKey{kind: object.Kind, ref: table + "\x00" + name})
	}
	return keys
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

// LacksTable reports whether the state was read and holds no table tableRef
// can name, so a statement that drops the table only if it exists drops
// nothing. It answers false wherever the state cannot say: a version the run
// never read, and a schema-qualified reference to a schema the state does not
// hold, which a reader scoped to another schema leaves out. A state of one
// schema whose reader named none cannot tell its schema from another, so it
// answers false for every qualified reference.
//
// The state reads tables through their columns, so a table with no column is
// not one it holds.
func (b State) LacksTable(tableRef string) bool {
	return lacks(b, b.byTable, tableRef, tableRef)
}

// LacksIndex reports whether the state was read and holds no index the
// reference indexRef can name, in any table, so a statement that drops the
// index only if it exists drops nothing. indexRef is the index's own name,
// qualified by a schema or not, in the linter's normalized form. It answers
// false wherever the state cannot say, as [State.LacksTable] does.
func (b State) LacksIndex(indexRef string) bool {
	return lacks(b, b.byIndexName, indexRef, indexRef)
}

// LacksObject reports whether the state was read, its read lists objects of
// kind in full, and it holds no such object the reference ref can name, so a
// statement that drops the object only if it exists drops nothing. ref is the
// object's name in the linter's normalized form. tableRef is the table a
// trigger is named within, as PostgreSQL writes `DROP TRIGGER name ON table`,
// and empty where the object is named on its own; a qualifier belongs to
// tableRef then, and to ref otherwise. It answers false wherever the state
// cannot say, as [State.LacksTable] does, and for every kind the read does
// not list in full.
func (b State) LacksObject(kind lintext.BaselineObjectKind, ref, tableRef string) bool {
	if !b.listed[kind] || ref == "" {
		return false
	}
	if tableRef == "" {
		return lacks(b, b.byObject, objectKey{kind: kind, ref: ref}, ref)
	}
	return lacks(b, b.byObject, objectKey{kind: kind, ref: tableRef + "\x00" + ref}, tableRef)
}

// Object returns the object of kind that the reference ref names, resolving
// the reference the way [State.TableColumns] does and failing closed on the
// same ambiguity. The object is a copy the caller may change. It reports
// false for an object the state does not hold, which shows the object absent
// only where [State.LacksObject] says so. A trigger is found by its name
// alone.
func (b State) Object(kind lintext.BaselineObjectKind, ref string) (lintext.BaselineObject, bool) {
	if ref == "" {
		return lintext.BaselineObject{}, false
	}
	object, ok := b.exactObject(kind, ref)
	if !ok && b.schemaless {
		if dot := strings.LastIndex(ref, "."); dot >= 0 {
			object, ok = b.exactObject(kind, ref[dot+1:])
		}
	}
	object.Values = slices.Clone(object.Values)
	return object, ok
}

// EnumValues returns the labels of the enum type typeRef names, in their
// declared order, resolving the reference as [State.Object] does. It reports
// false for a type the state does not hold as an enum.
func (b State) EnumValues(typeRef string) ([]string, bool) {
	typed, ok := b.Object(lintext.BaselineType, typeRef)
	if !ok || !typed.Enum {
		return nil, false
	}
	return typed.Values, true
}

// exactObject is the one object of kind indexed under exactly ref.
func (b State) exactObject(kind lintext.BaselineObjectKind, ref string) (lintext.BaselineObject, bool) {
	matches := b.byObject[objectKey{kind: kind, ref: ref}]
	if len(matches) != 1 {
		return lintext.BaselineObject{}, false
	}
	return matches[0], true
}

// lacks is the answer [State.LacksTable], [State.LacksIndex] and
// [State.LacksObject] share over the entries of one kind, filed under key.
// qualified is the reference whose schema qualifier, if any, decides whether
// the state read the schema the entry would belong to.
func lacks[K comparable, V any](b State, byName map[K][]V, key K, qualified string) bool {
	if b.Unread() || qualified == "" || len(byName[key]) > 0 {
		return false
	}
	dot := strings.LastIndex(qualified, ".")
	if dot < 0 {
		return true
	}
	if b.schemaless {
		return false
	}
	// The schema is held when any row of the state names it: the reader that
	// produced the row read every object of that schema.
	return b.schemas[qualified[:dot]]
}

// NamesSchemas reports a state that was read and whose rows name the schema
// each belongs to, so a reference qualified by a schema is matched as it is
// written. A state read from one schema names none, and neither does one that
// holds nothing.
func (b State) NamesSchemas() bool {
	return b.read && !b.schemaless
}

// Unread reports that the run read no state for this version, which is what
// a version a run without a dev database gets. A state that was read and holds
// nothing is not unread: the version starts from an empty schema.
func (b State) Unread() bool {
	return !b.read
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

// normalizeObjects drops the entries no lookup can use, for the same reason
// [normalizeColumns] does.
func normalizeObjects(objects []lintext.BaselineObject) []lintext.BaselineObject {
	var kept []lintext.BaselineObject
	for _, object := range objects {
		if object.Kind == "" || strings.TrimSpace(object.Name) == "" {
			continue
		}
		kept = append(kept, object)
	}
	return kept
}
