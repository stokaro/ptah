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
	// NotNull reports whether the column rejects NULL.
	NotNull bool
	// HasDefault reports whether the column carries a DEFAULT expression.
	HasDefault bool
}

// baselineColumns is one version's starting state, indexed for lookup by the
// source-spelled references the linter reads out of SQL.
//
// The zero value resolves nothing, which is what every run without a dev
// database gets and what keeps a rename reported exactly as it was before this
// existed.
type baselineColumns struct {
	byRef map[string][]BaselineColumn
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
	index := map[int64]baselineColumns{}
	for _, column := range columns {
		state, ok := index[column.Version]
		if !ok {
			state = baselineColumns{byRef: map[string][]BaselineColumn{}, schemaless: true}
		}
		if column.Schema != "" {
			state.schemaless = false
		}
		for _, key := range baselineKeys(column) {
			state.byRef[key] = append(state.byRef[key], column)
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

// column returns the baseline state of one column of one table.
//
// tableRef and columnName are the linter's normalized identifier forms. A
// reference that resolves to more than one column -- a bare table name carried
// by several schemas -- is not a match: naming the wrong table's column is worse
// than saying nothing, so ambiguity fails closed.
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

// baselineVersions returns the versions whose starting schema state would let
// the analysis say more than it can from SQL text alone, sorted ascending.
//
// Today that is exactly the versions carrying a column rename the compatibility
// surface models as a drop plus an add: the add side needs the retired column's
// type and nullability, which the statement does not carry. Files whose rename
// is exempt because this same file created the table are not listed -- nothing
// about them is reportable, so reading the database for them would be a round
// trip spent to learn nothing.
func baselineVersions(files []File) []int64 {
	var versions []int64
	for i := range files {
		file := &files[i]
		if !file.Selected || !file.IsUp || file.Version == 0 {
			continue
		}
		if len(renameAddSideCandidates(file)) == 0 {
			continue
		}
		versions = append(versions, file.Version)
	}
	slices.Sort(versions)
	return slices.Compact(versions)
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
