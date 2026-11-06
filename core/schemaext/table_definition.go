package schemaext

import "ptah.run/core/objectidentity"

// TableDefinition is implemented by a table facet value whose kind defines
// what its table is, rather than adding a setting to an ordinary table. The
// owner's declaration, not a column list, recreates such a table. A SQLite
// virtual table is one: CREATE VIRTUAL TABLE names a module, and the module
// answers what the columns are.
//
// A host comparing two states reads the answer in two places. It does not
// compare the columns of a table that either side defines this way, because a
// column statement cannot reach the definition; the owner's facet comparison
// answers for the table instead. And it removes a live table defined this way
// only when [PlansDefinedTableRemoval] says the desired state describes such
// tables: a source with no syntax for the kind has not asked for the table to
// go, and dropping it destroys what it holds.
//
// An owner answers for each representation. Both of a SQLite virtual table's do.
// Only a PostgreSQL partition's declaration does: the catalog lists a partition's
// columns as any table's, and dropping one is an ordinary, visible table drop. A
// table carries at most one value that defines it; an owner refuses a second.
type TableDefinition interface {
	Value
	// DefinesTable reports whether the value defines what its table is.
	DefinesTable() bool
}

// TableParent is implemented by a table facet value whose table cannot exist
// before another table, such as a PostgreSQL partition before its partitioned
// parent. A host creates the parent first.
type TableParent interface {
	Value
	// ParentTable names the other table, its schema empty where the source
	// leaves the name unqualified.
	ParentTable() (schema, name string)
}

// ParentTableOf names the table a value in facets requires, and reports false
// when no value does. Values are asked in kind order and the first one answers.
func ParentTableOf(facets Facets) (schema, name string, found bool) {
	for _, kind := range facets.Kinds() {
		if parent, ok := facets.values[kind].(TableParent); ok {
			schema, name = parent.ParentTable()
			return schema, name, true
		}
	}
	return "", "", false
}

// DefiningKind returns the kind of the value in facets that defines what their
// table is, and false when no value does. Values are asked in kind order and
// the first that answers true is returned.
func DefiningKind(facets Facets) (Kind, bool) {
	for _, kind := range facets.Kinds() {
		if definition, ok := facets.values[kind].(TableDefinition); ok && definition.DefinesTable() {
			return kind, true
		}
	}
	return "", false
}

// PlansDefinedTableRemoval reports whether a live table that a value of kind
// defines is removed when the desired state does not name it. It is removed
// only when desired, the desired state's coverage, describes kind for the
// table as [Complete] or [Absent]. Any other answer, including a kind the
// source never enrolled, keeps the table: silence from a source that cannot
// express the kind, or did not look at it, is not a request to drop it.
func PlansDefinedTableRemoval(desired Coverage, kind Kind, table objectidentity.ID) bool {
	state := desired.Lookup(kind, table).State
	return state == Complete || state == Absent
}
