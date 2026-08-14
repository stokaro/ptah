// Package sqliterebuild answers the one question two subsystems must not
// answer differently: does converging this table diff need SQLite to rebuild
// the table?
//
// SQLite's ALTER TABLE can rename a table, rename a column, add a column and
// drop a column, and nothing else. Every other shape -- a column's type,
// nullability, default or generated expression, and any table constraint --
// is converged by writing a new table, copying the rows into it, dropping the
// old one and renaming. The SQLite planner does exactly that.
//
// The SQLite virtual-table guard has to predict the same set. It refuses a
// comparison whose database holds a virtual table this build cannot load the
// module for, because a drop or a rebuild aimed at that module's private
// storage destroys the index the storage belongs to (stokaro/ptah#1028).
// Deriving the set a second time lets the two derivations drift, and drift is
// a defect in both directions: refusing what the planner expresses as
// `ALTER TABLE ... ADD COLUMN` takes away a safe scoped change, and missing a
// rebuild lets the destruction the guard exists for through.
//
// So both ask here, and a new [difftypes.TableDiff] field has one place to be
// classified rather than two.
package sqliterebuild

import (
	difftypes "go.5x5.cz/ptah/migration/schemadiff/types"
)

// NeedsTableRebuild reports whether this table diff has to be converged by
// rebuilding the table -- create-new, copy-rows, drop-old, rename -- rather
// than by an ALTER TABLE.
//
// Added columns are the one exception, and they are an exception because
// SQLite has a statement for them: `ALTER TABLE t ADD COLUMN c` adds the
// column in place, touching no other row and no other object. A diff that adds
// columns and changes nothing else is therefore planned directly, and calling
// it a rebuild would be wrong in the direction that costs a capability.
//
// Every other recorded change reaches the rebuild. Removed columns do too,
// even though SQLite gained `DROP COLUMN` in 3.35: the planner rebuilds for
// them so that one sequence covers a diff that removes a column and changes
// another, and this predicate reports what the planner does rather than what
// the grammar would allow.
func NeedsTableRebuild(table difftypes.TableDiff) bool {
	return len(table.ColumnsModified) > 0 ||
		len(table.ColumnsRemoved) > 0 ||
		len(table.ConstraintsAdded) > 0 ||
		len(table.ConstraintsRemoved) > 0
}
