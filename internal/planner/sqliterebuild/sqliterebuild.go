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
	"slices"

	"ptah.run/core/schemaext"
	"ptah.run/migration/schemadiff/difftypes"
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
// Every other recorded change reaches the rebuild, but a column whose comment
// is all that changed does not. SQLite stores no comment, so a rebuild would
// create the same table again and the next comparison would find the same
// difference: measured on SQLite 3.51, a declared column comment rebuilt the
// table on every diff. Removed columns reach it too, even though SQLite gained
// `DROP COLUMN` in 3.35: the planner rebuilds for them so that one sequence
// covers a diff that removes a column and changes another, and this predicate
// reports what the planner does rather than what the grammar would allow.
//
// A change of the table's options, STRICT and WITHOUT ROWID, reaches it too:
// SQLite turns neither on or off for a table that exists, and the rebuild
// creates the table again with the declared ones.
func NeedsTableRebuild(table difftypes.TableDiff) bool {
	return slices.ContainsFunc(table.ColumnsModified, changesMoreThanTheComment) ||
		len(table.ColumnsRemoved) > 0 ||
		len(table.ConstraintsAdded) > 0 ||
		len(table.ConstraintsRemoved) > 0 ||
		slices.ContainsFunc(table.FeatureChanges, IsTableRebuildChange)
}

// TableRebuild is implemented by a feature change its owner converges by
// having the SQLite planner rebuild the table the change is attached to: the
// rebuilt table is created from the declaration, so it takes the declared
// setting. A change of a table's STRICT or WITHOUT ROWID option is one. The
// owner says so on its change, so the planner and the packages that reach it
// name no owner.
type TableRebuild interface {
	schemaext.ChangeValue
	// RebuildsSQLiteTable reports whether rebuilding the table converges the
	// change.
	RebuildsSQLiteTable() bool
}

// IsTableRebuildChange reports whether a table's feature change is a
// [TableRebuild] that rebuilding the table converges.
func IsTableRebuildChange(record schemaext.ChangeRecord) bool {
	rebuild, ok := record.Value.(TableRebuild)
	return ok && rebuild.RebuildsSQLiteTable()
}

// changesMoreThanTheComment reports whether a column change carries anything
// but a comment.
func changesMoreThanTheComment(column difftypes.ColumnDiff) bool {
	return column.CommentChange == nil || len(column.Changes) > 0 || column.NotNullConstraintNameChange != nil
}
