package generator

// Reversing the keys a column's own UNIQUE creates. The forward direction adds
// and drops such a key through the column rather than as a constraint of its
// own, so the plain exchange of the constraint lists does not see it.

import (
	"maps"
	"slices"
	"strings"

	"ptah.run/core/difftypes"
	"ptah.run/core/platform/identifier"
	"ptah.run/internal/columnkey"
	"ptah.run/internal/constraintscope"
)

// reverseColumnKeyRemovals is the removal of each key a column took its own
// UNIQUE under in the forward direction, by the name the diff records for it;
// see [difftypes.TableDiff.ColumnKeyNames]. additions are the rollback's
// constraint additions.
//
// The forward direction adds such a key through the column -- ADD COLUMN ...
// UNIQUE, or the ADD CONSTRAINT a column gaining UNIQUE is planned -- and lists
// no constraint addition for it, so exchanging the constraint lists leaves the
// key out of the rollback. Without this removal a column the rollback takes
// UNIQUE away from keeps its key.
//
// A column the forward direction added is dropped by the rollback, and its key
// goes with it, so its removal is listed only where a constraint the rollback
// adds back on the table takes the key's name. The PostgreSQL planner adds
// constraints before it drops columns, and pairs the two by identity to drop
// the key first; without the pair, the addition is refused while the column
// still holds the name. The MySQL-family planner drops the column first and
// leaves out the drop the column took; see
// [ptah.run/internal/planner/keyrelease.DroppedWithColumns].
//
// The names are read here and not carried into the rollback's table diffs. A
// key the rollback puts back is one the pre-change database held under its
// real name, and the reversal restores it from the removed constraint, which
// carries that name. Measured on PostgreSQL 18.6: a column losing its key
// beside a unique index c_x_key drops c_x_key1 on the way up, and the down
// migration adds c_x_key1 back (stokaro/ptah#3859).
//
// A diff with no recorded names gives none: an engine whose naming Ptah has
// not measured, and a diff built by hand.
func reverseColumnKeyRemovals(
	diff *difftypes.SchemaDiff,
	additions difftypes.ConstraintAdditions,
	dialect string,
	semantics identifier.Semantics,
) difftypes.ConstraintRemovals {
	var removals difftypes.ConstraintRemovals
	for _, table := range diff.TablesModified {
		added := table.ColumnsAdded.Names()
		for _, column := range slices.Sorted(maps.Keys(table.ColumnKeyNames)) {
			name := table.ColumnKeyNames[column]
			if name == "" {
				continue
			}
			if slices.Contains(added, column) && !addsBackName(additions, table.TableName, name, dialect, semantics) {
				continue
			}
			removals = append(removals, difftypes.ConstraintRemovalInfo{
				Name:      name,
				TableName: table.TableName,
				Type:      "UNIQUE",
				Identity:  constraintscope.Identity(semantics, table.TableName, name),
			})
		}
	}
	return removals
}

// addsBackName reports whether an addition puts a constraint named name back
// on table.
func addsBackName(
	additions difftypes.ConstraintAdditions,
	table, name, dialect string,
	semantics identifier.Semantics,
) bool {
	key := semantics.QualifiedTableIdentityKey(table)
	return slices.ContainsFunc(additions, func(addition difftypes.ConstraintAdditionInfo) bool {
		return semantics.QualifiedTableIdentityKey(addition.TableName) == key &&
			columnkey.Same(dialect, addition.Name, name)
	})
}

// dropColumnUniqueRestoredByConstraints takes UNIQUE off a column the rollback
// adds or makes unique again when a reversed constraint addition restores a
// key over that column alone.
//
// The reversal restores every key the forward direction removed by name, with
// the body the pre-change database held; see [reverseConstraintAdditions]. The
// column's UNIQUE would create the same key a second time: inline on ADD
// COLUMN, or as the ADD CONSTRAINT a column gaining UNIQUE is planned. On
// PostgreSQL the second one is refused because the name exists, and where the
// constraint carries another name the column's flag adds a key the database
// never held. The named addition is the one kept, because it carries the name
// and the settings the database read reported.
func dropColumnUniqueRestoredByConstraints(reversed *difftypes.SchemaDiff, semantics identifier.Semantics) {
	restored := make(map[tableMemberKey]struct{})
	for _, addition := range reversed.ConstraintsAdded {
		if !strings.EqualFold(addition.Type, "UNIQUE") || len(addition.Columns) != 1 {
			continue
		}
		restored[columnKeyIdentity(semantics, addition.TableName, addition.Columns[0])] = struct{}{}
	}
	if len(restored) == 0 {
		return
	}
	for i := range reversed.TablesModified {
		table := &reversed.TablesModified[i]
		for j := range table.ColumnsAdded {
			column := &table.ColumnsAdded[j]
			if _, found := restored[columnKeyIdentity(semantics, table.TableName, column.Name)]; found {
				column.Unique = false
			}
		}
		for j := range table.ColumnsModified {
			column := &table.ColumnsModified[j]
			if _, changed := column.Changes["unique"]; !changed {
				continue
			}
			if _, found := restored[columnKeyIdentity(semantics, table.TableName, column.ColumnName)]; found {
				column.Desired.Unique = false
			}
		}
	}
}

// columnKeyIdentity keys a column of a table by the identity the comparison
// pairs the two with.
func columnKeyIdentity(semantics identifier.Semantics, table, column string) tableMemberKey {
	return tableMemberKey{
		table:  semantics.QualifiedTableIdentityKey(table),
		member: semantics.ColumnIdentityKey(column),
	}
}
