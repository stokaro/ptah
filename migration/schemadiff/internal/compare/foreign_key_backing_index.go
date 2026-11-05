package compare

import (
	"slices"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform"
	"ptah.run/core/platform/descriptor"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacomparison"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/indexscope"
)

// ForeignKeyBackingIndexesLeaving adds to the removed indexes the one the server
// built for each foreign key the plan removes, on a target whose server keeps that
// index after the key is gone. While the key stands the comparison leaves the index
// to it. Measured on MySQL 8.4 and MariaDB 12.3, DROP FOREIGN KEY leaves the index,
// the comparison then finds an index nobody declares, and the next run plans
// DROP INDEX, so a plan that removes a foreign key could not converge in one run.
//
// An index stays when the declaration holds it, when a foreign key the plan keeps
// or adds is built on it, and when its table leaves anyway. It runs after the
// constraints are compared.
func ForeignKeyBackingIndexesLeaving(
	desired *schemamodel.Database,
	database *catalog.Database,
	diff *difftypes.SchemaDiff,
	foreignKeys schemacomparison.ForeignKeyIndexes,
	dialect string,
	semantics identifier.Semantics,
) {
	// Spanner reports a backing index too, and drops it with the key.
	if described, _ := platform.Lookup(dialect); described.ForeignKeyNaming != descriptor.ForeignKeyNamingTableNumbered {
		return
	}
	if foreignKeys == nil || database == nil || len(diff.ForeignKeysRemovedWithTables) == 0 {
		return
	}
	removed := make(map[difftypes.ConstraintIdentity]bool, len(diff.ForeignKeysRemovedWithTables))
	for _, info := range diff.ForeignKeysRemovedWithTables {
		removed[info.Identity] = true
	}
	for _, add := range diff.ConstraintsAdded {
		delete(removed, add.Identity)
	}
	declared := generatedIndexIdentities(desired, semantics)
	var leaving []difftypes.IndexRef
	kept := make(map[difftypes.IndexRef]bool)
	for _, constraint := range database.Constraints {
		if !strings.EqualFold(constraint.Type, "FOREIGN KEY") {
			continue
		}
		for _, backing := range foreignKeys.BackingIndexes(database, constraint, semantics) {
			ref := difftypes.IndexRef{Name: backing.Name, TableName: backing.QualifiedTableName()}
			if removed[constraintIdentity(constraint.QualifiedTableName(), constraint.Name, semantics)] {
				leaving = append(leaving, ref)
			} else {
				kept[indexscope.IdentityKeyWithSemantics(semantics, ref)] = true
			}
		}
	}
	for _, ref := range leaving {
		identity := indexscope.IdentityKeyWithSemantics(semantics, ref)
		_, isDeclared := declared[identity]
		columns := indexColumns(database, ref, semantics)
		if isDeclared || kept[identity] || tableLeaves(diff, ref.TableName, semantics) ||
			addedForeignKeyUses(diff, columns, ref, semantics) || columnsLeave(diff, columns, ref, semantics) ||
			slices.ContainsFunc(diff.IndexesRemoved, func(other difftypes.IndexRef) bool {
				return indexscope.IdentityKeyWithSemantics(semantics, other) == identity
			}) {
			continue
		}
		diff.IndexesRemoved = append(diff.IndexesRemoved, ref)
	}
}

// tableLeaves reports whether the plan removes table.
func tableLeaves(diff *difftypes.SchemaDiff, table string, semantics identifier.Semantics) bool {
	return slices.ContainsFunc(diff.TablesRemoved.Names(), func(name string) bool {
		return semantics.QualifiedTableIdentityKey(name) == semantics.QualifiedTableIdentityKey(table)
	})
}

// indexColumns is the columns of the index ref, in key order.
func indexColumns(database *catalog.Database, ref difftypes.IndexRef, semantics identifier.Semantics) []string {
	for _, index := range database.Indexes {
		if index.Name == ref.Name && semantics.QualifiedTableIdentityKey(index.QualifiedTableName()) == semantics.QualifiedTableIdentityKey(ref.TableName) {
			return index.Columns
		}
	}
	return nil
}

// addedForeignKeyUses reports whether a foreign key the plan adds starts with
// columns, those of the index ref. The server builds it on that index rather than a
// new one, so the index's drop would answer that a foreign key needs it.
func addedForeignKeyUses(diff *difftypes.SchemaDiff, columns []string, ref difftypes.IndexRef, semantics identifier.Semantics) bool {
	return slices.ContainsFunc(diff.ConstraintsAdded, func(add difftypes.ConstraintAdditionInfo) bool {
		return strings.EqualFold(add.Type, "FOREIGN KEY") &&
			semantics.QualifiedTableIdentityKey(add.TableName) == semantics.QualifiedTableIdentityKey(ref.TableName) &&
			len(add.Columns) <= len(columns) && slices.Equal(columns[:len(add.Columns)], add.Columns)
	})
}

// columnsLeave reports whether the plan drops every one of columns, those of the
// index ref. The server drops an index with its last column, so a drop of the index
// after that answers that it does not exist.
func columnsLeave(diff *difftypes.SchemaDiff, columns []string, ref difftypes.IndexRef, semantics identifier.Semantics) bool {
	for _, table := range diff.TablesModified {
		if semantics.QualifiedTableIdentityKey(table.TableName) != semantics.QualifiedTableIdentityKey(ref.TableName) {
			continue
		}
		return !slices.ContainsFunc(columns, func(column string) bool {
			return !slices.ContainsFunc(table.ColumnsRemoved, func(removed schemamodel.Field) bool {
				return semantics.ColumnIdentityKey(removed.Name) == semantics.ColumnIdentityKey(column)
			})
		})
	}
	return false
}
