package compare

import (
	"cmp"
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
	"ptah.run/internal/mysqlname"
)

// ForeignKeyBackingIndexesLeaving adds to the removed indexes the one the server
// built for each foreign key the plan removes, on a target whose server keeps that
// index after the key is gone. While the key stands the comparison leaves the index
// to it. Measured on MySQL 8.4 and MariaDB 12.3, DROP FOREIGN KEY leaves the index,
// the comparison then finds an index nobody declares, and the next run plans
// DROP INDEX, so a plan that removes a foreign key could not converge in one run.
//
// An index stays when the declaration holds it, when a foreign key the plan adds is
// built on it, and when its table leaves anyway. A foreign key the plan keeps may
// stand on it too, and [ForeignKeyIndexesStaying] gives that key an index of its own.
// It runs after the constraints are compared.
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
	declared := generatedIndexIdentities(desired, semantics)
	for _, constraint := range database.Constraints {
		if !strings.EqualFold(constraint.Type, "FOREIGN KEY") || !removed[constraintIdentity(constraint.QualifiedTableName(), constraint.Name, semantics)] {
			continue
		}
		for _, backing := range foreignKeys.BackingIndexes(database, constraint, semantics) {
			ref := difftypes.IndexRef{Name: backing.Name, TableName: backing.QualifiedTableName()}
			identity := indexscope.IdentityKeyWithSemantics(semantics, ref)
			_, isDeclared := declared[identity]
			if isDeclared || tableLeaves(diff, ref.TableName, semantics) || addedForeignKeyUses(diff, backing, semantics) ||
				columnsLeave(diff, backing, semantics) ||
				slices.ContainsFunc(diff.IndexesRemoved, func(other difftypes.IndexRef) bool {
					return indexscope.IdentityKeyWithSemantics(semantics, other) == identity
				}) {
				continue
			}
			diff.IndexesRemoved = append(diff.IndexesRemoved, ref)
		}
	}
}

// ForeignKeyIndexesStaying adds an index for each foreign key the plan keeps when the
// plan removes every index the key could stand on, on a target whose server keeps an
// index for every foreign key. Measured on MySQL 8.4 and MariaDB 12.3, DROP INDEX of
// the last such index answers Error 1553, whoever built the index and whatever it is
// called: the index of a key the plan removes that another key shares, an index the
// author named, a unique one. The server takes the drop once another index begins
// with the key's columns.
//
// The removal stays, so the index the declaration leaves out still leaves, and the new
// index goes under the name the server gives the index it builds for the key, so the
// next comparison reads it as the key's. A wider key goes first, because the index it
// gets serves a narrower key over the same leading columns too. A name a live index
// that stays holds keeps the addition out, and the server's refusal names the reason.
// It runs after the constraints and the indexes are compared.
func ForeignKeyIndexesStaying(database *catalog.Database, diff *difftypes.SchemaDiff, dialect string, semantics identifier.Semantics) {
	described, _ := platform.Lookup(dialect)
	if described.ForeignKeyNaming != descriptor.ForeignKeyNamingTableNumbered || database == nil {
		return
	}
	removedConstraints := make(map[difftypes.ConstraintIdentity]bool, len(diff.ConstraintsRemoved))
	removedUniques := make(map[difftypes.ConstraintIdentity]bool)
	for _, info := range diff.ConstraintsRemoved {
		removedConstraints[info.Identity] = true
		if strings.EqualFold(info.Type, "UNIQUE") {
			removedUniques[info.Identity] = true
		}
	}
	removedIndexes := make(map[difftypes.IndexRef]difftypes.IndexRef, len(diff.IndexesRemoved))
	for _, ref := range diff.IndexesRemoved {
		removedIndexes[indexscope.IdentityKeyWithSemantics(semantics, ref)] = ref
	}
	leaves := func(index catalog.Index) bool {
		identity := indexscope.IdentityKeyWithSemantics(semantics, difftypes.IndexRef{Name: index.Name, TableName: index.QualifiedTableName()})
		_, removed := removedIndexes[identity]
		return removed || removedConstraints[constraintIdentity(index.QualifiedTableName(), index.Name, semantics)]
	}
	keys := slices.Clone(database.Constraints)
	slices.SortStableFunc(keys, func(a, b catalog.Constraint) int {
		return cmp.Compare(len(b.ColumnNamesOrDefault()), len(a.ColumnNamesOrDefault()))
	})
	added := false
	for _, key := range keys {
		if !strings.EqualFold(key.Type, "FOREIGN KEY") || removedConstraints[constraintIdentity(key.QualifiedTableName(), key.Name, semantics)] ||
			tableLeaves(diff, key.QualifiedTableName(), semantics) {
			continue
		}
		under := indexesUnder(database, key, semantics)
		if len(under) == 0 || !allOf(under, leaves) || planAddsIndexUnder(diff, key, semantics) {
			continue
		}
		name := keptKeyIndexName(database, key, described, semantics)
		if nameStays(database, key, name, leaves, semantics) {
			continue
		}
		change := difftypes.IndexChange{
			TableName: key.QualifiedTableName(),
			Index:     schemamodel.Index{Name: name, Fields: slices.Clone(key.ColumnNamesOrDefault())},
		}
		for _, index := range under {
			ref := difftypes.IndexRef{Name: index.Name, TableName: index.QualifiedTableName()}
			if removed, listed := removedIndexes[indexscope.IdentityKeyWithSemantics(semantics, ref)]; listed {
				change.Replaces = append(change.Replaces, removed)
			} else if removedUniques[constraintIdentity(ref.TableName, ref.Name, semantics)] {
				change.Replaces = append(change.Replaces, ref)
			}
		}
		diff.IndexesAdded = append(diff.IndexesAdded, change)
		added = true
	}
	if added {
		diff.SetIndexAdditions(diff.IndexesAdded)
	}
}

// allOf reports whether every index satisfies leaves.
func allOf(indexes []catalog.Index, leaves func(catalog.Index) bool) bool {
	return !slices.ContainsFunc(indexes, func(index catalog.Index) bool { return !leaves(index) })
}

// nameStays reports whether an index of key's table that the plan leaves holds name.
func nameStays(database *catalog.Database, key catalog.Constraint, name string, leaves func(catalog.Index) bool, semantics identifier.Semantics) bool {
	table := semantics.QualifiedTableIdentityKey(key.QualifiedTableName())
	return slices.ContainsFunc(database.Indexes, func(index catalog.Index) bool {
		return semantics.QualifiedTableIdentityKey(index.QualifiedTableName()) == table &&
			semantics.IndexIdentityKey(index.Name) == semantics.IndexIdentityKey(name) && !leaves(index)
	})
}

// indexesUnder returns the indexes of key's table that begin with key's columns.
func indexesUnder(database *catalog.Database, key catalog.Constraint, semantics identifier.Semantics) []catalog.Index {
	table := semantics.QualifiedTableIdentityKey(key.QualifiedTableName())
	var indexes []catalog.Index
	for _, index := range database.Indexes {
		if semantics.QualifiedTableIdentityKey(index.QualifiedTableName()) == table && startsWith(index, key.ColumnNamesOrDefault()) {
			indexes = append(indexes, index)
		}
	}
	return indexes
}

// planAddsIndexUnder reports whether the plan adds an index or a key that begins with
// key's columns on its table.
func planAddsIndexUnder(diff *difftypes.SchemaDiff, key catalog.Constraint, semantics identifier.Semantics) bool {
	table := semantics.QualifiedTableIdentityKey(key.QualifiedTableName())
	columns := key.ColumnNamesOrDefault()
	begins := func(held []string) bool {
		return len(columns) <= len(held) && slices.Equal(held[:len(columns)], columns)
	}
	return slices.ContainsFunc(diff.IndexesAdded, func(change difftypes.IndexChange) bool {
		return semantics.QualifiedTableIdentityKey(change.TableName) == table && begins(change.Index.Fields)
	}) || slices.ContainsFunc(diff.ConstraintsAdded, func(add difftypes.ConstraintAdditionInfo) bool {
		return !strings.EqualFold(add.Type, "CHECK") && semantics.QualifiedTableIdentityKey(add.TableName) == table && begins(add.Columns)
	})
}

// keptKeyIndexName names the index for key as the server names the one it builds: the
// key's name for a key written with one, and otherwise the key's first column, with
// the suffix the server adds when that is taken.
func keptKeyIndexName(database *catalog.Database, key catalog.Constraint, described descriptor.Dialect, semantics identifier.Semantics) string {
	if !mysqlname.IsServerForeignKeyName(described.ForeignKeyNameRules, key.TableName, key.Name) {
		return key.Name
	}
	table := semantics.QualifiedTableIdentityKey(key.QualifiedTableName())
	return mysqlname.IndexName(described.IndexNaming.BaseBytes, key.ColumnNamesOrDefault()[0], func(name string) bool {
		return slices.ContainsFunc(database.Indexes, func(index catalog.Index) bool {
			return semantics.QualifiedTableIdentityKey(index.QualifiedTableName()) == table &&
				semantics.IndexIdentityKey(index.Name) == semantics.IndexIdentityKey(name)
		})
	})
}

// startsWith reports whether columns lead index, so a foreign key over them can
// stand on it.
func startsWith(index catalog.Index, columns []string) bool {
	return len(columns) > 0 && len(columns) <= len(index.Columns) && slices.Equal(index.Columns[:len(columns)], columns)
}

// tableLeaves reports whether the plan removes table.
func tableLeaves(diff *difftypes.SchemaDiff, table string, semantics identifier.Semantics) bool {
	return slices.ContainsFunc(diff.TablesRemoved.Names(), func(name string) bool {
		return semantics.QualifiedTableIdentityKey(name) == semantics.QualifiedTableIdentityKey(table)
	})
}

// addedForeignKeyUses reports whether a foreign key the plan adds starts with the
// columns of index. The server builds it on that index rather than a new one, so the
// index's drop would answer that a foreign key needs it.
func addedForeignKeyUses(diff *difftypes.SchemaDiff, index catalog.Index, semantics identifier.Semantics) bool {
	return slices.ContainsFunc(diff.ConstraintsAdded, func(add difftypes.ConstraintAdditionInfo) bool {
		return strings.EqualFold(add.Type, "FOREIGN KEY") &&
			semantics.QualifiedTableIdentityKey(add.TableName) == semantics.QualifiedTableIdentityKey(index.QualifiedTableName()) &&
			startsWith(index, add.Columns)
	})
}

// columnsLeave reports whether the plan drops every column of index. The server
// drops an index with its last column, so a drop of the index after that answers
// that it does not exist.
func columnsLeave(diff *difftypes.SchemaDiff, index catalog.Index, semantics identifier.Semantics) bool {
	for _, table := range diff.TablesModified {
		if semantics.QualifiedTableIdentityKey(table.TableName) != semantics.QualifiedTableIdentityKey(index.QualifiedTableName()) {
			continue
		}
		return !slices.ContainsFunc(index.Columns, func(column string) bool {
			return !slices.ContainsFunc(table.ColumnsRemoved, func(removed schemamodel.Field) bool {
				return semantics.ColumnIdentityKey(removed.Name) == semantics.ColumnIdentityKey(column)
			})
		})
	}
	return false
}
