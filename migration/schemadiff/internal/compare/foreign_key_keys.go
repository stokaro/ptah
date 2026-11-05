package compare

import (
	"slices"
	"strings"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
)

// removedKey is a key a comparison removes or replaces, on its table's identity:
// the index that enforces it and the columns it covers.
type removedKey struct {
	table   string
	name    string
	columns []string
}

// foreignKeysOverRemovedKeys answers the foreign keys both sides hold that depend
// on a key this comparison removes or replaces, on a dialect where such a key
// cannot leave under its foreign key; see
// [ptah.run/core/platform/descriptor.Comparison.ForeignKeyHoldsItsKey].
//
// The foreign key's own definition does not change, and the difference is the
// plan's: it drops the foreign key before the key and adds it back after, in the
// shape of a changed foreign key. The server then binds it to a key the
// declaration holds, so the next comparison finds nothing to do.
//
// The server names the index a foreign key depends on, so where the read carries
// it, another key over the same columns does not rebuild a foreign key built on the
// one that stays. A catalog built from a description carries none, and there the
// columns decide, since a rebuild the server did not need still ends in the same
// state.
func foreignKeysOverRemovedKeys(
	genConstraints map[tableMemberKey]schemamodel.Constraint,
	dbConstraints map[tableMemberKey]catalog.Constraint,
	database *catalog.Database,
	diff *difftypes.SchemaDiff,
	dialect string,
	semantics identifier.Semantics,
	opts *config.CompareOptions,
) map[tableMemberKey]bool {
	if !comparisonOf(dialect).ForeignKeyHoldsItsKey {
		return nil
	}
	keys := removedKeysOf(genConstraints, dbConstraints, database, diff, dialect, semantics, opts)
	if len(keys) == 0 {
		return nil
	}
	held := make(map[tableMemberKey]bool)
	for constraintKey := range genConstraints {
		dbConstraint, exists := dbConstraints[constraintKey]
		if exists && strings.EqualFold(dbConstraint.Type, "FOREIGN KEY") && dependsOnAny(dbConstraint, keys, semantics) {
			held[constraintKey] = true
		}
	}
	return held
}

// removedKeysOf is the PRIMARY KEY and UNIQUE constraints the comparison removes or
// changes, and the unique indexes it removes.
func removedKeysOf(
	genConstraints map[tableMemberKey]schemamodel.Constraint,
	dbConstraints map[tableMemberKey]catalog.Constraint,
	database *catalog.Database,
	diff *difftypes.SchemaDiff,
	dialect string,
	semantics identifier.Semantics,
	opts *config.CompareOptions,
) []removedKey {
	var keys []removedKey
	for constraintKey, dbConstraint := range dbConstraints {
		if !strings.EqualFold(dbConstraint.Type, "PRIMARY KEY") && !strings.EqualFold(dbConstraint.Type, "UNIQUE") {
			continue
		}
		genConstraint, kept := genConstraints[constraintKey]
		if !kept || constraintDefinitionsChanged(genConstraint, dbConstraint, dialect, semantics, opts) {
			keys = append(keys, removedKey{
				table:   semantics.QualifiedTableIdentityKey(dbConstraint.QualifiedTableName()),
				name:    dbConstraint.Name,
				columns: dbConstraint.ColumnNamesOrDefault(),
			})
		}
	}
	for _, ref := range diff.IndexRemovals() {
		table := semantics.QualifiedTableIdentityKey(ref.TableName)
		for _, index := range database.Indexes {
			if index.IsUnique && index.Name == ref.Name && semantics.QualifiedTableIdentityKey(index.QualifiedTableName()) == table {
				keys = append(keys, removedKey{table: table, name: ref.Name, columns: index.Columns})
			}
		}
	}
	return keys
}

// dependsOnAny reports whether foreignKey depends on one of keys, which sit on the
// table it references.
func dependsOnAny(foreignKey catalog.Constraint, keys []removedKey, semantics identifier.Semantics) bool {
	referenced := semantics.QualifiedTableIdentityKey(foreignKey.QualifiedForeignTableName())
	return slices.ContainsFunc(keys, func(key removedKey) bool {
		if key.table != referenced {
			return false
		}
		if foreignKey.ReferencedIndex != "" {
			return key.name == foreignKey.ReferencedIndex
		}
		return sameColumnSet(key.columns, foreignKey.ForeignColumnsOrDefault())
	})
}

// sameColumnSet reports whether two column lists name the same columns in any
// order: a foreign key may list a key's columns in another order than the key does.
func sameColumnSet(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	sortedLeft, sortedRight := slices.Sorted(slices.Values(left)), slices.Sorted(slices.Values(right))
	return slices.Equal(sortedLeft, sortedRight)
}
