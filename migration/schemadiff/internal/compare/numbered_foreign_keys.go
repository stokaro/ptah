package compare

import (
	"cmp"
	"fmt"
	"slices"

	"ptah.run/catalog"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/mysqlname"
	"ptah.run/internal/tableref"
)

// pairNumberedForeignKeys lets a foreign key MariaDB 12.1 or later named `<n>`
// answer for the key a schema file left unnamed.
//
// The SQL reader names an unnamed key of the MySQL family `<table>_ibfk_<n>`,
// the name MySQL and MariaDB through 12.0 give it; see [mysqlname.ForeignKey].
// MariaDB 12.1 and later name the same key `<n>`, and a schema file does not
// say which line it is for. Left to the names, a file compared with the
// database it built on those lines plans to add `c_ibfk_1` and drop `1`, the
// same key under two names (stokaro/ptah#3743).
//
// So a database key with such a name takes the identity of a desired key of the
// same table whose name has the reader's shape, where the two have the same
// definition and neither has a counterpart of its own name. The pairing is by
// definition rather than by number, because the two schemes number one table's
// keys differently; see [mysqlname.IsNumberedForeignKeyName]. A desired key
// whose definition differs keeps its own identity, so the key is replaced, as
// any changed key is.
//
// The desired name may be one the author wrote rather than one the reader
// derived; the model does not say which. Such a key and a database key the
// server numbered are still one key with one definition, so nothing is planned
// for a difference in name alone.
func pairNumberedForeignKeys(
	genConstraints map[tableMemberKey]schemamodel.Constraint,
	dbConstraints map[tableMemberKey]catalog.Constraint,
	dialect string,
	semantics identifier.Semantics,
) {
	desired := unmatchedForeignKeys(genConstraints, dbConstraints, func(constraint schemamodel.Constraint) bool {
		return mysqlname.IsForeignKeyName(bareTableName(constraint.Table), constraint.Name)
	})
	numbered := unmatchedForeignKeys(dbConstraints, genConstraints, func(constraint catalog.Constraint) bool {
		return mysqlname.IsNumberedForeignKeyName(dialect, constraint.Name)
	})
	for _, want := range desired {
		for i, have := range numbered {
			if have.table != want.table ||
				foreignKeyConstraintChanged(genConstraints[want], dbConstraints[have], dialect, semantics) {
				continue
			}
			dbConstraints[want] = dbConstraints[have]
			delete(dbConstraints, have)
			numbered = slices.Delete(numbered, i, i+1)
			break
		}
	}
}

// unmatchedForeignKeys answers the keys of side's foreign keys that other has
// no entry for and that shape accepts, in a fixed order.
func unmatchedForeignKeys[T, U any](
	side map[tableMemberKey]T, other map[tableMemberKey]U, shape func(T) bool,
) []tableMemberKey {
	var keys []tableMemberKey
	for key, constraint := range side {
		if key.memberType != "FOREIGN KEY" || !shape(constraint) {
			continue
		}
		if _, matched := other[key]; matched {
			continue
		}
		keys = append(keys, key)
	}
	slices.SortFunc(keys, func(a, b tableMemberKey) int {
		return cmp.Or(cmp.Compare(fmt.Sprint(a.table), fmt.Sprint(b.table)), cmp.Compare(a.member, b.member))
	})
	return keys
}

// bareTableName is a table reference without its schema.
func bareTableName(qualified string) string {
	if ref, ok := tableref.Parse(qualified); ok {
		return ref.Name
	}
	return qualified
}
