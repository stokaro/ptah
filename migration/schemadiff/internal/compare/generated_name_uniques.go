package compare

import (
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
)

// pairGeneratedNameUniques lets a UNIQUE the server named answer for the
// UNIQUE a schema file left unnamed on the same table, over the same columns.
//
// Oracle names an unnamed key itself, SYS_C008753. A key over one column is
// carried on the column's flag and never reaches this map; a key over several
// columns has no flag and is read as a constraint under that name. Left to the
// keys, the declaration that created it plans to add it again, which Oracle
// refuses with ORA-02261, and to drop SYS_C008753.
//
// So a database UNIQUE the reader marks [catalog.Constraint.GeneratedName]
// takes the key of an unnamed declared UNIQUE of the same table over the same
// columns in the same order, with the same enforcement and deferral, where
// neither has a counterpart of its own. A key the author named is never paired
// this way: its name is part of what was declared.
func pairGeneratedNameUniques(
	genConstraints map[tableMemberKey]schemamodel.Constraint,
	dbConstraints map[tableMemberKey]catalog.Constraint,
	semantics identifier.Semantics,
) {
	declared := unmatchedOfType(genConstraints, dbConstraints, "UNIQUE", func(constraint schemamodel.Constraint) bool {
		return strings.TrimSpace(constraint.Name) == "" && len(constraint.Columns) > 0
	})
	generated := unmatchedOfType(dbConstraints, genConstraints, "UNIQUE", func(constraint catalog.Constraint) bool {
		return constraint.GeneratedName
	})
	for _, want := range declared {
		wanted := genConstraints[want]
		for i, have := range generated {
			held := dbConstraints[have]
			if have.table != want.table ||
				wanted.NotEnforced != held.NotEnforced ||
				deferralChanged(wanted, held) ||
				!sameColumns(wanted.Columns, held.ColumnNamesOrDefault(), semantics) {
				continue
			}
			dbConstraints[want] = held
			delete(dbConstraints, have)
			generated = append(generated[:i], generated[i+1:]...)
			break
		}
	}
}

// sameColumns reports whether two column lists name the same columns in the
// same order, by the target's identity rules.
func sameColumns(declared, held []string, semantics identifier.Semantics) bool {
	if len(declared) != len(held) {
		return false
	}
	for i := range declared {
		if semantics.ColumnIdentityKey(declared[i]) != semantics.ColumnIdentityKey(held[i]) {
			return false
		}
	}
	return true
}
