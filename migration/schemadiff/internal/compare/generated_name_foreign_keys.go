package compare

import (
	"slices"

	"ptah.run/catalog"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
)

// pairGeneratedNameForeignKeys lets a foreign key the server named answer for
// the foreign key a schema file left unnamed on the same table.
//
// Oracle names an unnamed key itself, SYS_C009299, and the declared key takes
// a name Ptah derived from its columns, fk_ch_x_y. Keyed by those names the two
// never meet, so the plan adds fk_ch_x_y and drops SYS_C009299 on every run.
// Oracle also holds one foreign key per column list and reference, so the ADD
// answers ORA-02275 while the server's key stands and the plan never applies.
//
// So a database foreign key the reader marks
// [catalog.Constraint.GeneratedName] takes the key of a declared foreign key
// of the same table whose name Ptah derived ([schemamodel.Constraint.NameDerived])
// and that has the same columns and the same reference, where neither has a
// counterpart of its own. The rest of the definition is left to the
// comparison: a key with another delete rule, update rule or deferral is
// replaced, as any changed key is. A name the author wrote is never paired
// this way, because the name is part of what was declared.
func pairGeneratedNameForeignKeys(
	genConstraints map[tableMemberKey]schemamodel.Constraint,
	dbConstraints map[tableMemberKey]catalog.Constraint,
	semantics identifier.Semantics,
) {
	declared := unmatchedForeignKeys(genConstraints, dbConstraints, func(constraint schemamodel.Constraint) bool {
		return constraint.NameDerived
	})
	generated := unmatchedForeignKeys(dbConstraints, genConstraints, func(constraint catalog.Constraint) bool {
		return constraint.GeneratedName
	})
	for _, want := range declared {
		wanted := genConstraints[want]
		for i, have := range generated {
			held := dbConstraints[have]
			if have.table != want.table || !sameForeignKeyReference(wanted, held, semantics) {
				continue
			}
			dbConstraints[want] = held
			delete(dbConstraints, have)
			generated = slices.Delete(generated, i, i+1)
			break
		}
	}
}
