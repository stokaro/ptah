package compare

import (
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
)

// pairGeneratedNameChecks lets a CHECK the server named answer for the CHECK
// a schema file left unnamed on the same table.
//
// Oracle names an unnamed CHECK itself, SYS_C008933, with a number no file can
// predict, and keeps the condition as it was written in
// user_constraints.search_condition. The declared CHECK is keyed by its
// definition and the database one by that name, so left to the keys the
// declaration that created the constraint plans to add it again and drop
// SYS_C008933 on every run.
//
// So a database CHECK the reader marks [catalog.Constraint.GeneratedName]
// takes the key of an unnamed declared CHECK of the same table whose condition
// normalizes to the same text and whose enforcement is the same, where neither
// has a counterpart of its own. A declared condition that differs keeps its own
// key, so the CHECK is replaced, as any changed CHECK is. A CHECK the author
// named is never paired this way: its name is part of what was declared.
func pairGeneratedNameChecks(
	genConstraints map[tableMemberKey]schemamodel.Constraint,
	dbConstraints map[tableMemberKey]catalog.Constraint,
) {
	declared := unmatchedChecks(genConstraints, dbConstraints, func(constraint schemamodel.Constraint) bool {
		return strings.TrimSpace(constraint.Name) == "" && strings.TrimSpace(constraint.CheckExpression) != ""
	})
	generated := unmatchedChecks(dbConstraints, genConstraints, func(constraint catalog.Constraint) bool {
		return constraint.GeneratedName && strings.TrimSpace(getStringValue(constraint.CheckClause)) != ""
	})
	for _, want := range declared {
		wanted := genConstraints[want]
		for i, have := range generated {
			held := dbConstraints[have]
			if have.table != want.table ||
				wanted.NotEnforced != held.NotEnforced ||
				normalizeCheckExpression(wanted.CheckExpression) != normalizeCheckExpression(getStringValue(held.CheckClause)) {
				continue
			}
			dbConstraints[want] = held
			delete(dbConstraints, have)
			generated = append(generated[:i], generated[i+1:]...)
			break
		}
	}
}

// unmatchedChecks answers the keys of side's CHECK constraints that other has
// no entry for and that shape accepts, in a fixed order.
func unmatchedChecks[T, U any](
	side map[tableMemberKey]T, other map[tableMemberKey]U, shape func(T) bool,
) []tableMemberKey {
	return unmatchedOfType(side, other, "CHECK", shape)
}
