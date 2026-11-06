package compare

import (
	"maps"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
)

// renamedPrimaryKeys pairs each declared primary key with the live key it is under
// another name, keyed by the live key's table and name: the two sit on one table,
// and nothing but the name differs. A dialect that renames no key answers nil, and
// its key is rebuilt.
func renamedPrimaryKeys(
	genConstraints map[tableMemberKey]schemamodel.Constraint,
	dbConstraints map[tableMemberKey]catalog.Constraint,
	dialect string,
	semantics identifier.Semantics,
) map[tableMemberKey]schemamodel.Constraint {
	if !comparisonOf(dialect).PrimaryKeyRenames {
		return nil
	}
	renamed := make(map[tableMemberKey]schemamodel.Constraint)
	for declaredKey, declared := range genConstraints {
		if !isPrimaryKey(declared.Type) {
			continue
		}
		if _, held := dbConstraints[declaredKey]; held {
			continue
		}
		identity := newQualifiedTableIdentity(declared.Table, semantics)
		for liveKey, live := range dbConstraints {
			if isPrimaryKey(live.Type) && newQualifiedTableIdentity(live.QualifiedTableName(), semantics) == identity &&
				!primaryKeyConstraintChanged(declared, live, dialect, semantics) {
				renamed[liveKey] = declared
			}
		}
	}
	return renamed
}

// markRenamedPrimaryKeys records on each addition the live key it renames.
func markRenamedPrimaryKeys(
	additions difftypes.ConstraintAdditions,
	renamed map[tableMemberKey]schemamodel.Constraint,
	dbConstraints map[tableMemberKey]catalog.Constraint,
	semantics identifier.Semantics,
) {
	for liveKey, declared := range renamed {
		identity := constraintIdentity(declared.Table, declared.Name, semantics)
		for i := range additions {
			if additions[i].Identity == identity {
				additions[i].RenamedFrom = dbConstraints[liveKey].Name
			}
		}
	}
}

// constraintsLessRenames is dbConstraints without the keys renamed takes. A renamed
// key stays where it is, so a foreign key over it stays too.
func constraintsLessRenames(
	dbConstraints map[tableMemberKey]catalog.Constraint,
	renamed map[tableMemberKey]schemamodel.Constraint,
) map[tableMemberKey]catalog.Constraint {
	if len(renamed) == 0 {
		return dbConstraints
	}
	kept := maps.Clone(dbConstraints)
	for liveKey := range renamed {
		delete(kept, liveKey)
	}
	return kept
}

func isPrimaryKey(constraintType string) bool {
	return strings.EqualFold(strings.TrimSpace(constraintType), "PRIMARY KEY")
}
