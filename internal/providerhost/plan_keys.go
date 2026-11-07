package providerhost

import (
	"fmt"
	"strings"

	"ptah.run/core/difftypes"
	"ptah.run/core/objectidentity"
)

// ownsKeys reports whether the provider plans a change of the primary key of
// the tables it owns: it reports the key a table holds, so the comparison has
// a current side, and it states that it plans the change it is handed.
func (p commonPlanner) ownsKeys() bool {
	return p.ownsColumns() && p.observedKeys && p.keys
}

// ownsConstraints reports whether the provider plans the foreign key and
// CHECK changes of the tables it owns: it reports the constraints a table
// holds, so the comparison has a current side, and it states that it plans
// the change it is handed.
func (p commonPlanner) ownsConstraints() bool {
	return p.ownsColumns() && p.observedConstraints && p.constraints
}

// ownsConstraintChange reports whether the owner of a kept table plans a
// change of a constraint of kind.
func (p commonPlanner) ownsConstraintChange(kind string) bool {
	return primaryKeyType(kind) && p.ownsKeys() || reportedConstraintType(kind) && p.ownsConstraints()
}

// constraintChanges accepts the constraint changes of a diff whose engine
// reports what they change, which planningContext hands to the owner of each
// kept table as an alteration of the table: a primary key where the engine
// reports ordered keys, and a FOREIGN KEY or CHECK where it reports
// constraints. A constraint of a table the plan creates travels in the
// creation's declaration, and one of a table the plan removes leaves with the
// table, so neither asks the owner to plan a change of it.
//
// It refuses a change of any other constraint, and one of a kind no read
// reports, as the planner does without the read; and a change of a kept
// table's constraint the owner does not plan, by the table it names: planning
// the table as if the constraint matched would report a constraint the table
// no longer declares as synced.
func (p commonPlanner) constraintChanges(diff *difftypes.SchemaDiff) error {
	lifecycle, err := createdAndRemovedTables(diff, p.target)
	if err != nil {
		return err
	}
	builder := objectidentity.NewBuilder(diff.EffectiveIdentifierSemantics(p.target))
	check := func(field, name, table, kind string) error {
		if primaryKeyType(kind) && !p.observedKeys || reportedConstraintType(kind) && !p.observedConstraints || !primaryKeyType(kind) && !reportedConstraintType(kind) {
			return unsupported("SchemaDiff." + field)
		}
		key, err := tableKey(builder, table)
		if err != nil {
			return err
		}
		if lifecycle[key] || p.ownsConstraintChange(kind) {
			return nil
		}
		if primaryKeyType(kind) {
			return unsupported(fmt.Sprintf("changing the primary key of table %s on the %s engine, whose provider does not plan it", table, p.target))
		}
		return unsupported(fmt.Sprintf("changing constraint %s of table %s on the %s engine, whose provider does not plan it", name, table, p.target))
	}
	for _, added := range diff.ConstraintsAdded {
		if err := check("ConstraintsAdded", added.Name, added.TableName, added.Type); err != nil {
			return err
		}
	}
	for _, removed := range diff.ConstraintsRemoved {
		if err := check("ConstraintsRemoved", removed.Name, removed.TableName, removed.Type); err != nil {
			return err
		}
	}
	return nil
}

// createdAndRemovedTables keys the tables a diff creates and removes.
func createdAndRemovedTables(diff *difftypes.SchemaDiff, target string) (map[objectidentity.Key]bool, error) {
	builder := objectidentity.NewBuilder(diff.EffectiveIdentifierSemantics(target))
	result := make(map[objectidentity.Key]bool, len(diff.TablesAdded)+len(diff.TablesRemoved))
	for _, creation := range diff.TablesAdded {
		result[builder.TableParts(creation.Table.Schema, creation.Table.Name).Key()] = true
	}
	for _, name := range diff.TablesRemoved.Names() {
		key, err := tableKey(builder, name)
		if err != nil {
			return nil, err
		}
		result[key] = true
	}
	return result, nil
}

func primaryKeyType(constraintType string) bool {
	return strings.EqualFold(strings.TrimSpace(constraintType), "PRIMARY KEY")
}

// reportedConstraintType reports whether a read under observed constraints
// reports a constraint of this type: a FOREIGN KEY or a CHECK.
func reportedConstraintType(constraintType string) bool {
	kind := strings.TrimSpace(constraintType)
	return strings.EqualFold(kind, "FOREIGN KEY") || strings.EqualFold(kind, "CHECK")
}
