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

// constraintChanges accepts the constraint changes of a diff that reach an
// owner of the table lifecycle. A constraint of a table the owner creates
// travels in the declaration it creates the table from, and one of a table it
// drops leaves with the table, so neither is a change. A change of a kept
// table's constraint is handed to the table's owner as an alteration of the
// table: a primary key where the engine reports ordered keys, and a FOREIGN
// KEY or CHECK where it reports constraints; see planningContext.
//
// It refuses a change of any other constraint, and one of a kind no read
// reports, as the planner does without the read; and a change of a kept
// table's constraint the owner does not plan, by the table it names: planning
// the table as if the constraint matched would report a constraint the table
// no longer declares as synced. A host that plans a provider's tables itself
// has no owner to hand a created table's constraints to, so it refuses those
// too.
func (p commonPlanner) constraintChanges(diff *difftypes.SchemaDiff) error {
	lifecycle, err := p.ownedTableChanges(diff)
	if err != nil {
		return err
	}
	builder := objectidentity.NewBuilder(diff.EffectiveIdentifierSemantics(p.target))
	check := func(field, name, table, kind string) error {
		key, err := tableKey(builder, table)
		if err != nil {
			return err
		}
		switch {
		case lifecycle[key]:
			return nil
		case primaryKeyType(kind) && !p.observedKeys, reportedConstraintType(kind) && !p.observedConstraints,
			!primaryKeyType(kind) && !reportedConstraintType(kind):
			return unsupported("SchemaDiff." + field)
		case p.ownsConstraintChange(kind):
			return nil
		case primaryKeyType(kind):
			return unsupported(fmt.Sprintf("changing the primary key of table %s on the %s engine, whose provider does not plan it", table, p.target))
		default:
			return unsupported(fmt.Sprintf("changing constraint %s of table %s on the %s engine, whose provider does not plan it", name, table, p.target))
		}
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

// ownedTableChanges keys the tables the plan creates and drops when the
// provider owns the table lifecycle, and none otherwise.
func (p commonPlanner) ownedTableChanges(diff *difftypes.SchemaDiff) (map[objectidentity.Key]bool, error) {
	result := make(map[objectidentity.Key]bool, len(diff.TablesAdded)+len(diff.TablesRemoved))
	if !p.ownsColumns() {
		return result, nil
	}
	builder := objectidentity.NewBuilder(diff.EffectiveIdentifierSemantics(p.target))
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
