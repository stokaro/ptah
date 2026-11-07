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

// keyChanges accepts the primary key changes of a diff whose engine reports
// its keys, which planningContext hands to the table's owner as an alteration
// of the table. It refuses a change of any other constraint, as the planner
// does without the read, and a key change the owner does not plan, by the
// table it names: planning the table as if its key matched would report the
// key the table no longer declares as synced.
//
// A constraint of a table the plan creates or drops is no change of a kept
// table when the provider owns the table lifecycle: a created table's
// constraints travel in the declaration the owner creates it from, and a
// dropped table takes its constraints with it. Without this, a foreign key
// declared in a new table was refused before its owner saw it.
func (p commonPlanner) keyChanges(diff *difftypes.SchemaDiff) error {
	created, dropped, err := p.ownedTableChanges(diff)
	if err != nil {
		return err
	}
	builder := objectidentity.NewBuilder(diff.EffectiveIdentifierSemantics(p.target))
	tables := make([]string, 0, len(diff.ConstraintsAdded)+len(diff.ConstraintsRemoved))
	for _, added := range diff.ConstraintsAdded {
		key, err := tableKey(builder, added.TableName)
		if err != nil {
			return err
		}
		if created[key] {
			continue
		}
		if !primaryKeyType(added.Type) {
			return unsupported("SchemaDiff.ConstraintsAdded")
		}
		tables = append(tables, added.TableName)
	}
	for _, removed := range diff.ConstraintsRemoved {
		key, err := tableKey(builder, removed.TableName)
		if err != nil {
			return err
		}
		if dropped[key] {
			continue
		}
		if !primaryKeyType(removed.Type) {
			return unsupported("SchemaDiff.ConstraintsRemoved")
		}
		tables = append(tables, removed.TableName)
	}
	if len(tables) != 0 && !p.ownsKeys() {
		return unsupported(fmt.Sprintf("changing the primary key of table %s on the %s engine, whose provider does not plan it", tables[0], p.target))
	}
	return nil
}

// ownedTableChanges returns the tables the plan creates and the tables it
// drops, by key, when the provider owns the table lifecycle, and none
// otherwise: a host that plans a provider's tables itself has no owner to
// hand a created table's constraints to.
func (p commonPlanner) ownedTableChanges(diff *difftypes.SchemaDiff) (created, dropped map[objectidentity.Key]bool, err error) {
	created, dropped = make(map[objectidentity.Key]bool), make(map[objectidentity.Key]bool)
	if !p.ownsColumns() {
		return created, dropped, nil
	}
	builder := objectidentity.NewBuilder(diff.EffectiveIdentifierSemantics(p.target))
	for _, creation := range diff.TablesAdded {
		created[builder.TableParts(creation.Table.Schema, creation.Table.Name).Key()] = true
	}
	for _, name := range diff.TablesRemoved.Names() {
		key, err := tableKey(builder, name)
		if err != nil {
			return nil, nil, err
		}
		dropped[key] = true
	}
	return created, dropped, nil
}

func primaryKeyType(constraintType string) bool {
	return strings.EqualFold(strings.TrimSpace(constraintType), "PRIMARY KEY")
}
