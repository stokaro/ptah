package providerhost

import (
	"fmt"
	"strings"

	"ptah.run/core/difftypes"
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
func (p commonPlanner) keyChanges(diff *difftypes.SchemaDiff) error {
	tables := make([]string, 0, len(diff.ConstraintsAdded)+len(diff.ConstraintsRemoved))
	for _, added := range diff.ConstraintsAdded {
		if !primaryKeyType(added.Type) {
			return unsupported("SchemaDiff.ConstraintsAdded")
		}
		tables = append(tables, added.TableName)
	}
	for _, removed := range diff.ConstraintsRemoved {
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

func primaryKeyType(constraintType string) bool {
	return strings.EqualFold(strings.TrimSpace(constraintType), "PRIMARY KEY")
}
