package sqlschema

import (
	"fmt"

	schemacoverage "ptah.run/core/coverage"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/sqlite/sqlitetable"
)

// sqliteVirtualDirective is how a SQLite document's header says it does not
// describe virtual tables: `-- ptah:not-described virtual_table`.
const sqliteVirtualDirective = "virtual_table"

// sqliteVirtualLimits records the header directive that takes virtual tables
// out of a SQLite document's claim. A SQLite SQL document can state a virtual
// table, so it describes them unless its header says otherwise.
type sqliteVirtualLimits struct {
	declined bool
}

// consume claims the virtual table directive for the SQLite owner. The
// directive covers the whole kind: a named one is refused rather than widened
// to every virtual table or narrowed to a name the comparison may spell
// differently.
func (l *sqliteVirtualLimits) consume(object schemacoverage.Object) (bool, error) {
	if string(object.Kind) != sqliteVirtualDirective {
		return false, nil
	}
	if !object.WholeKind() {
		return false, fmt.Errorf("%w: ptah:not-described %s names %q; the directive takes no name in a SQLite document",
			ptaherr.ErrUnsupportedFeature, sqliteVirtualDirective, object.Name)
	}
	l.declined = true
	return true, nil
}

// coverage is the document's claim about virtual tables: complete, or none
// when its header declined them.
func (l sqliteVirtualLimits) coverage() (schemaext.Coverage, error) {
	knowledge := schemaext.Knowledge{State: schemaext.Complete}
	if l.declined {
		knowledge = schemaext.Knowledge{State: schemaext.Uninspected, Reason: "the document's header says it does not describe virtual tables"}
	}
	return sqlitetable.VirtualCoverage(schemaext.Desired, knowledge, nil)
}
