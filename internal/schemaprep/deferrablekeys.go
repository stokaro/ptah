package schemaprep

import (
	"fmt"
	"strings"

	"ptah.run/core/schemamodel"
)

// DeferrableKeys names every PRIMARY KEY, UNIQUE and EXCLUDE of database that
// defers its check, in declaration order: the tables' primary keys first, then
// the constraint list. A foreign key is not listed; it has a deferral of its
// own that some formats write.
//
// It is one answer for the exporters whose format has no spelling for a
// deferrable key, so each refuses the same set rather than writing the key
// plain, which would reject at once the rows its author arranged to fix before
// commit (stokaro/ptah#3824).
func DeferrableKeys(database *schemamodel.Database) []string {
	var keys []string
	for _, table := range database.Tables {
		if table.PrimaryKeyDeferrable {
			keys = append(keys, fmt.Sprintf("the primary key of table %q", table.Name))
		}
	}
	for _, constraint := range database.Constraints {
		if constraint.Deferrable && !IsForeignKeyConstraint(constraint) {
			keys = append(keys, fmt.Sprintf("%s %q", strings.ToUpper(constraint.Type), constraint.Name))
		}
	}
	return keys
}
