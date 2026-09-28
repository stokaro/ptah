package schemaprep

import (
	"fmt"
	"strings"

	"ptah.run/core/schemamodel"
)

// UnvalidatedConstraints names each CHECK or foreign key the database allows
// to stay NOT VALID, in declaration order, for an export whose format has no
// way to say so. Written without it, the constraint reads back validated, and
// a plan from the export to the database it came from validates the
// constraint: a scan of every row, which fails where a row breaks it.
func UnvalidatedConstraints(database *schemamodel.Database) []string {
	var names []string
	for _, constraint := range database.Constraints {
		if constraint.NotValid {
			names = append(names, fmt.Sprintf("%s %q", strings.ToUpper(constraint.Type), constraint.Name))
		}
	}
	return names
}
