package schemaprep

import (
	"fmt"
	"strings"

	"ptah.run/core/schemamodel"
)

// EnforcementAndMatch names every CHECK and foreign key of database that the
// server keeps without checking, and every foreign key with a MATCH type, in
// declaration order: the columns' constraints first, then the constraint list.
//
// It is one answer for the exporters whose format has no spelling for either
// clause, so each refuses the same set rather than writing the constraint
// without it, which would check what its author said it does not, or let
// through what MATCH FULL refuses (stokaro/ptah#3853).
func EnforcementAndMatch(database *schemamodel.Database) []string {
	var clauses []string
	for _, field := range database.Fields {
		if field.Check != "" && field.CheckNotEnforced {
			clauses = append(clauses, fmt.Sprintf("the CHECK of column %q is NOT ENFORCED", field.Name))
		}
		if field.Foreign == "" {
			continue
		}
		if field.ForeignKeyNotEnforced {
			clauses = append(clauses, fmt.Sprintf("the foreign key of column %q is NOT ENFORCED", field.Name))
		}
		if field.ForeignKeyMatch != "" {
			clauses = append(clauses, fmt.Sprintf("the foreign key of column %q is MATCH %s",
				field.Name, strings.ToUpper(field.ForeignKeyMatch)))
		}
	}
	for _, constraint := range database.Constraints {
		if constraint.NotEnforced {
			clauses = append(clauses, fmt.Sprintf("%s %q is NOT ENFORCED", strings.ToUpper(constraint.Type), constraint.Name))
		}
		if constraint.Match != "" && IsForeignKeyConstraint(constraint) {
			clauses = append(clauses, fmt.Sprintf("FOREIGN KEY %q is MATCH %s",
				constraint.Name, strings.ToUpper(constraint.Match)))
		}
	}
	return clauses
}
