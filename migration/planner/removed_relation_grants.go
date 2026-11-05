package planner

import (
	"fmt"

	"ptah.run/core/difftypes"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemavalidation"
)

// refuseGrantsOnRemovedRelations refuses a grant the comparison adds on a table,
// a view or a materialized view the plan removes. The source names an object it
// no longer declares, and no order of the two statements gives it what it says.
// Measured on YDB 26.2.1.14, the DROP VIEW applies and the GRANT then fails with
// SCHEME_ERROR. Measured on PostgreSQL 18, the GRANT runs first, the drop takes
// it along, the apply succeeds, and the next apply fails with 42P01.
//
// A view the plan drops to create again is a modification rather than a removal,
// and is granted after its creation. A removal diff.skip keeps has left the diff
// by the time this runs.
func refuseGrantsOnRemovedRelations(diff *difftypes.SchemaDiff, semantics identifier.Semantics, target string) error {
	if diff == nil || len(diff.GrantsAdded) == 0 {
		return nil
	}
	removed := make(map[string]string, len(diff.TablesRemoved)+len(diff.ViewsRemoved)+len(diff.MaterializedViewsRemoved))
	for _, name := range diff.TablesRemoved.Names() {
		removed[semantics.QualifiedTableIdentityKey(name)] = "table"
	}
	for _, name := range diff.ViewsRemoved.Names() {
		removed[semantics.QualifiedTableIdentityKey(name)] = "view"
	}
	for _, name := range diff.MaterializedViewsRemoved.Names() {
		removed[semantics.QualifiedTableIdentityKey(name)] = "materialized view"
	}
	for _, grant := range diff.GrantsAdded {
		kind, dropped := removed[semantics.QualifiedTableIdentityKey(grant.ObjectName)]
		if !grant.OnRelation() || !dropped {
			continue
		}
		subject := fmt.Sprintf("granting %s on %s %s to %s", grant.Privilege, kind, grant.ObjectName, grant.Role)
		return schemavalidation.Result{Complete: true, Diagnostics: []schemavalidation.Diagnostic{{
			Code: schemavalidation.UnsupportedFeature, Kind: "schema", Feature: subject,
			Message: fmt.Sprintf("%s: the plan drops the %s; declare it, or leave the grant out", subject, kind),
		}}}.Err(target)
	}
	return nil
}
