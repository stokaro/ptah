package mysql

import (
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/sqlident"
	"ptah.run/internal/tableref"
)

// recreatedRoutineList includes additions because the reverse of a removal
// creates a routine again. Its observed privileges still describe the
// pre-migration routine. A new routine has no matching observed privilege.
func (p *Planner) recreatedRoutineList(diff *difftypes.SchemaDiff) []schemamodel.Function {
	routines := p.replacedRoutineList(diff)
	for _, added := range diff.FunctionsAdded {
		routines = append(routines, added.Function)
	}
	return routines
}

// recreatedRoutines keys the routines whose CREATE may need to restore a
// privilege, without adding them to the objects the plan drops.
func (p *Planner) recreatedRoutines(diff *difftypes.SchemaDiff, semantics identifier.Semantics) map[string]schemamodel.Function {
	routines := make(map[string]schemamodel.Function)
	for _, routine := range p.recreatedRoutineList(diff) {
		routines[semantics.QualifiedTableIdentityKey(routine.Name)] = routine
	}
	return routines
}

// addedRoutine reports that name is an addition, which distinguishes a
// restored grant from the grant of an in-place routine replacement.
func addedRoutine(diff *difftypes.SchemaDiff, semantics identifier.Semantics, name string) bool {
	key := semantics.QualifiedTableIdentityKey(name)
	for _, added := range diff.FunctionsAdded {
		if semantics.QualifiedTableIdentityKey(added.Name) == key {
			return true
		}
	}
	return false
}

// restoredGrantOption restores an observed grant on an Oracle routine.
// Oracle refuses a grant option to a role, so the declaration renderer cannot
// write one to an unclassified grantee. A catalog row that already holds it
// proves that this grantee may hold it. The rollback copies that row only;
// it does not accept a new grant option from the declaration.
func (p *Planner) restoredGrantOption(grant difftypes.GrantRef) (ast.Node, error) {
	ref, ok := tableref.Parse(grant.ObjectName)
	if !ok || !permissionName.MatchString(grant.Privilege) {
		return nil, fmt.Errorf("%w: %s: cannot restore the observed grant option on %q to %q: invalid object or privilege name",
			ptaherr.ErrInvalidSchemaDiff, p.targetDialect(), grant.ObjectName, grant.Role)
	}
	name := sqlident.Quote(p.targetDialect(), ref.Name)
	if ref.Qualified {
		name = sqlident.Quote(p.targetDialect(), ref.Schema) + "." + name
	}
	return ast.NewRawSQL(fmt.Sprintf("GRANT %s ON %s TO %s WITH GRANT OPTION;",
		grant.Privilege, name, sqlident.Quote(p.targetDialect(), grant.Role))), nil
}
