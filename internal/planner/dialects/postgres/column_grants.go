package postgres

import (
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform/identifier"
)

// restoreColumnGrants gives back column privileges a table revoke takes even
// though the diff keeps them. CurrentGrants carries the pre-change read in
// both directions, so the rollback of an added table grant also restores the
// column privileges the database held before that addition.
func restoreColumnGrants(
	result []ast.Node,
	diff *difftypes.SchemaDiff,
	revoked difftypes.GrantRef,
	semantics identifier.Semantics,
) []ast.Node {
	if revoked.Column != "" || !strings.EqualFold(revoked.ObjectType, "TABLE") {
		return result
	}
	for _, held := range diff.CurrentGrants {
		if held.Column == "" || !tableRevokeTakesColumn(diff, revoked, held, semantics) {
			continue
		}
		matches := func(planned difftypes.GrantRef) bool {
			return planned.Column != "" && sameColumnPrivilege(planned, held, semantics)
		}
		if slices.ContainsFunc(diff.GrantsRemoved, matches) || slices.ContainsFunc(diff.GrantsAdded, matches) ||
			slices.ContainsFunc(diff.GrantOptionsAdded, matches) {
			continue
		}
		result = append(result, ast.NewGrantPrivilege(held.Role, held.ObjectType, held.ObjectName, []string{held.Privilege}).
			SetColumns([]string{held.Column}).SetWithOption(held.WithOption))
	}
	return result
}

// tableRevokeTakesColumn matches the grantee, object, privilege and grantor
// a table revoke affects. A different grantor's independent column grant
// stays in place and must not become a new grant from the connected role.
func tableRevokeTakesColumn(
	diff *difftypes.SchemaDiff,
	revoked, held difftypes.GrantRef,
	semantics identifier.Semantics,
) bool {
	if !samePrivilegeTarget(revoked, held, semantics) {
		return false
	}
	grantor := revoked.GrantedBy
	if grantor == "" {
		grantor = diff.ConnectedRole
		if diff.ConnectedRoleSuperuser && held.ObjectOwner != "" {
			grantor = held.ObjectOwner
		}
	}
	return grantor == "" || held.GrantedBy == "" || held.GrantedBy == grantor
}

func sameColumnPrivilege(planned, held difftypes.GrantRef, semantics identifier.Semantics) bool {
	return samePrivilegeTarget(planned, held, semantics) &&
		semantics.ColumnIdentityKey(planned.Column) == semantics.ColumnIdentityKey(held.Column)
}

// samePrivilegeTarget matches an individual privilege or ALL on one object
// of one grantee. The column and grantor are decided by its caller.
func samePrivilegeTarget(planned, held difftypes.GrantRef, semantics identifier.Semantics) bool {
	return planned.Role == held.Role && strings.EqualFold(planned.ObjectType, held.ObjectType) &&
		sameObject(planned.ObjectName, held.ObjectName, semantics) &&
		(strings.EqualFold(planned.Privilege, held.Privilege) || strings.EqualFold(planned.Privilege, "ALL"))
}
