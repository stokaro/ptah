package compare

import (
	"cmp"
	"slices"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/difftypes"
)

// RoleMemberships compares the membership of each role in other roles, on a
// target with [capability.RoleMembership], and does nothing elsewhere: a
// PostgreSQL reader reports memberships for analysis, and no planner there
// plans one, so comparing them would plan statements nothing renders.
//
// A declared membership the database does not hold is added. Adding one is
// safe to repeat (measured on YDB 25.1.4.7 to 26.2.1.14: adding a member twice
// succeeds with a notice), so an addition is planned even where the read did
// not describe the memberships.
//
// A membership the database holds and the declaration does not is removed
// only when both roles are declared. A cluster adds every new YDB user to its
// all-users group, USERS on a default cluster, and the group holds the right
// to connect; a removal planned because a declaration did not list that group
// would take it away. So a membership in a group the schema does not declare
// is the server's, and is left as it is.
func RoleMemberships(
	desired *schemamodel.Database,
	database *catalog.Database,
	diff *difftypes.SchemaDiff,
	caps capability.Capabilities,
) {
	if !caps.Has(capability.RoleMembership) || desired == nil || database == nil {
		return
	}
	managed := make(map[string]bool, len(desired.Roles))
	declared := make(map[difftypes.RoleMembershipRef]bool)
	for _, role := range desired.Roles {
		managed[role.Name] = true
		for _, group := range role.MemberOf {
			if group = strings.TrimSpace(group); group != "" {
				declared[difftypes.RoleMembershipRef{Role: group, Member: role.Name}] = true
			}
		}
	}
	held := make(map[difftypes.RoleMembershipRef]bool, len(database.RoleMemberships))
	for _, membership := range database.RoleMemberships {
		held[difftypes.RoleMembershipRef{Role: membership.Role, Member: membership.Member}] = true
	}
	for membership := range declared {
		if !held[membership] {
			diff.RoleMembershipsAdded = append(diff.RoleMembershipsAdded, membership)
		}
	}
	for membership := range held {
		if !declared[membership] && managed[membership.Role] && managed[membership.Member] {
			diff.RoleMembershipsRemoved = append(diff.RoleMembershipsRemoved, membership)
		}
	}
	slices.SortFunc(diff.RoleMembershipsAdded, compareMemberships)
	slices.SortFunc(diff.RoleMembershipsRemoved, compareMemberships)
}

func compareMemberships(a, b difftypes.RoleMembershipRef) int {
	return cmp.Or(cmp.Compare(a.Role, b.Role), cmp.Compare(a.Member, b.Member))
}
