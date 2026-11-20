// Package rolemembership turns the membership changes of a schema diff into
// the statement nodes the PostgreSQL, MySQL and ClickHouse planners plan.
//
// The three engines share one shape (`GRANT role TO member`, `REVOKE role FROM
// member`) and differ in how an option leaves a membership. That difference is
// a [Withdrawal], chosen by the planner from what its target measured, and not
// a dialect name.
package rolemembership

import (
	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform/capability"
)

// Withdrawal is how a target takes the admin option off a membership that
// stays.
type Withdrawal int

const (
	// RevokeOptionFor writes `REVOKE ADMIN OPTION FOR role FROM member`, which
	// keeps the membership. PostgreSQL and ClickHouse accept it; measured on
	// PostgreSQL 14.25 and 16 and on ClickHouse 26.10.
	RevokeOptionFor Withdrawal = iota
	// RevokeAndGrant revokes the membership and grants it again without the
	// option. MySQL has no statement that revokes the option alone: measured
	// on 8.4, `REVOKE ADMIN OPTION FOR` is a syntax error and a plain GRANT
	// on a membership that holds the option leaves it on.
	RevokeAndGrant
)

// Removals returns the statements that take a membership out of a role.
func Removals(diff *difftypes.SchemaDiff) []ast.Node {
	nodes := make([]ast.Node, 0, len(diff.RoleMembershipsRemoved))
	for _, membership := range diff.RoleMembershipsRemoved {
		nodes = append(nodes, ast.NewRevokeRoleMembership(membership.Role, membership.Member))
	}
	return nodes
}

// Additions returns the statements that add a membership with the options its
// declaration states, and then the ones that change the options of a
// membership both sides hold.
//
// A target with [capability.RoleMembershipInheritSet] changes every differing
// option with one GRANT that names it, because PostgreSQL 16 updates the
// membership in place. A target without it changes the admin option only, as
// withdrawal says.
func Additions(diff *difftypes.SchemaDiff, caps capability.Capabilities, withdrawal Withdrawal) []ast.Node {
	nodes := make([]ast.Node, 0, len(diff.RoleMembershipsAdded)+len(diff.RoleMembershipsModified))
	for _, membership := range diff.RoleMembershipsAdded {
		nodes = append(nodes, ast.NewGrantRoleMembership(membership.Role, membership.Member).
			SetOptions(membership.Options.Admin, membership.Options.Inherit, membership.Options.Set))
	}
	for _, change := range diff.RoleMembershipsModified {
		nodes = append(nodes, modification(change, caps, withdrawal)...)
	}
	return nodes
}

func modification(change difftypes.RoleMembershipChange, caps capability.Capabilities, withdrawal Withdrawal) []ast.Node {
	admin := differing(change.From.Admin, change.To.Admin)
	if caps.Has(capability.RoleMembershipInheritSet) {
		inherit := differing(change.From.Inherit, change.To.Inherit)
		set := differing(change.From.Set, change.To.Set)
		if admin == nil && inherit == nil && set == nil {
			return nil
		}
		return []ast.Node{ast.NewGrantRoleMembership(change.Role, change.Member).SetOptions(admin, inherit, set)}
	}
	switch {
	case admin == nil:
		return nil
	case *admin:
		return []ast.Node{ast.NewGrantRoleMembership(change.Role, change.Member).SetOptions(admin, nil, nil)}
	case withdrawal == RevokeOptionFor:
		return []ast.Node{ast.NewRevokeRoleMembership(change.Role, change.Member).SetAdminOptionFor(true)}
	default:
		return []ast.Node{
			ast.NewRevokeRoleMembership(change.Role, change.Member),
			ast.NewGrantRoleMembership(change.Role, change.Member),
		}
	}
}

// differing returns the value an option takes when it differs from the one the
// database holds, and nil when it does not or the server does not report it.
func differing(from, to *bool) *bool {
	if from == nil || to == nil || *from == *to {
		return nil
	}
	return new(*to)
}
