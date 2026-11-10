// Package rolegraph follows the role-in-role edges a database read reports,
// and selects the memberships and role attributes a saved plan depends on.
//
// A read reports the whole graph, because access analysis has to follow a
// privilege through roles the read does not describe. A plan's source
// fingerprint needs less: a membership between two roles with no tie to the
// plan cannot change what the plan does, and hashing it makes every plan on the
// server stale when a credential rotation grants a group to a new user.
package rolegraph

import (
	"cmp"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/descriptor"
)

// ForPlan returns the memberships a saved plan's source fingerprint covers:
// those [Related] finds from the roles the plan depends on, which are
//
//   - privilege holders, grantors, object owners, routine definers and policy targets;
//   - the role the read ran as, which is the role that runs the plan through
//     that connection;
//   - the roles whose names appear in statements, the plan's statement bodies,
//     which reach a role the plan grants to, creates or drops before any object
//     refers to it.
//
// Every membership comes back as the read reported it in two cases, because
// neither can be narrowed without guessing. A target with
// [capability.RoleMembership] compares memberships, so any edge can change the
// plan. A read that does not say which role it ran as leaves the role running
// the plan unknown.
// An edge known to allow neither inheritance nor SET ROLE joins the plan only
// when a statement names one of its roles.
//
// A referenced-scope read ties listed roles to the inspected schema. A
// server-wide read does not, and an ADMIN-only creator edge can add a role to
// a referenced read without giving the creator access to it.
func ForPlan(database *catalog.Database, statements []string, scope descriptor.RoleScope, capabilities capability.Capabilities) []catalog.RoleMembership {
	if database.ConnectedRole == "" || capabilities.Has(capability.RoleMembership) {
		return database.RoleMemberships
	}
	// PostgreSQL can give a role ADMIN without allowing inheritance or SET
	// ROLE. Such an edge cannot change a plan that never names the role.
	effective := make([]catalog.RoleMembership, 0, len(database.RoleMemberships))
	for _, edge := range database.RoleMemberships {
		if optionEnabled(edge.InheritOption) || optionEnabled(edge.SetOption) {
			effective = append(effective, edge)
		}
	}
	related := Related(effective, planRoles(database, statements, scope))
	// A plan that names a role can depend on its ADMIN-only memberships too,
	// for example when dropping that role.
	related = append(related, Related(database.RoleMemberships, Named(database.RoleMemberships, statements))...)
	return sortMemberships(related)
}

// An unreported option stays eligible. Only a known false/false pair proves
// the edge conveys no privilege and no ability to switch roles.
func optionEnabled(option *bool) bool { return option == nil || *option }

// ForPlanRoles keeps the role attributes a saved plan can depend on. A reader
// with server-wide roles reports unrelated accounts too; creating one of them
// must not replace a plan for another database. A role named by the plan, a
// privilege, an owner, or a relevant membership still changes the fingerprint.
func ForPlanRoles(database *catalog.Database, statements []string, scope descriptor.RoleScope, capabilities capability.Capabilities) []catalog.Role {
	if database.ConnectedRole == "" {
		return database.Roles
	}
	needed := make(map[string]bool)
	for _, name := range planRoles(database, statements, scope) {
		needed[name] = true
	}
	for _, edge := range ForPlan(database, statements, scope, capabilities) {
		needed[edge.Role], needed[edge.Member] = true, true
	}
	for _, name := range NamedRoles(database.Roles, statements) {
		needed[name] = true
	}
	var roles []catalog.Role
	for _, role := range database.Roles {
		if needed[role.Name] {
			roles = append(roles, role)
		}
	}
	return roles
}

// planRoles collects the roles whose attributes or privileges can affect a
// plan. The same roots select memberships and server-wide role attributes.
func planRoles(database *catalog.Database, statements []string, scope descriptor.RoleScope) []string {
	roles := []string{database.ConnectedRole}
	if scope == descriptor.RoleScopeReferenced {
		for _, role := range database.Roles {
			if !adminOnlyCreatorRole(database, role.Name) {
				roles = append(roles, role.Name)
			}
		}
	}
	for _, grant := range database.Grants {
		roles = append(roles, grant.Role, grant.GrantedBy)
	}
	for _, owner := range database.ObjectOwners {
		roles = append(roles, owner.Owner)
	}
	for _, function := range database.Functions {
		roles = append(roles, accountUser(function.Definer))
	}
	policyTargets := make([]string, 0, len(database.RLSPolicies))
	for _, policy := range database.RLSPolicies {
		policyTargets = append(policyTargets, policy.ToRoles)
	}
	roles = append(roles, NamedRoles(database.Roles, policyTargets)...)
	roles = append(roles, Named(database.RoleMemberships, policyTargets)...)
	roles = append(roles, Named(database.RoleMemberships, statements)...)
	return roles
}

func adminOnlyCreatorRole(database *catalog.Database, name string) bool {
	for _, edge := range database.RoleMemberships {
		if edge.Role == name && edge.Member == database.ConnectedRole && edge.AdminOption &&
			!optionEnabled(edge.InheritOption) && !optionEnabled(edge.SetOption) {
			return true
		}
	}
	return false
}

// NamedRoles returns listed roles whose names appear in a statement body.
func NamedRoles(roles []catalog.Role, statements []string) []string {
	names := make([]string, 0, len(roles))
	for _, role := range roles {
		names = append(names, role.Name)
	}
	return namesInStatements(names, statements)
}

// accountUser is the user part of a `user@host` account, which is how a
// membership names it. A user name can hold an `@`, and a host cannot.
func accountUser(account string) string {
	if at := strings.LastIndexByte(account, '@'); at >= 0 {
		return account[:at]
	}
	return account
}

// Related returns the memberships connected to roles along the direction
// privileges travel: what a role holds, followed upward through the groups it
// belongs to, and who holds it, followed downward through its members. Both
// walks are transitive, pass through roles the list does not name, and stop at a
// cycle.
//
// A membership only reached sideways is left out. When a role belongs to a
// group, another member of that group holds nothing more of the role's, and the
// role holds nothing more of that member's.
//
// The result is sorted and holds each membership once, so two reads of one
// graph agree byte for byte whatever order the server returned, and whatever
// number of rows it keeps for one edge. It is nil when nothing is connected.
func Related(memberships []catalog.RoleMembership, roles []string) []catalog.RoleMembership {
	held := reach(memberships, roles, func(edge catalog.RoleMembership) (string, string) { return edge.Member, edge.Role })
	holders := reach(memberships, roles, func(edge catalog.RoleMembership) (string, string) { return edge.Role, edge.Member })

	var related []catalog.RoleMembership
	for _, edge := range memberships {
		if held[edge.Member] || holders[edge.Role] {
			related = append(related, edge)
		}
	}
	return sortMemberships(related)
}

func sortMemberships(memberships []catalog.RoleMembership) []catalog.RoleMembership {
	slices.SortFunc(memberships, func(left, right catalog.RoleMembership) int {
		return cmp.Or(
			strings.Compare(left.Role, right.Role),
			strings.Compare(left.Member, right.Member),
			compareBool(left.AdminOption, right.AdminOption),
			compareOptionalBool(left.InheritOption, right.InheritOption),
			compareOptionalBool(left.SetOption, right.SetOption),
		)
	})
	return slices.CompactFunc(memberships, func(left, right catalog.RoleMembership) bool {
		return left.Role == right.Role && left.Member == right.Member && left.AdminOption == right.AdminOption &&
			compareOptionalBool(left.InheritOption, right.InheritOption) == 0 &&
			compareOptionalBool(left.SetOption, right.SetOption) == 0
	})
}

func compareOptionalBool(left, right *bool) int {
	if left == nil || right == nil {
		switch {
		case left == nil && right == nil:
			return 0
		case left == nil:
			return -1
		default:
			return 1
		}
	}
	return compareBool(*left, *right)
}

func compareBool(left, right bool) int {
	switch {
	case left == right:
		return 0
	case right:
		return -1
	default:
		return 1
	}
}

// reach is every role a walk from roles arrives at, roles included. step says
// which end of an edge the walk leaves from and which it arrives at.
func reach(memberships []catalog.RoleMembership, roles []string, step func(catalog.RoleMembership) (string, string)) map[string]bool {
	next := make(map[string][]string, len(memberships))
	for _, edge := range memberships {
		from, to := step(edge)
		next[from] = append(next[from], to)
	}
	seen := make(map[string]bool, len(roles))
	pending := slices.Clone(roles)
	for len(pending) > 0 {
		role := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		if !seen[role] {
			seen[role] = true
			pending = append(pending, next[role]...)
		}
	}
	return seen
}

// Named returns the roles at either end of memberships whose names appear in
// statements, sorted, each once.
//
// It searches the text for the names the graph holds rather than parsing roles
// out of the statements, so it covers every statement a plan can hold, including
// one a feature owner renders, a routine body and a quoted name such as
// `v-token-1`. A table named like a role counts as naming it. That widens what
// the fingerprint covers and never narrows it, so the cost of a false match is a
// plan replaced for a membership it did not depend on. The comparison ignores
// case, because a server folds an unquoted name and the statement's spelling
// need not be the catalog's. A caller strips comments first.
func Named(memberships []catalog.RoleMembership, statements []string) []string {
	roles := make([]string, 0, 2*len(memberships))
	for _, edge := range memberships {
		roles = append(roles, edge.Role, edge.Member)
	}
	return namesInStatements(roles, statements)
}

func namesInStatements(roles, statements []string) []string {
	lowered := make([]string, len(statements))
	for i, statement := range statements {
		lowered[i] = strings.ToLower(statement)
	}
	slices.Sort(roles)
	var named []string
	for _, role := range slices.Compact(roles) {
		name := strings.ToLower(role)
		if slices.ContainsFunc(lowered, func(statement string) bool { return containsName(statement, name) }) {
			named = append(named, role)
		}
	}
	return named
}

// containsName reports whether name occurs in text with no identifier character
// touching it on either side.
func containsName(text, name string) bool {
	if name == "" {
		return false
	}
	for offset := 0; ; {
		found := strings.Index(text[offset:], name)
		if found < 0 {
			return false
		}
		start := offset + found
		end := start + len(name)
		before, _ := utf8.DecodeLastRuneInString(text[:start])
		after, _ := utf8.DecodeRuneInString(text[end:])
		if !isIdentifierRune(before) && !isIdentifierRune(after) {
			return true
		}
		offset = start + 1
	}
}

// isIdentifierRune is false for the error rune an empty string decodes to, which
// is what the start and the end of a statement read as.
func isIdentifierRune(r rune) bool {
	return r == '_' || unicode.IsLetter(r) || unicode.IsDigit(r)
}
