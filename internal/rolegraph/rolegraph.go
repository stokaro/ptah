// Package rolegraph follows the role-in-role edges a database read reports, and
// selects the ones a saved plan depends on.
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
//   - the roles the read's description names: privilege holders, grantors,
//     object owners, routine definers and, where the read lists only the roles
//     its schemas refer to, every listed role;
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
//
// scope is the dialect's [descriptor.Roles.OutOfScope]. Under
// [descriptor.RoleScopeNone] the listed roles are the whole server's, so being
// listed is no tie to the plan and only the other sources count.
func ForPlan(database *catalog.Database, statements []string, scope descriptor.RoleScope, capabilities capability.Capabilities) []catalog.RoleMembership {
	if database.ConnectedRole == "" || capabilities.Has(capability.RoleMembership) {
		return database.RoleMemberships
	}
	roles := []string{database.ConnectedRole}
	if scope != descriptor.RoleScopeNone {
		for _, role := range database.Roles {
			roles = append(roles, role.Name)
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
	roles = append(roles, Named(database.RoleMemberships, statements)...)
	return Related(database.RoleMemberships, roles)
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
	slices.SortFunc(related, func(left, right catalog.RoleMembership) int {
		return cmp.Or(
			strings.Compare(left.Role, right.Role),
			strings.Compare(left.Member, right.Member),
			compareBool(left.AdminOption, right.AdminOption),
		)
	})
	return slices.Compact(related)
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
	lowered := make([]string, len(statements))
	for i, statement := range statements {
		lowered[i] = strings.ToLower(statement)
	}
	roles := make([]string, 0, 2*len(memberships))
	for _, edge := range memberships {
		roles = append(roles, edge.Role, edge.Member)
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
