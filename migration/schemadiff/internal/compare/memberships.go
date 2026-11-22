package compare

import (
	"cmp"
	"slices"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/coverage"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
)

// RoleMemberships compares the membership of each role in other roles, on a
// target with [capability.RoleMembership], and does nothing elsewhere: a
// reader there reports memberships for analysis, and no planner plans one, so
// comparing them would plan statements nothing renders.
//
// A declared membership the database does not hold is added. Adding one is
// safe to repeat (measured on YDB 25.1.4.7 to 26.2.1.14: adding a member twice
// succeeds with a notice), so an addition is planned even where the read did
// not describe the memberships.
//
// A membership the database holds and the declaration does not is removed
// only when both roles are declared and the member owns its memberships
// ([schemamodel.Role.OwnsMemberships]). A cluster adds every new YDB user to
// its all-users group, USERS on a default cluster, and the group holds the
// right to connect; a removal planned because a declaration did not list that
// group would take it away. So a membership in a group the schema does not
// declare is the server's, and is left as it is. A member that never wrote
// member_of leaves its memberships to the server too: a role an application
// connects as often holds its permissions through a membership granted outside
// the schema, and a plan that read the omission as "none" would revoke them.
// Such a membership between two declared roles is listed in
// UnmanagedRoleMemberships, so a report can say the plan kept it: deleting
// member_of from a role revokes nothing, and the line is how a reader finds out.
//
// A membership both sides hold is compared option by option, where the target
// reports options. A declaration that states no option means the default:
// admin false, set true, and inherit as the member role's own attribute. See
// [difftypes.RoleMembershipChange].
//
// Both roles of a membership are matched by the name the server stores, as
// [Roles] matches a role, so a bare `reader` member of `team` is the
// membership of READER in TEAM where the server folds unquoted names to upper
// case. A planned addition names the roles as declared, and a removal as read.
func RoleMemberships(
	desired *schemamodel.Database,
	database *catalog.Database,
	diff *difftypes.SchemaDiff,
	semantics identifier.Semantics,
	caps capability.Capabilities,
) {
	if !caps.Has(capability.RoleMembership) || desired == nil || database == nil {
		return
	}
	managed, owners, declared := declaredMemberships(desired, semantics)
	held := make(map[membershipKey]catalog.RoleMembership, len(database.RoleMemberships))
	for _, membership := range database.RoleMemberships {
		key := newMembershipKey(membership.Role, membership.Member, semantics)
		if previous, seen := held[key]; seen {
			membership = mergeHeldMemberships(previous, membership)
		}
		held[key] = membership
	}
	for key, want := range declared {
		if !desired.NotDescribed.Describes(coverage.RoleMembership, want.ref.Member) {
			continue
		}
		live, found := held[key]
		if !found {
			// A role the read refused to describe may not exist, and a GRANT
			// planned beside the undecided CREATE would name it first.
			if !database.NotDescribed.Describes(coverage.Role, want.ref.Member) ||
				!database.NotDescribed.Describes(coverage.Role, want.ref.Role) {
				continue
			}
			added := want.ref
			added.Options = want.options.Clone()
			diff.RoleMembershipsAdded = append(diff.RoleMembershipsAdded, added)
			continue
		}
		if change, differs := compareMembershipOptions(want.ref, want, live, caps); differs {
			diff.RoleMembershipsModified = append(diff.RoleMembershipsModified, change)
			continue
		}
		diff.Matched.RoleMemberships++
	}
	for key, live := range held {
		if !desired.NotDescribed.Describes(coverage.RoleMembership, live.Member) ||
			!database.NotDescribed.Describes(coverage.RoleMembership, live.Member) {
			continue
		}
		if _, isDeclared := declared[key]; isDeclared || !managed[key.role] || !managed[key.member] {
			continue
		}
		if !owners[key.member] {
			diff.UnmanagedRoleMemberships = append(diff.UnmanagedRoleMemberships,
				difftypes.RoleMembershipRef{Role: live.Role, Member: live.Member})
			continue
		}
		removed := difftypes.RoleMembershipRef{Role: live.Role, Member: live.Member}
		removed.Options = heldMembershipOptions(live, caps)
		diff.RoleMembershipsRemoved = append(diff.RoleMembershipsRemoved, removed)
	}
	slices.SortFunc(diff.RoleMembershipsAdded, compareMemberships)
	slices.SortFunc(diff.RoleMembershipsRemoved, compareMemberships)
	slices.SortFunc(diff.UnmanagedRoleMemberships, compareMemberships)
	slices.SortFunc(diff.RoleMembershipsModified, func(a, b difftypes.RoleMembershipChange) int {
		return cmp.Or(cmp.Compare(a.Role, b.Role), cmp.Compare(a.Member, b.Member))
	})
}

// declaredMembership is one declared membership: the options it states, and
// the member's own inherit attribute, which is the default for the inherit
// option.
type declaredMembership struct {
	// ref names the two roles as the declaration spells them.
	ref            difftypes.RoleMembershipRef
	options        schemamodel.RoleMembershipOptions
	memberInherits bool
}

// membershipKey is a membership matched by the names the server stores for
// its two roles; see [roleKey].
type membershipKey struct{ role, member string }

func newMembershipKey(role, member string, semantics identifier.Semantics) membershipKey {
	return membershipKey{role: roleKey(role, semantics), member: roleKey(member, semantics)}
}

// mergeHeldMemberships folds a second row for one membership into the first.
// PostgreSQL 16 keeps one row per grantor, and what the member holds is the
// union of them, so an option is on when any row has it on. An option one row
// does not report stays as the other reports it. first is the row read earlier.
func mergeHeldMemberships(first, next catalog.RoleMembership) catalog.RoleMembership {
	first.AdminOption = first.AdminOption || next.AdminOption
	first.InheritOption = orOption(first.InheritOption, next.InheritOption)
	first.SetOption = orOption(first.SetOption, next.SetOption)
	return first
}

func orOption(a, b *bool) *bool {
	switch {
	case a == nil:
		return b
	case b == nil:
		return a
	default:
		return new(*a || *b)
	}
}

// heldMembershipOptions describes the options the database holds as options a
// declaration states: every option the server reports and the target holds,
// spelled out.
func heldMembershipOptions(live catalog.RoleMembership, caps capability.Capabilities) schemamodel.RoleMembershipOptions {
	var options schemamodel.RoleMembershipOptions
	if caps.Has(capability.RoleMembershipAdmin) {
		options.Admin = new(live.AdminOption)
	}
	if live.InheritOption != nil && caps.Has(capability.RoleMembershipInheritSet) {
		options.Inherit = new(*live.InheritOption)
	}
	if live.SetOption != nil && caps.Has(capability.RoleMembershipInheritSet) {
		options.Set = new(*live.SetOption)
	}
	return options
}

// compareMembershipOptions reports whether a held membership differs from the
// declaration. The default of an option stands for a declaration that does
// not state it. An option the database does not report, or that the target has
// no key for, is left out of the comparison.
func compareMembershipOptions(
	key difftypes.RoleMembershipRef,
	want declaredMembership,
	live catalog.RoleMembership,
	caps capability.Capabilities,
) (difftypes.RoleMembershipChange, bool) {
	from := heldMembershipOptions(live, caps)
	to := from.Clone()
	differs := false
	if caps.Has(capability.RoleMembershipAdmin) {
		to.Admin = new(optionOr(want.options.Admin, false))
		differs = differs || *to.Admin != live.AdminOption
	}
	if caps.Has(capability.RoleMembershipInheritSet) {
		if live.InheritOption != nil {
			to.Inherit = new(optionOr(want.options.Inherit, want.memberInherits))
			differs = differs || *to.Inherit != *live.InheritOption
		}
		if live.SetOption != nil {
			to.Set = new(optionOr(want.options.Set, true))
			differs = differs || *to.Set != *live.SetOption
		}
	}
	return difftypes.RoleMembershipChange{Role: key.Role, Member: key.Member, From: from, To: to}, differs
}

func optionOr(value *bool, fallback bool) bool {
	if value == nil {
		return fallback
	}
	return *value
}

func compareMemberships(a, b difftypes.RoleMembershipRef) int {
	return cmp.Or(cmp.Compare(a.Role, b.Role), cmp.Compare(a.Member, b.Member))
}

// declaredMemberships collects the roles the schema declares, the ones among
// them that own their memberships, and each membership they state, each keyed
// by the names the server stores.
func declaredMemberships(desired *schemamodel.Database, semantics identifier.Semantics) (
	managed, owners map[string]bool,
	declared map[membershipKey]declaredMembership,
) {
	managed = make(map[string]bool, len(desired.Roles))
	owners = make(map[string]bool, len(desired.Roles))
	declared = make(map[membershipKey]declaredMembership)
	for _, role := range desired.Roles {
		managed[roleKey(role.Name, semantics)] = true
		owners[roleKey(role.Name, semantics)] = role.OwnsMemberships()
		for _, group := range role.MemberOf {
			spelled := group
			if group = strings.TrimSpace(group); group != "" {
				options, stated := role.MemberOptions[spelled]
				if !stated {
					options = role.MemberOptions[group]
				}
				declared[newMembershipKey(group, role.Name, semantics)] = declaredMembership{
					ref:     difftypes.RoleMembershipRef{Role: group, Member: role.Name},
					options: options, memberInherits: role.Inherit,
				}
			}
		}
	}
	return managed, owners, declared
}
