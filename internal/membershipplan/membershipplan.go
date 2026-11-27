// Package membershipplan decides which role memberships a plan grants and
// which it removes, from what a desired schema declares about its roles. The
// comparison plans those changes, and the revoke check of
// [ptah.run/internal/grantcover] asks which memberships still pass a
// privilege on once the plan runs. Both read this one decision: a check that
// kept its own copy would agree with the plan when it was written and stop
// agreeing the first time the comparison learned a new rule.
package membershipplan

import (
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/coverage"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
)

// Key is a membership matched by the names the server stores for its two
// roles.
type Key struct{ Role, Member string }

// Stated is one membership a declaration states: the options it states, and
// the member's own inherit attribute, which is the default for the inherit
// option.
type Stated struct {
	// Ref names the two roles as the declaration spells them.
	Ref            difftypes.RoleMembershipRef
	Options        schemamodel.RoleMembershipOptions
	MemberInherits bool
}

// Fate is what a plan does with a membership the database holds.
type Fate int

const (
	// Kept is a membership the plan leaves as it is: the declaration states
	// it, does not manage both of its roles, or does not describe it.
	Kept Fate = iota
	// Unmanaged is a membership between two declared roles that the plan
	// keeps because the member does not own its memberships
	// ([schemamodel.Role.OwnsMemberships]).
	Unmanaged
	// Removed is a membership the plan removes.
	Removed
)

// Declared is what a desired schema says about the memberships of its roles,
// on a target that plans memberships. The zero value declares nothing and
// removes nothing.
type Declared struct {
	semantics identifier.Semantics
	desired   *schemamodel.Database
	managed   map[string]bool
	owners    map[string]bool
	stated    map[Key]Stated
}

// Read collects what desired declares about memberships, keyed by the names
// semantics says the server stores. On a target without
// [capability.RoleMembership] no planner plans a membership, so the result
// declares nothing and removes nothing there.
func Read(desired *schemamodel.Database, semantics identifier.Semantics, caps capability.Capabilities) Declared {
	if desired == nil || !caps.Has(capability.RoleMembership) {
		return Declared{}
	}
	declared := Declared{
		semantics: semantics,
		desired:   desired,
		managed:   make(map[string]bool, len(desired.Roles)),
		owners:    make(map[string]bool, len(desired.Roles)),
		stated:    make(map[Key]Stated),
	}
	for _, role := range desired.Roles {
		declared.managed[declared.roleKey(role.Name)] = true
		declared.owners[declared.roleKey(role.Name)] = role.OwnsMemberships()
		for _, group := range role.MemberOf {
			spelled := group
			if group = strings.TrimSpace(group); group == "" {
				continue
			}
			options, stated := role.MemberOptions[spelled]
			if !stated {
				options = role.MemberOptions[group]
			}
			declared.stated[declared.KeyOf(group, role.Name)] = Stated{
				Ref:     difftypes.RoleMembershipRef{Role: group, Member: role.Name},
				Options: options, MemberInherits: role.Inherit,
			}
		}
	}
	return declared
}

// KeyOf is the key of the membership of member in role.
func (d Declared) KeyOf(role, member string) Key {
	return Key{Role: d.roleKey(role), Member: d.roleKey(member)}
}

// Stated is every membership the declaration states, by key. A caller must
// not change it.
func (d Declared) Stated() map[Key]Stated {
	return d.stated
}

// Fate is what the plan does with held, a membership database holds.
//
// It is removed only when both roles are declared and the member owns its
// memberships, and only where both the declaration and the read describe the
// member's memberships. A membership in a role the schema does not declare is
// the server's: a cluster adds every new YDB user to its all-users group, and
// a removal planned because a declaration did not list that group would take
// away the right to connect. A member that never wrote member_of leaves its
// memberships to the server too, and such a membership between two declared
// roles is [Unmanaged].
func (d Declared) Fate(database *catalog.Database, held catalog.RoleMembership) Fate {
	if d.desired == nil || database == nil ||
		!d.desired.NotDescribed.Describes(coverage.RoleMembership, held.Member) ||
		!database.NotDescribed.Describes(coverage.RoleMembership, held.Member) {
		return Kept
	}
	key := d.KeyOf(held.Role, held.Member)
	if _, isStated := d.stated[key]; isStated || !d.managed[key.Role] || !d.managed[key.Member] {
		return Kept
	}
	if !d.owners[key.Member] {
		return Unmanaged
	}
	return Removed
}

// roleKey is the name the server stores for a role the declaration names.
func (d Declared) roleKey(name string) string {
	return d.semantics.StoredName(name)
}
