package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/coverage"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/internal/compare"
)

func compareMemberships(
	caps capability.Capabilities,
	roles []schemamodel.Role,
	held ...catalog.RoleMembership,
) *difftypes.SchemaDiff {
	diff := &difftypes.SchemaDiff{}
	compare.RoleMemberships(&schemamodel.Database{Roles: roles}, &catalog.Database{RoleMemberships: held}, diff, caps)
	return diff
}

func membershipRoles(inherit bool, options schemamodel.RoleMembershipOptions) []schemamodel.Role {
	app := schemamodel.Role{Name: "app", Inherit: inherit, MemberOf: []string{"readers"}}
	if !options.IsZero() {
		app.MemberOptions = map[string]schemamodel.RoleMembershipOptions{"readers": options}
	}
	return []schemamodel.Role{{Name: "readers", Inherit: true}, app}
}

func heldEdge(admin bool, inherit, set *bool) catalog.RoleMembership {
	return catalog.RoleMembership{Role: "readers", Member: "app", AdminOption: admin, InheritOption: inherit, SetOption: set}
}

// TestRoleMemberships_Options_NoChange covers both directions of the default:
// a declaration that states nothing matches a membership holding the defaults,
// and so does one that states exactly the defaults.
func TestRoleMemberships_Options_NoChange(t *testing.T) {
	tests := []struct {
		name    string
		caps    capability.Capabilities
		inherit bool
		options schemamodel.RoleMembershipOptions
		held    catalog.RoleMembership
	}{
		{name: "nothing stated against the defaults", caps: capability.Postgres16(), inherit: true,
			held: heldEdge(false, new(true), new(true))},
		{name: "the defaults stated against the defaults", caps: capability.Postgres16(), inherit: true,
			options: schemamodel.RoleMembershipOptions{Admin: new(false), Inherit: new(true), Set: new(true)},
			held:    heldEdge(false, new(true), new(true))},
		{name: "inherit follows a member that does not inherit", caps: capability.Postgres16(), inherit: false,
			held: heldEdge(false, new(false), new(true))},
		{name: "a stated option equal to the held one", caps: capability.Postgres16(), inherit: true,
			options: schemamodel.RoleMembershipOptions{Admin: new(true), Inherit: new(false), Set: new(false)},
			held:    heldEdge(true, new(false), new(false))},
		{name: "options a server before 16 does not report are not compared", caps: capability.Postgres14(), inherit: true,
			held: heldEdge(false, nil, nil)},
		{name: "a stated admin option on MySQL", caps: capability.MySQL84(), inherit: true,
			options: schemamodel.RoleMembershipOptions{Admin: new(true)}, held: heldEdge(true, nil, nil)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := compareMemberships(test.caps, membershipRoles(test.inherit, test.options), test.held)
			c.Assert(diff.RoleMembershipsAdded, qt.HasLen, 0)
			c.Assert(diff.RoleMembershipsModified, qt.HasLen, 0)
			c.Assert(diff.RoleMembershipsRemoved, qt.HasLen, 0)
			c.Assert(diff.Matched.RoleMemberships, qt.Equals, 1)
		})
	}
}

func TestRoleMemberships_Options_Modified(t *testing.T) {
	t.Run("a held option the declaration does not state returns to the default", func(t *testing.T) {
		c := qt.New(t)
		diff := compareMemberships(capability.Postgres16(), membershipRoles(true, schemamodel.RoleMembershipOptions{}),
			heldEdge(true, new(true), new(false)))
		c.Assert(diff.RoleMembershipsModified, qt.DeepEquals, []difftypes.RoleMembershipChange{{
			Role: "readers", Member: "app",
			From: schemamodel.RoleMembershipOptions{Admin: new(true), Inherit: new(true), Set: new(false)},
			To:   schemamodel.RoleMembershipOptions{Admin: new(false), Inherit: new(true), Set: new(true)},
		}})
		c.Assert(diff.Matched.RoleMemberships, qt.Equals, 0)
	})
	t.Run("a stated option the server does not hold", func(t *testing.T) {
		c := qt.New(t)
		diff := compareMemberships(capability.Postgres14(),
			membershipRoles(true, schemamodel.RoleMembershipOptions{Admin: new(true)}), heldEdge(false, nil, nil))
		c.Assert(diff.RoleMembershipsModified, qt.DeepEquals, []difftypes.RoleMembershipChange{{
			Role: "readers", Member: "app",
			From: schemamodel.RoleMembershipOptions{Admin: new(false)},
			To:   schemamodel.RoleMembershipOptions{Admin: new(true)},
		}})
	})
	t.Run("two rows of one membership are read as the union", func(t *testing.T) {
		c := qt.New(t)
		diff := compareMemberships(capability.Postgres16(), membershipRoles(true, schemamodel.RoleMembershipOptions{}),
			heldEdge(false, new(true), new(true)), heldEdge(true, new(false), new(true)))
		c.Assert(diff.RoleMembershipsModified, qt.HasLen, 1)
		c.Assert(*diff.RoleMembershipsModified[0].From.Admin, qt.IsTrue)
		c.Assert(*diff.RoleMembershipsModified[0].From.Inherit, qt.IsTrue)
	})
}

func TestRoleMemberships_Options_AddedAndRemovedCarryTheirOptions(t *testing.T) {
	c := qt.New(t)

	added := compareMemberships(capability.Postgres16(),
		membershipRoles(true, schemamodel.RoleMembershipOptions{Admin: new(true), Set: new(false)}))
	removed := compareMemberships(capability.Postgres16(),
		[]schemamodel.Role{{Name: "readers", Inherit: true}, {Name: "app", Inherit: true}}, heldEdge(true, new(false), new(true)))

	c.Assert(added.RoleMembershipsAdded, qt.DeepEquals, []difftypes.RoleMembershipRef{{
		Role: "readers", Member: "app", Options: schemamodel.RoleMembershipOptions{Admin: new(true), Set: new(false)},
	}})
	c.Assert(removed.RoleMembershipsRemoved, qt.DeepEquals, []difftypes.RoleMembershipRef{{
		Role: "readers", Member: "app",
		Options: schemamodel.RoleMembershipOptions{Admin: new(true), Inherit: new(false), Set: new(true)},
	}})
}

// TestRoleMemberships_AMembershipOfARoleTheReadRefusedIsUndecided plans no
// GRANT for a role the read did not describe: the role is itself an undecided
// addition, and the statement would name it before it exists.
func TestRoleMemberships_AMembershipOfARoleTheReadRefusedIsUndecided(t *testing.T) {
	c := qt.New(t)
	database := &catalog.Database{NotDescribed: coverage.Set{}.With(coverage.Refused(coverage.Role))}
	diff := &difftypes.SchemaDiff{}

	compare.RoleMemberships(&schemamodel.Database{Roles: membershipRoles(true, schemamodel.RoleMembershipOptions{})},
		database, diff, capability.Postgres16())

	c.Assert(diff.RoleMembershipsAdded, qt.HasLen, 0)
	c.Assert(diff.RoleMembershipsModified, qt.HasLen, 0)
}

// TestRoleMemberships_EachEndOfAnUndecidedRoleDecidesAlone holds the guard to
// both roles: the member not described, and the group not described.
func TestRoleMemberships_EachEndOfAnUndecidedRoleDecidesAlone(t *testing.T) {
	for _, name := range []string{"app", "readers"} {
		t.Run(name, func(t *testing.T) {
			c := qt.New(t)
			database := &catalog.Database{NotDescribed: coverage.Set{}.WithObject(coverage.Role, name)}
			diff := &difftypes.SchemaDiff{}

			compare.RoleMemberships(&schemamodel.Database{Roles: membershipRoles(true, schemamodel.RoleMembershipOptions{})},
				database, diff, capability.Postgres16())

			c.Assert(diff.RoleMembershipsAdded, qt.HasLen, 0)
		})
	}
}

// TestRoleMemberships_AMembershipBetweenDeclaredRolesTheDeclarationOmitsIsRemoved
// holds the decision that a declared role whose member_of leaves out a
// membership it holds in another declared role loses it, as a managed role
// loses a grant the declaration does not list.
func TestRoleMemberships_AMembershipBetweenDeclaredRolesTheDeclarationOmitsIsRemoved(t *testing.T) {
	c := qt.New(t)
	roles := []schemamodel.Role{{Name: "readers", Inherit: true}, {Name: "app", Inherit: true}}

	diff := compareMemberships(capability.Postgres16(), roles, heldEdge(false, new(true), new(true)))

	c.Assert(diff.RoleMembershipsRemoved, qt.DeepEquals, []difftypes.RoleMembershipRef{{
		Role: "readers", Member: "app",
		Options: schemamodel.RoleMembershipOptions{Admin: new(false), Inherit: new(true), Set: new(true)},
	}})
	c.Assert(diff.RoleMembershipsAdded, qt.HasLen, 0)
}

// TestRoleMemberships_AMembershipInAnUndeclaredRoleIsLeftAlone is the control:
// the same live edge stays when the declaration does not name the group, and
// when it does not name the member.
func TestRoleMemberships_AMembershipInAnUndeclaredRoleIsLeftAlone(t *testing.T) {
	tests := []struct {
		name  string
		roles []schemamodel.Role
	}{
		{name: "the group is not declared", roles: []schemamodel.Role{{Name: "app", Inherit: true}}},
		{name: "the member is not declared", roles: []schemamodel.Role{{Name: "readers", Inherit: true}}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := compareMemberships(capability.Postgres16(), test.roles, heldEdge(false, new(true), new(true)))
			c.Assert(diff.RoleMembershipsRemoved, qt.HasLen, 0)
			c.Assert(diff.RoleMembershipsAdded, qt.HasLen, 0)
		})
	}
}
