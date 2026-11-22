package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/internal/compare"
)

// TestRoles_StoredNames matches a declared role with the role Oracle stores it
// under, in a read already given the spelling that addresses its names: a bare
// reader is READER, and a quoted "reader" is the role created quoted, whether
// the read describes the role or only reports it out of scope.
func TestRoles_StoredNames(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		database *catalog.Database
		added    []string
	}{
		{name: "a bare role is the described role in upper case", declared: "reader",
			database: &catalog.Database{Roles: []catalog.Role{{Name: "READER"}}}},
		{name: "a quoted role is the role out of scope created quoted", declared: `"reader"`,
			database: &catalog.Database{RolesOutOfScope: []catalog.Role{{Name: `"reader"`}}}},
		{name: "a bare role is not the role created quoted", declared: "reader",
			database: &catalog.Database{Roles: []catalog.Role{{Name: `"reader"`}}}, added: []string{"reader"}},
		{name: "a quoted role is not the role in upper case", declared: `"reader"`,
			database: &catalog.Database{Roles: []catalog.Role{{Name: "READER"}}}, added: []string{`"reader"`}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := &schemamodel.Database{Roles: []schemamodel.Role{{Name: test.declared}}}
			diff := &difftypes.SchemaDiff{}

			compare.Roles(desired, test.database, diff, identifier.ForDialect("oracle"), compare.CoverageOf(desired, test.database))

			var added []string
			for _, role := range diff.RolesAdded {
				added = append(added, role.Name)
			}
			c.Assert(added, qt.DeepEquals, test.added)
		})
	}
}

// TestObjectComments_RoleStoredNames matches a role's comment the way
// [compare.Roles] matches the role, under a target that folds unquoted names:
// the comment of a bare reader is the comment of READER.
func TestObjectComments_RoleStoredNames(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{Roles: []schemamodel.Role{{Name: "reader", Comment: "reads"}}}
	database := &catalog.Database{Roles: []catalog.Role{{Name: "READER"}}}
	diff := &difftypes.SchemaDiff{}

	compare.ObjectComments(desired, database, diff, &config.CompareOptions{Dialect: "oracle"},
		capability.Of(map[capability.Capability]bool{capability.RoleComments: true}), identifier.ForDialect("oracle"))

	c.Assert(diff.ObjectCommentsChanged, qt.HasLen, 1)
	c.Assert(diff.ObjectCommentsChanged[0].Kind, qt.Equals, difftypes.CommentedRole)
	c.Assert(diff.ObjectCommentsChanged[0].Name, qt.Equals, "reader")
}

// TestRoleMemberships_StoredNames matches a declared membership with the one
// Oracle stores under the roles' names, in a read already given the spelling
// that addresses them: a bare reader member of team is READER in TEAM, and
// the membership of the role created quoted is another one. A planned
// addition names the roles as declared.
func TestRoleMemberships_StoredNames(t *testing.T) {
	tests := []struct {
		name  string
		held  []catalog.RoleMembership
		added []difftypes.RoleMembershipRef
	}{
		{name: "the membership of the roles in upper case", held: []catalog.RoleMembership{{Role: "TEAM", Member: "READER"}}},
		{
			name:  "the membership of the role created quoted",
			held:  []catalog.RoleMembership{{Role: "TEAM", Member: `"reader"`}},
			added: []difftypes.RoleMembershipRef{{Role: "team", Member: "reader"}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := &schemamodel.Database{Roles: []schemamodel.Role{
				{Name: "reader", MemberOf: []string{"team"}, MemberOfStated: true},
				{Name: "team", Group: true},
			}}
			diff := &difftypes.SchemaDiff{}

			compare.RoleMemberships(desired, &catalog.Database{RoleMemberships: test.held}, diff, identifier.ForDialect("oracle"),
				capability.Of(map[capability.Capability]bool{capability.RoleMembership: true}))

			c.Assert(diff.RoleMembershipsAdded, qt.DeepEquals, test.added)
			c.Assert(diff.RoleMembershipsRemoved, qt.HasLen, 0)
		})
	}
}

// TestRoleMemberships_StoredNamesRemoval removes a membership between two
// declared roles that the declaration does not state, matching both roles by
// the names Oracle stores, and names it as the read does: TEAM, or "team" for
// the group created quoted.
func TestRoleMemberships_StoredNamesRemoval(t *testing.T) {
	tests := []struct {
		name  string
		group string
	}{
		{name: "a group in upper case", group: "TEAM"},
		{name: "a group created quoted", group: `"team"`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := &schemamodel.Database{Roles: []schemamodel.Role{
				{Name: "reader", MemberOfStated: true},
				{Name: test.group, Group: true},
			}}
			database := &catalog.Database{RoleMemberships: []catalog.RoleMembership{{Role: test.group, Member: "READER"}}}
			diff := &difftypes.SchemaDiff{}

			compare.RoleMemberships(desired, database, diff, identifier.ForDialect("oracle"),
				capability.Of(map[capability.Capability]bool{capability.RoleMembership: true}))

			c.Assert(diff.RoleMembershipsRemoved, qt.DeepEquals, []difftypes.RoleMembershipRef{{Role: test.group, Member: "READER"}})
			c.Assert(diff.RoleMembershipsAdded, qt.HasLen, 0)
		})
	}
}
