package membershipplan_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/coverage"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/membershipplan"
)

// TestDeclared_Fate decides what a plan does with each membership a MySQL
// database holds: it removes one between two declared roles that the member,
// which states member_of, leaves out, keeps one in a role the schema does not
// declare or one the declaration does not describe, and reports one a member
// that never wrote member_of keeps as unmanaged.
func TestDeclared_Fate(t *testing.T) {
	tests := []struct {
		name   string
		roles  []schemamodel.Role
		limits coverage.Set
		held   catalog.RoleMembership
		want   membershipplan.Fate
	}{
		{
			name:  "left out by a member that owns its memberships",
			roles: []schemamodel.Role{{Name: "gcp"}, {Name: "gcmm", MemberOfStated: true}},
			held:  catalog.RoleMembership{Role: "gcp", Member: "gcmm"},
			want:  membershipplan.Removed,
		},
		{
			name:  "stated",
			roles: []schemamodel.Role{{Name: "gcp"}, {Name: "gcmm", MemberOf: []string{"gcp"}}},
			held:  catalog.RoleMembership{Role: "gcp", Member: "gcmm"},
			want:  membershipplan.Kept,
		},
		{
			name:  "in a role the schema does not declare",
			roles: []schemamodel.Role{{Name: "gcmm", MemberOfStated: true}},
			held:  catalog.RoleMembership{Role: "gcp", Member: "gcmm"},
			want:  membershipplan.Kept,
		},
		{
			name:  "of a member that never wrote member_of",
			roles: []schemamodel.Role{{Name: "gcp"}, {Name: "gcmm"}},
			held:  catalog.RoleMembership{Role: "gcp", Member: "gcmm"},
			want:  membershipplan.Unmanaged,
		},
		{
			name:   "of a member whose memberships the declaration does not describe",
			roles:  []schemamodel.Role{{Name: "gcp"}, {Name: "gcmm", MemberOfStated: true}},
			limits: coverage.Set{}.WithObject(coverage.RoleMembership, "gcmm"),
			held:   catalog.RoleMembership{Role: "gcp", Member: "gcmm"},
			want:   membershipplan.Kept,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := &schemamodel.Database{Roles: test.roles, NotDescribed: test.limits}
			plan := membershipplan.Read(desired, identifier.ForDialect("mysql"), capability.ForDialect("mysql"))

			c.Assert(plan.Fate(&catalog.Database{}, test.held), qt.Equals, test.want)
		})
	}
}

// TestDeclared_FateOfAnUndescribedRead keeps a membership whose member's
// memberships the read does not describe: a removal there would take what
// the read could not see.
func TestDeclared_FateOfAnUndescribedRead(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{Roles: []schemamodel.Role{{Name: "gcp"}, {Name: "gcmm", MemberOfStated: true}}}
	plan := membershipplan.Read(desired, identifier.ForDialect("mysql"), capability.ForDialect("mysql"))
	database := &catalog.Database{NotDescribed: coverage.Set{}.WithKind(coverage.RoleMembership)}

	c.Assert(plan.Fate(database, catalog.RoleMembership{Role: "gcp", Member: "gcmm"}), qt.Equals, membershipplan.Kept)
}

// TestRead_Stated lists each membership the declaration states, with its
// options, on a target that plans memberships, and none on one that does not:
// Oracle plans none, and the zero value declares nothing either.
func TestRead_Stated(t *testing.T) {
	admin := true
	desired := &schemamodel.Database{Roles: []schemamodel.Role{{
		Name: "gcmm", Inherit: true, MemberOf: []string{" gcp", ""},
		MemberOptions: map[string]schemamodel.RoleMembershipOptions{" gcp": {Admin: &admin}},
	}}}
	tests := []struct {
		name    string
		dialect string
		want    map[membershipplan.Key]membershipplan.Stated
	}{
		{name: "MySQL", dialect: "mysql", want: map[membershipplan.Key]membershipplan.Stated{
			{Role: "gcp", Member: "gcmm"}: {
				Ref:            difftypes.RoleMembershipRef{Role: "gcp", Member: "gcmm"},
				Options:        schemamodel.RoleMembershipOptions{Admin: &admin},
				MemberInherits: true,
			},
		}},
		{name: "Oracle", dialect: "oracle"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			plan := membershipplan.Read(desired, identifier.ForDialect(test.dialect), capability.ForDialect(test.dialect))

			c.Assert(plan.Stated(), qt.DeepEquals, test.want)
			c.Assert(plan.Fate(&catalog.Database{}, catalog.RoleMembership{Role: "other", Member: "gcmm"}), qt.Equals, membershipplan.Kept)
		})
	}
}

// TestDeclared_KeyOf matches a membership by the names the server stores, as
// the semantics it is read with say: Oracle's fold an unquoted name to upper
// case, and MySQL's keep it as written.
func TestDeclared_KeyOf(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		want    membershipplan.Key
	}{
		{name: "a fold", dialect: "oracle", want: membershipplan.Key{Role: "READERS", Member: "APP"}},
		{name: "no fold", dialect: "mysql", want: membershipplan.Key{Role: "Readers", Member: "App"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			plan := membershipplan.Read(&schemamodel.Database{}, identifier.ForDialect(test.dialect), capability.ForDialect("mysql"))

			c.Assert(plan.KeyOf("Readers", "App"), qt.Equals, test.want)
		})
	}
}
