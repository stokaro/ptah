package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/internal/compare"
)

// A catalog name includes its padding. A grant to the padded principal does
// not satisfy the unpadded principal, and a removal names its actual holder.
func TestGrantsWithOptions_KeepsPrincipalNames(t *testing.T) {
	for _, dialect := range []string{"postgres", "mariadb", "oracle"} {
		for _, held := range []string{" READER", "READER ", "\tREADER", " public"} {
			t.Run(dialect+"/"+held, func(t *testing.T) {
				c := qt.New(t)
				desired := &schemamodel.Database{Grants: []schemamodel.Grant{{Role: "READER", Privileges: []string{"SELECT"}, OnTable: "t"}}}
				current := &catalog.Database{Grants: []catalog.Grant{{
					Role: held, Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "t",
					GrantedBy: " OWNER", ObjectOwner: " OWNER",
				}}}
				diff := &difftypes.SchemaDiff{}

				compare.GrantsWithOptions(desired, current, diff, identifier.ForDialect(dialect), compare.GrantOptions{DesiredComplete: true})

				c.Assert(diff.GrantsAdded, qt.HasLen, 1)
				c.Assert(diff.GrantsAdded[0].Role, qt.Equals, "READER")
				c.Assert(diff.GrantsRemoved, qt.HasLen, 1)
				c.Assert(diff.GrantsRemoved[0].Role, qt.Equals, held)
				c.Assert(diff.GrantsRemoved[0].GrantedBy, qt.Equals, " OWNER")
				c.Assert(diff.GrantsRemoved[0].ObjectOwner, qt.Equals, " OWNER")
			})
		}
	}
}

// A differently padded catalog role cannot satisfy a role declaration.
func TestRoles_KeepsPrincipalNames(t *testing.T) {
	for _, dialect := range []string{"postgres", "mariadb", "oracle"} {
		for _, test := range []struct {
			declared, held string
			added          []string
		}{
			{declared: "READER", held: " READER", added: []string{"READER"}},
			{declared: "READER", held: "READER ", added: []string{"READER"}},
			{declared: "READER", held: "\tREADER", added: []string{"READER"}},
			{declared: " READER", held: " READER"},
			{declared: "READER ", held: "READER "},
			{declared: "\tREADER", held: "\tREADER"},
		} {
			t.Run(dialect+"/"+test.declared+"/"+test.held, func(t *testing.T) {
				c := qt.New(t)
				desired := &schemamodel.Database{Roles: []schemamodel.Role{{Name: test.declared}}}
				current := &catalog.Database{Roles: []catalog.Role{{Name: test.held}}}
				diff := &difftypes.SchemaDiff{}

				compare.Roles(desired, current, diff, identifier.ForDialect(dialect), compare.CoverageOf(desired, current))

				c.Assert(diff.RolesAdded.Names(), qt.DeepEquals, test.added)
				c.Assert(diff.RolesModified, qt.HasLen, 0)
			})
		}
	}
}

func TestCurrentGrants_KeepsPrincipalNames(t *testing.T) {
	c := qt.New(t)
	current := &catalog.Database{Grants: []catalog.Grant{{
		Role: " reader ", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "t",
		GrantedBy: " grantor ", ObjectOwner: " owner ",
	}}}

	c.Assert(compare.CurrentGrants(current), qt.DeepEquals, []difftypes.GrantRef{{
		Role: " reader ", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "t",
		GrantedBy: " grantor ", ObjectOwner: " owner ",
	}})
}

func TestPrincipalNames_SQLServerIgnoresTrailingSpaces(t *testing.T) {
	for _, test := range []struct{ desired, current string }{
		{desired: "reader ", current: "reader"},
		{desired: "reader", current: "reader "},
	} {
		t.Run(test.desired+"/"+test.current, func(t *testing.T) {
			c := qt.New(t)
			desired := &schemamodel.Database{
				Roles:  []schemamodel.Role{{Name: test.desired, Inherit: true}},
				Grants: []schemamodel.Grant{{Role: test.desired, OnTable: "t", Privileges: []string{"SELECT"}}},
			}
			current := &catalog.Database{
				Roles:  []catalog.Role{{Name: test.current, Inherit: true}},
				Grants: []catalog.Grant{{Role: test.current, ObjectType: "TABLE", ObjectName: "t", Privilege: "SELECT"}},
			}
			semantics := identifier.ForDialect("sqlserver")
			diff := &difftypes.SchemaDiff{}

			compare.Roles(desired, current, diff, semantics, compare.CoverageOf(desired, current))
			compare.GrantsWithOptions(desired, current, diff, semantics, compare.GrantOptions{DesiredComplete: true})

			c.Assert(diff.RolesAdded, qt.HasLen, 0)
			c.Assert(diff.RolesModified, qt.HasLen, 0)
			c.Assert(diff.GrantsAdded, qt.HasLen, 0)
			c.Assert(diff.GrantsRemoved, qt.HasLen, 0)
		})
	}
}
