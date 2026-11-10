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

// An ACL can hold the same privilege from more than one grantor. The plan
// must retain the row it cannot safely revoke, regardless of catalog order.
func TestGrantsWithSemantics_KeepsForeignGrantorForRemoval(t *testing.T) {
	fromRunner := catalog.Grant{
		Role: "reader", Privilege: "SELECT", ObjectType: "TABLE",
		Schema: "public", ObjectName: "events", GrantedBy: "runner",
	}
	fromOwner := fromRunner
	fromOwner.GrantedBy = "owner"

	for _, test := range []struct {
		name   string
		grants []catalog.Grant
	}{
		{name: "runner first", grants: []catalog.Grant{fromRunner, fromOwner}},
		{name: "owner first", grants: []catalog.Grant{fromOwner, fromRunner}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := &schemamodel.Database{Roles: []schemamodel.Role{{Name: "reader"}}}
			database := &catalog.Database{ConnectedRole: "runner", Grants: test.grants}
			diff := &difftypes.SchemaDiff{}

			compare.GrantsWithSemantics(desired, database, diff, identifier.ForDialect("postgres"))

			c.Assert(diff.ConnectedRole, qt.Equals, "runner")
			c.Assert(diff.GrantsRemoved, qt.HasLen, 1)
			c.Assert(diff.GrantsRemoved[0].GrantedBy, qt.Equals, "owner")
		})
	}
}

// The option belongs to the grantor's ACL row too; keeping the first row
// would let a plain REVOKE GRANT OPTION FOR leave another row in place.
func TestGrantsWithSemantics_KeepsForeignGrantorForOptionRevoke(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{
		Roles:  []schemamodel.Role{{Name: "reader"}},
		Grants: []schemamodel.Grant{{Role: "reader", Privileges: []string{"SELECT"}, OnTable: "events"}},
	}
	database := &catalog.Database{
		ConnectedRole: "runner",
		Grants: []catalog.Grant{
			{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", Schema: "public", ObjectName: "events", WithOption: true, GrantedBy: "runner"},
			{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", Schema: "public", ObjectName: "events", WithOption: true, GrantedBy: "owner"},
		},
	}
	diff := &difftypes.SchemaDiff{}

	compare.GrantsWithSemantics(desired, database, diff, identifier.ForDialect("postgres"))

	c.Assert(diff.GrantsRemoved, qt.HasLen, 0)
	c.Assert(diff.GrantOptionsRevoked, qt.HasLen, 1)
	c.Assert(diff.GrantOptionsRevoked[0].GrantedBy, qt.Equals, "owner")
}

func TestGrantsWithSemantics_SuperuserKeepsNonownerGrantor(t *testing.T) {
	owner := catalog.Grant{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", Schema: "public", ObjectName: "events", GrantedBy: "owner", ObjectOwner: "owner"}
	delegate := owner
	delegate.GrantedBy = "delegate"
	for _, grants := range [][]catalog.Grant{{owner, delegate}, {delegate, owner}} {
		c := qt.New(t)
		desired := &schemamodel.Database{Roles: []schemamodel.Role{{Name: "reader"}}}
		database := &catalog.Database{ConnectedRole: "admin", ConnectedRoleSuperuser: true, Grants: grants}
		diff := &difftypes.SchemaDiff{}
		compare.GrantsWithSemantics(desired, database, diff, identifier.ForDialect("postgres"))
		c.Assert(diff.GrantsRemoved, qt.HasLen, 1)
		c.Assert(diff.GrantsRemoved[0].GrantedBy, qt.Equals, "delegate")
	}
}
