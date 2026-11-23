package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/migration/schemadiff/internal/compare"
)

// TestCurrentPartialRevokes_HappyPath pins which read rows a plan that
// recreates an object writes again as partial revokes: a DENY of any grantee,
// sorted. A grant, a privilege an object's owner revoked from itself and the
// empty row recording an owner's revoked EXECUTE are not partial revokes it
// writes, and CurrentGrants carries none of them either.
func TestCurrentPartialRevokes_HappyPath(t *testing.T) {
	c := qt.New(t)
	database := &catalog.Database{Grants: []catalog.Grant{
		{Role: "reader", Privilege: "EXECUTE", ObjectType: "TABLE", Schema: "dbo", ObjectName: "rt"},
		{Role: "r_d", Privilege: "EXECUTE", ObjectType: "TABLE", Schema: "dbo", ObjectName: "rt", IsPartialRevoke: true},
		{Role: "app_user", Privilege: "VIEW DEFINITION", ObjectType: "TABLE", Schema: "dbo", ObjectName: "rt", IsPartialRevoke: true},
		{Role: "owner", Privilege: "SELECT", ObjectType: "TABLE", Schema: "app", ObjectName: "t", Implicit: true, IsPartialRevoke: true},
		{Role: "owner", ObjectType: "FUNCTION", Schema: "app", ObjectName: "f", Implicit: true, IsPartialRevoke: true},
	}}

	revokes := compare.CurrentPartialRevokes(database)
	grants := compare.CurrentGrants(database)

	c.Assert(revokes, qt.DeepEquals, []difftypes.GrantRef{
		{Role: "app_user", Privilege: "VIEW DEFINITION", ObjectType: "TABLE", ObjectName: "dbo.rt"},
		{Role: "r_d", Privilege: "EXECUTE", ObjectType: "TABLE", ObjectName: "dbo.rt"},
	})
	c.Assert(grants, qt.DeepEquals, []difftypes.GrantRef{
		{Role: "reader", Privilege: "EXECUTE", ObjectType: "TABLE", ObjectName: "dbo.rt"},
	})
}

// A database with no read, or with no partial revoke, has none to write.
func TestCurrentPartialRevokes_HappyPathNone(t *testing.T) {
	c := qt.New(t)

	c.Assert(compare.CurrentPartialRevokes(nil), qt.IsNil)
	c.Assert(compare.CurrentPartialRevokes(&catalog.Database{Grants: []catalog.Grant{
		{Role: "reader", Privilege: "EXECUTE", ObjectType: "TABLE", Schema: "dbo", ObjectName: "rt"},
	}}), qt.IsNil)
}
