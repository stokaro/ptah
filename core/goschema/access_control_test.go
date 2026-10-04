package goschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
)

// TestParseSource_AccessControl_HappyPath reads the attributes a YDB access
// model is declared with: a group, the groups a role is a member of, and a
// grant or a revoke on the database itself. Each is a declaration a target
// without the key refuses, so it has to arrive in the model to be refused.
func TestParseSource_AccessControl_HappyPath(t *testing.T) {
	c := qt.New(t)
	database := parseStringAsGoFile(c, `
package test

//ptah:schema:role name="readers" group="true"
//ptah:schema:role name="app" login="true" member_of="readers, DATA-READERS"
//ptah:schema:grant role="app" privilege="CONNECT" on_database="true"
//ptah:schema:revoke role="app" privilege="DROP" on_database="true"
type Access struct{}
`)

	c.Assert(database.Roles, qt.HasLen, 2)
	c.Assert(database.Roles[0].Name, qt.Equals, "readers")
	c.Assert(database.Roles[0].Group, qt.IsTrue)
	c.Assert(database.Roles[0].MemberOf, qt.IsNil)
	c.Assert(database.Roles[1].Group, qt.IsFalse)
	c.Assert(database.Roles[1].MemberOf, qt.DeepEquals, []string{"readers", "DATA-READERS"})
	c.Assert(database.Grants, qt.HasLen, 1)
	c.Assert(database.Grants[0].OnDatabase, qt.IsTrue)
	c.Assert(database.Grants[0].TargetKey(), qt.Equals, "DATABASE")
	c.Assert(database.RevokedGrants, qt.HasLen, 1)
	c.Assert(database.RevokedGrants[0], qt.DeepEquals, schemamodel.Grant{
		StructName: "Access", Role: "app", Privileges: []string{"DROP"}, OnDatabase: true,
	})
}
