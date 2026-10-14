package goschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/goschema"
	"ptah.run/core/ptaherr"
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

// TestParseSource_ObjectGrants_HappyPath reads a grant and a revoke on each
// kind of scheme object the model lists, by the attribute the kind is declared
// with, spelled as on_table spells a table.
func TestParseSource_ObjectGrants_HappyPath(t *testing.T) {
	for _, kind := range schemamodel.GrantObjectKinds() {
		t.Run(kind.Attribute, func(t *testing.T) {
			c := qt.New(t)

			database, err := goschema.ParseSource(noOwners, "models.go", `package test

//ptah:schema:grant role="readers" privilege="SELECT ROW" `+kind.Attribute+`="app.events"
//ptah:schema:revoke role="writers" privilege="ydb.generic.write" `+kind.Attribute+`="app.events"
type Access struct{}
`)

			c.Assert(err, qt.IsNil)
			c.Assert(database.Grants, qt.DeepEquals, []schemamodel.Grant{{StructName: "Access", Role: "readers",
				Privileges: []string{"SELECT ROW"}, OnObject: "app.events", ObjectKind: kind.Kind}})
			c.Assert(database.RevokedGrants, qt.DeepEquals, []schemamodel.Grant{{StructName: "Access", Role: "writers",
				Privileges: []string{"YDB.GENERIC.WRITE"}, OnObject: "app.events", ObjectKind: kind.Kind}})
		})
	}
}

// TestParseSource_ObjectGrants_FailurePath refuses a grant that names two
// scheme objects, which one target cannot hold, rather than keeping one of
// them.
func TestParseSource_ObjectGrants_FailurePath(t *testing.T) {
	c := qt.New(t)

	database, err := goschema.ParseSource(noOwners, "models.go", `package test

//ptah:schema:grant role="readers" privilege="SELECT ROW" on_topic="app.events" on_secret="app.pw"
type Access struct{}
`)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidAttributeValue)
	c.Assert(err, qt.ErrorMatches, `.*on_topic and on_secret on //ptah:schema:grant at .* each name a target; a grant names one object.*`)
	c.Assert(database, qt.DeepEquals, schemamodel.Database{})
}
