package yamlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/core/yamlschema"
)

// TestParse_AccessControl_HappyPath pins the YAML spelling of a YDB group, a
// role's member_of list and a grant or revoke on the database, with the keys
// the Go annotations use.
func TestParse_AccessControl_HappyPath(t *testing.T) {
	c := qt.New(t)
	document := `roles:
  readers:
    group: true
  app:
    login: true
    member_of: [readers, DATA-READERS]
grants:
  app_connects:
    role: app
    privileges: [CONNECT]
    on_database: true
revokes:
  app_drops:
    role: app
    privileges: [DROP]
    on_database: true
`

	db, err := yamlschema.Parse(t.Context(), noOwners, []byte(document))

	c.Assert(err, qt.IsNil)
	c.Assert(db.Roles, qt.DeepEquals, []schemamodel.Role{
		{Name: "app", Login: true, Inherit: true, MemberOf: []string{"readers", "DATA-READERS"}},
		{Name: "readers", Inherit: true, Group: true},
	})
	c.Assert(db.Grants, qt.DeepEquals, []schemamodel.Grant{
		{Role: "app", Privileges: []string{"CONNECT"}, OnDatabase: true},
	})
	c.Assert(db.RevokedGrants, qt.DeepEquals, []schemamodel.Grant{
		{Role: "app", Privileges: []string{"DROP"}, OnDatabase: true},
	})
}

// TestParse_ObjectGrants_HappyPath reads a grant and a revoke on each kind of
// scheme object the model lists, by the key the Go annotations name it with,
// spelled as on_table spells a table.
func TestParse_ObjectGrants_HappyPath(t *testing.T) {
	for _, kind := range schemamodel.GrantObjectKinds() {
		t.Run(kind.Attribute, func(t *testing.T) {
			c := qt.New(t)
			document := `grants:
  readers_read:
    role: readers
    privileges: [SELECT ROW]
    ` + kind.Attribute + `: app.events
revokes:
  writers_write:
    role: writers
    privileges: [ydb.generic.write]
    ` + kind.Attribute + `: app.events
`

			db, err := yamlschema.Parse(t.Context(), noOwners, []byte(document))

			c.Assert(err, qt.IsNil)
			c.Assert(db.Grants, qt.DeepEquals, []schemamodel.Grant{
				{Role: "readers", Privileges: []string{"SELECT ROW"}, OnObject: "app.events", ObjectKind: kind.Kind},
			})
			c.Assert(db.RevokedGrants, qt.DeepEquals, []schemamodel.Grant{
				{Role: "writers", Privileges: []string{"YDB.GENERIC.WRITE"}, OnObject: "app.events", ObjectKind: kind.Kind},
			})
		})
	}
}

// TestParse_ObjectGrants_FailurePath refuses a grant that names two scheme
// objects, which one target cannot hold, rather than keeping one of them.
func TestParse_ObjectGrants_FailurePath(t *testing.T) {
	c := qt.New(t)
	document := `grants:
  readers_read:
    role: readers
    privileges: [SELECT ROW]
    on_topic: app.events
    on_external_table: app.events
`

	db, err := yamlschema.Parse(t.Context(), noOwners, []byte(document))

	c.Assert(err, qt.ErrorMatches, `.*grant "readers_read" names a target with both on_topic and on_external_table; a grant names one object`)
	c.Assert(db, qt.IsNil)
}
