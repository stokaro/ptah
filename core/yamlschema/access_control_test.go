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

// TestParse_MemberOptions_HappyPath reads the options a membership states, in
// the spelling the Go annotations use.
func TestParse_MemberOptions_HappyPath(t *testing.T) {
	c := qt.New(t)
	document := `roles:
  readers: {}
  app:
    member_of: ["readers(admin;set=false)"]
`

	db, err := yamlschema.Parse(t.Context(), noOwners, []byte(document))

	c.Assert(err, qt.IsNil)
	c.Assert(db.Roles, qt.DeepEquals, []schemamodel.Role{
		{Name: "app", Inherit: true, MemberOf: []string{"readers"},
			MemberOptions: map[string]schemamodel.RoleMembershipOptions{"readers": {Admin: new(true), Set: new(false)}}},
		{Name: "readers", Inherit: true},
	})
}

// TestParse_MemberOfStated_HappyPath tells an empty member_of, which states
// that the role belongs to no role, from an absent or null key, which says
// nothing about its memberships.
func TestParse_MemberOfStated_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		entry string
		want  bool
	}{
		{name: "an empty sequence", entry: "member_of: []", want: true},
		{name: "an empty string", entry: `member_of: ""`, want: true},
		{name: "a null value", entry: "member_of:", want: false},
		{name: "no key", entry: "login: true", want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			document := "roles:\n  app:\n    " + test.entry + "\n"

			db, err := yamlschema.Parse(t.Context(), noOwners, []byte(document))

			c.Assert(err, qt.IsNil)
			c.Assert(db.Roles, qt.HasLen, 1)
			c.Assert(db.Roles[0].MemberOf, qt.HasLen, 0)
			c.Assert(db.Roles[0].MemberOfStated, qt.Equals, test.want)
		})
	}
}

// TestParse_MemberOptions_FailurePath refuses an option the grammar does not
// have, naming the role.
func TestParse_MemberOptions_FailurePath(t *testing.T) {
	c := qt.New(t)
	document := `roles:
  app:
    member_of: ["readers(owner)"]
`

	db, err := yamlschema.Parse(t.Context(), noOwners, []byte(document))

	c.Assert(err, qt.ErrorMatches, `(?s).*role "app".*unknown membership option "owner".*`)
	c.Assert(db, qt.IsNil)
}
