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

	db, err := yamlschema.Parse([]byte(document))

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
