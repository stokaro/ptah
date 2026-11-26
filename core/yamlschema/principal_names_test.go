package yamlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/yamlschema"
	"ptah.run/internal/builtintest"
)

func TestParse_KeepsPrincipalNames(t *testing.T) {
	c := qt.New(t)
	document := `roles:
  " reader ": {}
grants:
  read:
    role: " reader "
    privileges: [SELECT]
    on_table: t
revokes:
  write:
    role: " reader "
    privileges: [INSERT]
    on_table: t
`

	database, err := yamlschema.Parse(t.Context(), builtintest.Runtime().YAML(), []byte(document))

	c.Assert(err, qt.IsNil)
	c.Assert(database.Roles, qt.HasLen, 1)
	c.Assert(database.Roles[0].Name, qt.Equals, " reader ")
	c.Assert(database.Grants, qt.HasLen, 1)
	c.Assert(database.Grants[0].Role, qt.Equals, " reader ")
	c.Assert(database.RevokedGrants, qt.HasLen, 1)
	c.Assert(database.RevokedGrants[0].Role, qt.Equals, " reader ")
}
