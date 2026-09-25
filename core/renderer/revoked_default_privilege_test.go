package renderer_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/renderer"
	"ptah.run/core/schemamodel"
)

// TestGetOrderedCreateStatements_RevokedDefaultPrivilegeRendersNothing pins
// that a default privilege revoking and granting nothing renders no statement.
// A schema-scoped default is only ever added to the global ones, so a
// database the schema creates has nothing to revoke; a GRANT with no privilege
// list is a statement PostgreSQL refuses.
func TestGetOrderedCreateStatements_RevokedDefaultPrivilegeRendersNothing(t *testing.T) {
	c := qt.New(t)
	db := &schemamodel.Database{DefaultPrivileges: []schemamodel.DefaultPrivilege{
		{Grantor: "app_owner", Schema: "app", ObjectType: "TABLES", Grantee: "app_reader", Revoked: []string{"INSERT"}},
		{
			Grantor: "app_owner", Schema: "app", ObjectType: "SEQUENCES", Grantee: "app_reader",
			Privileges: []schemamodel.PrivilegeGrant{{Privilege: "USAGE"}}, Revoked: []string{"UPDATE"},
		},
	}}

	statements, err := renderer.GetOrderedCreateStatements(db, platform.Postgres)

	c.Assert(err, qt.IsNil)
	rendered := strings.Join(statements, "\n")
	c.Assert(strings.Count(rendered, "ALTER DEFAULT PRIVILEGES"), qt.Equals, 1)
	c.Assert(rendered, qt.Contains, "GRANT USAGE ON SEQUENCES")
}
