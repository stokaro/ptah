package builtin_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
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

	statements, err := builtin.GetOrderedCreateStatements(db, platform.Postgres)

	c.Assert(err, qt.IsNil)
	rendered := strings.Join(statements, "\n")
	c.Assert(strings.Count(rendered, "ALTER DEFAULT PRIVILEGES"), qt.Equals, 1)
	c.Assert(rendered, qt.Contains, "GRANT USAGE ON SEQUENCES")
}

// TestGetOrderedCreateStatements_GlobalDefaultPrivilegeRendersItsRevokes pins
// the global default, which has no IN SCHEMA and starts from the built-in one
// (stokaro/ptah#3772). A database the schema creates holds the built-in
// default, so a global revoke is a statement it needs. A revoke from the owner
// that names every privilege of the class is written as ALL, which on
// CockroachDB takes privileges the list cannot name; a revoke from PUBLIC
// keeps the spelling it was written in.
func TestGetOrderedCreateStatements_GlobalDefaultPrivilegeRendersItsRevokes(t *testing.T) {
	c := qt.New(t)
	db := &schemamodel.Database{DefaultPrivileges: []schemamodel.DefaultPrivilege{
		{Grantor: "app_owner", ObjectType: "FUNCTIONS", Grantee: "PUBLIC", Revoked: []string{"EXECUTE"}},
		{Grantor: "app_owner", ObjectType: "SEQUENCES", Grantee: "app_owner", Revoked: []string{"USAGE", "SELECT", "UPDATE"}},
		{
			Grantor: "app_owner", ObjectType: "SCHEMAS", Grantee: "app_reader",
			Privileges: []schemamodel.PrivilegeGrant{{Privilege: "USAGE"}},
		},
	}}

	statements, err := builtin.GetOrderedCreateStatements(db, platform.Postgres)

	c.Assert(err, qt.IsNil)
	var rendered []string
	for _, statement := range statements {
		rendered = append(rendered, strings.Split(strings.TrimSpace(statement), "\n")...)
	}
	c.Assert(rendered, qt.DeepEquals, []string{
		`ALTER DEFAULT PRIVILEGES FOR ROLE "app_owner" REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;`,
		`ALTER DEFAULT PRIVILEGES FOR ROLE "app_owner" REVOKE ALL ON SEQUENCES FROM "app_owner";`,
		`ALTER DEFAULT PRIVILEGES FOR ROLE "app_owner" GRANT USAGE ON SCHEMAS TO "app_reader";`,
	})
}
