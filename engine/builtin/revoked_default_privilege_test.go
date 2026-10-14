package builtin_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/feature/pgdefaultprivilege"
)

// TestGetOrderedCreateStatements_RevokedDefaultPrivilegeRendersNothing pins
// that a default privilege revoking and granting nothing renders no statement.
// A schema-scoped default is only ever added to the global ones, so a
// database the schema creates has nothing to revoke; a GRANT with no privilege
// list is a statement PostgreSQL refuses.
func TestGetOrderedCreateStatements_RevokedDefaultPrivilegeRendersNothing(t *testing.T) {
	c := qt.New(t)
	db := &schemamodel.Database{FeatureObjects: must.Must(schemaext.NewObjects(
		defaultPrivilegeDeclaration("app_owner", "app", "TABLES", "app_reader", nil, []string{"INSERT"}),
		defaultPrivilegeDeclaration("app_owner", "app", "SEQUENCES", "app_reader", []pgdefaultprivilege.Grant{{Privilege: "USAGE"}}, []string{"UPDATE"}),
	))}

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
	db := &schemamodel.Database{FeatureObjects: must.Must(schemaext.NewObjects(
		defaultPrivilegeDeclaration("app_owner", "", "FUNCTIONS", "PUBLIC", nil, []string{"EXECUTE"}),
		defaultPrivilegeDeclaration("app_owner", "", "SEQUENCES", "app_owner", nil, []string{"USAGE", "SELECT", "UPDATE"}),
		defaultPrivilegeDeclaration("app_owner", "", "SCHEMAS", "app_reader", []pgdefaultprivilege.Grant{{Privilege: "USAGE"}}, nil),
	))}

	statements, err := builtin.GetOrderedCreateStatements(db, platform.Postgres)

	c.Assert(err, qt.IsNil)
	var rendered []string
	for _, statement := range statements {
		rendered = append(rendered, strings.Split(strings.TrimSpace(statement), "\n")...)
	}
	c.Assert(rendered, qt.DeepEquals, []string{
		`ALTER DEFAULT PRIVILEGES FOR ROLE "app_owner" REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;`,
		`ALTER DEFAULT PRIVILEGES FOR ROLE "app_owner" GRANT USAGE ON SCHEMAS TO "app_reader";`,
		`ALTER DEFAULT PRIVILEGES FOR ROLE "app_owner" REVOKE ALL ON SEQUENCES FROM "app_owner";`,
	})
}

func defaultPrivilegeDeclaration(grantor, schema, objectType, grantee string, grants []pgdefaultprivilege.Grant, revoked []string) schemaext.Object {
	ref := must.Must(pgdefaultprivilege.Ref(pgdefaultprivilege.Identity{Grantor: grantor, Schema: schema, ObjectType: objectType, Grantee: grantee}, identifier.ForDialect("postgres")))
	return must.Must(pgdefaultprivilege.DesiredObject(ref, pgdefaultprivilege.Desired{Privileges: grants, Revoked: revoked}))
}
