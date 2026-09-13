package postgres_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/renderer"
	"ptah.run/internal/planner/dialects/postgres"
	"ptah.run/migration/schemadiff/difftypes"
)

// TestPlanner_GenerateMigrationAST_DefaultPrivileges pins the whole statement
// list for one plan that touches every default-privilege category at once.
//
// The role removal is part of the fixture rather than a separate test. Both
// spellings of the revoke have to reach the server before the DROP ROLE:
// PostgreSQL refuses to drop a role that still holds a default ACL entry, and a
// plan carrying the revoke below the drop renders, lints and passes an
// assertion that only looks for the statements. The order is the claim here,
// which is why this asserts the list rather than membership.
//
// The CREATE SCHEMA is the other claim. A default privilege names its schema in
// the IN SCHEMA clause, not by qualifying an object name, so the plan reaches it
// only if the precondition step reads that field; without it the ALTER below
// runs against a schema nothing created.
func TestPlanner_GenerateMigrationAST_DefaultPrivileges(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{
		DefaultPrivilegesAdded: []difftypes.DefaultPrivilegeRef{
			{Grantor: "app_owner", Schema: "app", ObjectType: "TABLES", Grantee: "app_reader", Privilege: "SELECT"},
			{Grantor: "app_owner", Schema: "app", ObjectType: "SEQUENCES", Grantee: "app_reader", Privilege: "USAGE", WithOption: true},
		},
		DefaultPrivilegeOptionsAdded: []difftypes.DefaultPrivilegeRef{
			{Grantor: "app_owner", Schema: "app", ObjectType: "TABLES", Grantee: "PUBLIC", Privilege: "INSERT", WithOption: true},
		},
		DefaultPrivilegesRemoved: []difftypes.DefaultPrivilegeRef{
			{Grantor: "legacy_owner", Schema: "app", ObjectType: "TABLES", Grantee: "app_reader", Privilege: "DELETE"},
		},
		DefaultPrivilegeOptionsRevoked: []difftypes.DefaultPrivilegeRef{
			{Grantor: "app_owner", Schema: "app", ObjectType: "FUNCTIONS", Grantee: "app_reader", Privilege: "EXECUTE", WithOption: true},
		},
		RolesRemoved: difftypes.RoleChanges{{Name: "legacy_owner"}},
	}

	nodes, err := postgres.New().GenerateMigrationAST(diff)
	c.Assert(err, qt.IsNil)
	sql, err := renderer.RenderSQL("postgres", nodes...)
	c.Assert(err, qt.IsNil)
	sql = legacyRenderedSQL(sql)
	lines := strings.Split(strings.TrimSpace(sql), "\n")
	c.Assert(lines, qt.DeepEquals, []string{
		"CREATE SCHEMA IF NOT EXISTS app;",
		"ALTER DEFAULT PRIVILEGES FOR ROLE legacy_owner IN SCHEMA app REVOKE DELETE ON TABLES FROM app_reader;",
		"ALTER DEFAULT PRIVILEGES FOR ROLE app_owner IN SCHEMA app REVOKE GRANT OPTION FOR EXECUTE ON FUNCTIONS FROM app_reader;",
		"ALTER DEFAULT PRIVILEGES FOR ROLE app_owner IN SCHEMA app GRANT SELECT ON TABLES TO app_reader;",
		"ALTER DEFAULT PRIVILEGES FOR ROLE app_owner IN SCHEMA app GRANT USAGE ON SEQUENCES TO app_reader WITH GRANT OPTION;",
		"ALTER DEFAULT PRIVILEGES FOR ROLE app_owner IN SCHEMA app GRANT INSERT ON TABLES TO PUBLIC WITH GRANT OPTION;",
		"-- WARNING: Ensure no other objects depend on this role",
		"DROP ROLE IF EXISTS legacy_owner;",
	})
}

// TestPlanner_GenerateMigrationAST_DefaultPrivilegeRemovalIsNeverADrop is the
// control for the statement the removal must not become.
//
// pg_default_acl holds no object to drop: the row is the privilege list, and it
// disappears when its last privilege goes. A removal planned as a DROP would
// name something that does not exist, and a removal planned as nothing at all
// would leave the privilege in place while the plan reported success.
func TestPlanner_GenerateMigrationAST_DefaultPrivilegeRemovalIsNeverADrop(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{
		DefaultPrivilegesRemoved: []difftypes.DefaultPrivilegeRef{
			{Grantor: "app_owner", Schema: "app", ObjectType: "TABLES", Grantee: "app_reader", Privilege: "SELECT"},
		},
	}

	nodes, err := postgres.New().GenerateMigrationAST(diff)
	c.Assert(err, qt.IsNil)
	sql, err := renderer.RenderSQL("postgres", nodes...)
	c.Assert(err, qt.IsNil)

	c.Assert(sql, qt.Contains, "ALTER DEFAULT PRIVILEGES")
	c.Assert(sql, qt.Contains, "REVOKE SELECT ON TABLES FROM")
	c.Assert(sql, qt.Not(qt.Contains), "DROP")
}
