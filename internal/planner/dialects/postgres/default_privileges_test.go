package postgres_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/engine/builtin"
	"ptah.run/feature/pgdefaultprivilege"
	"ptah.run/internal/planner/dialects/postgres"
	"ptah.run/migration/schemadiff/difftypes"
)

func defaultPrivilegeChange(grantor, class, grantee string, change pgdefaultprivilege.Change) schemaext.ChangeRecord {
	return schemaext.ChangeRecord{Subject: must.Must(pgdefaultprivilege.Ref(pgdefaultprivilege.Identity{
		Grantor: grantor, Schema: "app", ObjectType: class, Grantee: grantee,
	}, identifier.ForDialect("postgres"))), Value: &change}
}

// Every ACL operation follows schema creation, and the role's revoke precedes
// its removal. The public planner schedules owner and common steps together.
func TestPlanner_GenerateMigrationAST_DefaultPrivileges(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{
		FeatureChanges: []schemaext.ChangeRecord{
			defaultPrivilegeChange("app_owner", "TABLES", "app_reader", pgdefaultprivilege.Change{Granted: []pgdefaultprivilege.Grant{{Privilege: "SELECT"}}}),
			defaultPrivilegeChange("app_owner", "SEQUENCES", "app_reader", pgdefaultprivilege.Change{Granted: []pgdefaultprivilege.Grant{{Privilege: "USAGE", WithOption: true}}}),
			defaultPrivilegeChange("app_owner", "TABLES", "PUBLIC", pgdefaultprivilege.Change{GrantOptionsAdded: []string{"INSERT"}}),
			defaultPrivilegeChange("legacy_owner", "TABLES", "app_reader", pgdefaultprivilege.Change{Revoked: []pgdefaultprivilege.Grant{{Privilege: "DELETE"}}}),
			defaultPrivilegeChange("app_owner", "FUNCTIONS", "app_reader", pgdefaultprivilege.Change{GrantOptionsRevoked: []string{"EXECUTE"}}),
		},
		RolesRemoved: difftypes.RoleChanges{{Name: "legacy_owner"}},
	}
	nodes, err := postgres.New().GenerateMigrationAST(t.Context(), must.Must(builtin.New()), diff)
	c.Assert(err, qt.IsNil)
	sql, err := builtin.RenderSQL("postgres", nodes...)
	c.Assert(err, qt.IsNil)
	sql = legacyRenderedSQL(sql)
	acl := []string{
		"ALTER DEFAULT PRIVILEGES FOR ROLE legacy_owner IN SCHEMA app REVOKE DELETE ON TABLES FROM app_reader;",
		"ALTER DEFAULT PRIVILEGES FOR ROLE app_owner IN SCHEMA app REVOKE GRANT OPTION FOR EXECUTE ON FUNCTIONS FROM app_reader;",
		"ALTER DEFAULT PRIVILEGES FOR ROLE app_owner IN SCHEMA app GRANT SELECT ON TABLES TO app_reader;",
		"ALTER DEFAULT PRIVILEGES FOR ROLE app_owner IN SCHEMA app GRANT USAGE ON SEQUENCES TO app_reader WITH GRANT OPTION;",
		"ALTER DEFAULT PRIVILEGES FOR ROLE app_owner IN SCHEMA app GRANT INSERT ON TABLES TO PUBLIC WITH GRANT OPTION;",
	}
	c.Assert(strings.Split(strings.TrimSpace(sql), "\n"), qt.ContentEquals, append(acl,
		"CREATE SCHEMA IF NOT EXISTS app;", "-- WARNING: Ensure no other objects depend on this role", "DROP ROLE IF EXISTS legacy_owner;",
	))
	for _, statement := range acl {
		c.Assert(strings.Index(sql, "CREATE SCHEMA") < strings.Index(sql, statement), qt.IsTrue)
	}
	c.Assert(strings.Index(sql, acl[0]) < strings.Index(sql, "DROP ROLE"), qt.IsTrue)
}

func TestPlanner_GenerateMigrationAST_DefaultPrivilegeRemovalIsNeverADrop(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{FeatureChanges: []schemaext.ChangeRecord{
		defaultPrivilegeChange("app_owner", "TABLES", "app_reader", pgdefaultprivilege.Change{Revoked: []pgdefaultprivilege.Grant{{Privilege: "SELECT"}}}),
	}}
	nodes, err := postgres.New().GenerateMigrationAST(t.Context(), must.Must(builtin.New()), diff)
	c.Assert(err, qt.IsNil)
	sql, err := builtin.RenderSQL("postgres", nodes...)
	c.Assert(err, qt.IsNil)
	c.Assert(strings.TrimSpace(sql), qt.Equals, `ALTER DEFAULT PRIVILEGES FOR ROLE "app_owner" IN SCHEMA "app" REVOKE SELECT ON TABLES FROM "app_reader";`)
}
