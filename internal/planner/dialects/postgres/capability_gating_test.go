package postgres_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/planner/dialects/postgres"
	"ptah.run/migration/schemadiff/difftypes"
)

func TestPlanner_CapabilityGatesRoleManagement(t *testing.T) {
	c := qt.New(t)

	diff := &difftypes.SchemaDiff{
		RolesAdded: difftypes.RoleChanges{{Name: "app_role"}},
		RolesModified: []difftypes.RoleDiff{
			{RoleName: "existing_role", Changes: map[string]string{"login": "false -> true"}},
		},
		RolesRemoved: difftypes.RoleChanges{{Name: "old_role"}},
		GrantsRemoved: []difftypes.GrantRef{
			{Role: "app_role", Privilege: "DELETE", ObjectType: "TABLE", ObjectName: "users"},
		},
		GrantOptionsRevoked: []difftypes.GrantRef{
			{Role: "app_role", Privilege: "UPDATE", ObjectType: "TABLE", ObjectName: "users"},
		},
		GrantOptionsAdded: []difftypes.GrantRef{
			{Role: "app_role", Privilege: "REFERENCES", ObjectType: "TABLE", ObjectName: "users"},
		},
		GrantsAdded: []difftypes.GrantRef{
			{Role: "app_role", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "users"},
		},
	}
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: "users", StructName: "User"}},
		Roles: []schemamodel.Role{
			{Name: "app_role", Inherit: true},
			{Name: "existing_role", Login: true, Inherit: true},
		},
	}

	nodes, err := postgres.NewForDialect(platform.Spanner, capability.SpannerPostgres()).GenerateMigrationAST(
		context.Background(), must.Must(builtin.New()),
		withDeclaredObjects(diff, desired),
	)
	c.Assert(err, qt.IsNil)
	sql, err := builtin.RenderSQLWithCapabilities(platform.Spanner, capability.SpannerPostgres(), nodes...)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Not(qt.Contains), "CREATE ROLE")
	c.Assert(sql, qt.Not(qt.Contains), "ALTER ROLE")
	c.Assert(sql, qt.Not(qt.Contains), "DROP ROLE")
	c.Assert(sql, qt.Not(qt.Contains), "GRANT ")
	c.Assert(sql, qt.Not(qt.Contains), "REVOKE ")
}
