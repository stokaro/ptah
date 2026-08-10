package postgres_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/internal/planner/dialects/postgres"
	"go.5x5.cz/ptah/migration/schemadiff/types"
)

func TestPlanner_CapabilityGatesRLSAndRoleManagement(t *testing.T) {
	c := qt.New(t)

	diff := &types.SchemaDiff{
		RolesAdded: []string{"app_role"},
		RolesModified: []types.RoleDiff{
			{RoleName: "existing_role", Changes: map[string]string{"login": "false -> true"}},
		},
		RolesRemoved: []string{"old_role"},
		GrantsRemoved: []types.GrantRef{
			{Role: "app_role", Privilege: "DELETE", ObjectType: "TABLE", ObjectName: "users"},
		},
		GrantOptionsRevoked: []types.GrantRef{
			{Role: "app_role", Privilege: "UPDATE", ObjectType: "TABLE", ObjectName: "users"},
		},
		GrantOptionsAdded: []types.GrantRef{
			{Role: "app_role", Privilege: "REFERENCES", ObjectType: "TABLE", ObjectName: "users"},
		},
		GrantsAdded: []types.GrantRef{
			{Role: "app_role", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "users"},
		},
		RLSPoliciesAdded: []types.RLSPolicyRef{
			{PolicyName: "tenant_policy", TableName: "users"},
		},
		RLSPoliciesRemoved: []types.RLSPolicyRef{
			{PolicyName: "old_policy", TableName: "users"},
		},
	}
	generated := &goschema.Database{
		Tables: []goschema.Table{{Name: "users", StructName: "User"}},
		Roles: []goschema.Role{
			{Name: "app_role", Inherit: true},
			{Name: "existing_role", Login: true, Inherit: true},
		},
		RLSPolicies: []goschema.RLSPolicy{{
			Name:            "tenant_policy",
			Table:           "users",
			PolicyFor:       "SELECT",
			ToRoles:         "app_role",
			UsingExpression: "tenant_id = current_setting('app.tenant_id')::uuid",
		}},
	}

	nodes, err := postgres.NewForDialect(platform.Spanner, capability.SpannerPostgres()).GenerateMigrationASTChecked(diff, generated)
	c.Assert(err, qt.IsNil)
	sql, err := renderer.RenderSQLWithCapabilities(platform.Spanner, capability.SpannerPostgres(), nodes...)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Not(qt.Contains), "CREATE ROLE")
	c.Assert(sql, qt.Not(qt.Contains), "ALTER ROLE")
	c.Assert(sql, qt.Not(qt.Contains), "DROP ROLE")
	c.Assert(sql, qt.Not(qt.Contains), "GRANT ")
	c.Assert(sql, qt.Not(qt.Contains), "REVOKE ")
	c.Assert(sql, qt.Not(qt.Contains), "ROW LEVEL SECURITY")
	c.Assert(sql, qt.Not(qt.Contains), "CREATE POLICY")
	c.Assert(sql, qt.Not(qt.Contains), "DROP POLICY")
}
