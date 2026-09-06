package postgres_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/renderer"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/planner/dialects/postgres"
	"ptah.run/migration/schemadiff/difftypes"
)

func TestPlanner_CapabilityGatesRLSAndRoleManagement(t *testing.T) {
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
		RLSPoliciesAdded: []difftypes.RLSPolicyRef{
			{
				PolicyName: "tenant_policy", TableName: "users",
				Desired: schemamodel.RLSPolicy{
					Name: "tenant_policy", Table: "users",
					PolicyFor: "ALL", ToRoles: "app_role", UsingExpression: "true",
				},
			},
		},
		RLSPoliciesRemoved: []difftypes.RLSPolicyRef{
			{PolicyName: "old_policy", TableName: "users"},
		},
	}
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: "users", StructName: "User"}},
		Roles: []schemamodel.Role{
			{Name: "app_role", Inherit: true},
			{Name: "existing_role", Login: true, Inherit: true},
		},
		RLSPolicies: []schemamodel.RLSPolicy{{
			Name:            "tenant_policy",
			Table:           "users",
			PolicyFor:       "SELECT",
			ToRoles:         "app_role",
			UsingExpression: "tenant_id = current_setting('app.tenant_id')::uuid",
		}},
	}

	nodes, err := postgres.NewForDialect(platform.Spanner, capability.SpannerPostgres()).GenerateMigrationAST(withDeclaredObjects(diff, desired))
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
