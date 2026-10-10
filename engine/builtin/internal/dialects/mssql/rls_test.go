package mssql_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/engine/builtin"
)

// The shared policy and switch nodes describe PostgreSQL's model. SQL
// Server's row-level security is a security policy its owner plans, so each
// shared node that reaches this renderer from a diff built by hand is named
// and skipped rather than rendered into a policy nobody declared.
func TestSQLServerRenderer_SkipsTheSharedRowSecurityNodes(t *testing.T) {
	tests := []struct {
		name string
		node ast.Node
		want string
	}{
		{name: "a policy", node: ast.NewCreatePolicy("dbo.p_tenant", "t_rls").SetUsingExpression("dbo.fn_tenant(tenant)"),
			want: "-- SQLSERVER: RLS policies \"dbo.p_tenant\" is not generated for this target; skipped.\n"},
		{name: "a policy drop", node: ast.NewDropPolicy("dbo.p_tenant", "t_rls"),
			want: "-- SQLSERVER: DROP POLICY \"dbo.p_tenant\" is not generated for this target; skipped.\n"},
		{name: "an enablement", node: ast.NewAlterTableEnableRLS("t_rls"),
			want: "-- SQLSERVER: row-level security \"t_rls\" is not generated for this target; skipped.\n"},
		{name: "a disablement", node: ast.NewAlterTableDisableRLS("t_rls"),
			want: "-- SQLSERVER: row-level security \"t_rls\" is not generated for this target; skipped.\n"},
		{name: "a forced switch", node: ast.NewAlterTableForceRLS("t_rls"),
			want: "-- SQLSERVER: row-level security \"t_rls\" is not generated for this target; skipped.\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statement, err := builtin.RenderSQL(platform.SQLServer, test.node)

			c.Assert(err, qt.IsNil)
			c.Assert(statement, qt.Equals, test.want)
		})
	}
}
