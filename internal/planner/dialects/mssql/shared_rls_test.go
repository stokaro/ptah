package mssql_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/planner/dialects/mssql"
	"ptah.run/migration/schemadiff/difftypes"
)

// SQL Server's row-level security is a security policy, which its owner plans
// from feature changes. A shared policy or switch change reaches this planner
// only from a diff built by hand, and is named and skipped rather than turned
// into a security policy, a DROP SECURITY POLICY, or a drop-and-create pair
// that leaves the table unprotected in between (stokaro/ptah#2211).
func TestGenerateMigrationAST_SkipsSharedRowSecurity(t *testing.T) {
	c := qt.New(t)
	policy := schemamodel.RLSPolicy{Name: "tenant_filter", Table: "docs", UsingExpression: "dbo.fn_pred(tenant)"}
	diff := &difftypes.SchemaDiff{
		RLSPoliciesAdded:    []difftypes.RLSPolicyRef{{PolicyName: "tenant_filter", TableName: "docs", Desired: policy}},
		RLSPoliciesModified: []difftypes.RLSPolicyDiff{{PolicyName: "tenant_read", TableName: "docs", Desired: policy}},
		RLSPoliciesRemoved:  []difftypes.RLSPolicyRef{{PolicyName: "tenant_old", TableName: "docs"}},
	}

	nodes, err := mssql.New().GenerateMigrationAST(context.Background(), must.Must(builtin.New()), diff)
	c.Assert(err, qt.IsNil)
	sql, err := builtin.RenderSQL("sqlserver", nodes...)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Equals, "-- SQLSERVER: RLS policies \"tenant_filter\" is not generated for this target; skipped.\n"+
		"-- SQLSERVER: RLS policies \"tenant_read\" is not generated for this target; skipped.\n"+
		"-- SQLSERVER: DROP POLICY \"tenant_old\" is not generated for this target; skipped.\n")
}
