package mssql_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/planner/dialects/mssql"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

// planModifiedPolicy renders the plan for a policy the diff reports as changed.
func planModifiedPolicy(c *qt.C, policyFor, withCheck string) string {
	c.Helper()

	// The policy travels WITH the change (stokaro/ptah#2315), so the schema
	// handed to the planner is empty.
	diff := &difftypes.SchemaDiff{
		RLSPoliciesModified: []difftypes.RLSPolicyDiff{{
			PolicyName: "tenant_filter", TableName: "docs",
			Changes: map[string]string{"for": "ALL -> " + policyFor},
			Desired: schemamodel.RLSPolicy{
				Name: "tenant_filter", Table: "docs",
				PolicyFor:           policyFor,
				UsingExpression:     "dbo.fn_pred(tenant)",
				WithCheckExpression: withCheck,
			},
		}},
	}

	nodes, err := mssql.New().GenerateMigrationAST(diff)
	c.Assert(err, qt.IsNil)
	sql, err := renderer.RenderSQL("sqlserver", nodes...)
	c.Assert(err, qt.IsNil)
	return sql
}

// A replacement whose create half is refused contributes no DROP -- stokaro/ptah#2211.
//
// A modified policy is planned as a drop followed by a create. When the create
// half is refused, the drop half is the whole plan, and applying it takes the
// table's row-level security away and puts nothing back -- a worse answer than
// the difference the pair was planned to close.
//
// The refusal is still emitted, so the plan names what Ptah cannot express. A
// planner that simply skipped the policy would hand back a plan that silently
// does nothing about a declaration it refused.
func TestPlanRLS_ARefusedReplacementDoesNotDropThePolicy(t *testing.T) {
	tests := []struct {
		name      string
		policyFor string
		withCheck string
		fragment  string
	}{
		{name: "a filter-only policy naming a read", policyFor: "SELECT", fragment: "no form for"},
		{name: "an operation with no block predicate", policyFor: "INSERT", fragment: "WITH CHECK"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			sql := planModifiedPolicy(c, test.policyFor, test.withCheck)

			c.Assert(sql, qt.Not(qt.Contains), "DROP SECURITY POLICY")
			c.Assert(sql, qt.Not(qt.Contains), "CREATE SECURITY POLICY")
			c.Assert(sql, qt.Contains, test.fragment)
		})
	}
}

// An expressible replacement is still planned as the drop and create pair.
//
// This is the control the refusal above is measured against: without it, a
// planner that dropped every replacement would pass the test above.
func TestPlanRLS_AnExpressibleReplacementIsStillPlannedAsAPair(t *testing.T) {
	tests := []struct {
		name      string
		policyFor string
		withCheck string
	}{
		{name: "ALL on a filter-only policy", policyFor: "ALL"},
		{name: "an operation riding a block predicate", policyFor: "UPDATE", withCheck: "dbo.fn_write(tenant)"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			sql := planModifiedPolicy(c, test.policyFor, test.withCheck)

			c.Assert(sql, qt.Contains, "DROP SECURITY POLICY")
			c.Assert(sql, qt.Contains, "CREATE SECURITY POLICY")
		})
	}
}
