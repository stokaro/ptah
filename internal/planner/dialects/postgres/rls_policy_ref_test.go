package postgres_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/planner/dialects/postgres"
	"go.5x5.cz/ptah/migration/schemadiff/types"
)

// generatedSharedPolicyName is a desired schema where one policy name is
// carried by two tables, which PostgreSQL permits: a policy name is scoped to
// its table, so tenant_isolation can exist on alpha_orders and zeta_orders at
// once. The planner therefore cannot resolve an addition by name alone.
func generatedSharedPolicyName() *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{
			{Name: "alpha_orders", StructName: "AlphaOrder"},
			{Name: "zeta_orders", StructName: "ZetaOrder"},
		},
		RLSPolicies: []goschema.RLSPolicy{
			{Name: "tenant_isolation", Table: "alpha_orders", PolicyFor: "ALL", ToRoles: "PUBLIC", UsingExpression: "tenant_id = 1"},
			{Name: "tenant_isolation", Table: "zeta_orders", PolicyFor: "ALL", ToRoles: "PUBLIC", UsingExpression: "tenant_id = 2"},
		},
	}
}

func TestPlanner_RLSPolicyRefs_CreatesThePolicyOnTheNamedTable(t *testing.T) {
	tests := []struct {
		name      string
		added     []types.RLSPolicyRef
		wantTable []string
		wantUsing []string
	}{
		{
			name:      "only the second table is missing its policy",
			added:     []types.RLSPolicyRef{{PolicyName: "tenant_isolation", TableName: "zeta_orders"}},
			wantTable: []string{"zeta_orders"},
			wantUsing: []string{"tenant_id = 2"},
		},
		{
			name:      "only the first table is missing its policy",
			added:     []types.RLSPolicyRef{{PolicyName: "tenant_isolation", TableName: "alpha_orders"}},
			wantTable: []string{"alpha_orders"},
			wantUsing: []string{"tenant_id = 1"},
		},
		{
			name: "both tables are missing their policy",
			added: []types.RLSPolicyRef{
				{PolicyName: "tenant_isolation", TableName: "alpha_orders"},
				{PolicyName: "tenant_isolation", TableName: "zeta_orders"},
			},
			wantTable: []string{"alpha_orders", "zeta_orders"},
			wantUsing: []string{"tenant_id = 1", "tenant_id = 2"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &types.SchemaDiff{RLSPoliciesAdded: test.added}

			nodes, err := postgres.New().GenerateMigrationASTChecked(diff, generatedSharedPolicyName())
			c.Assert(err, qt.IsNil)

			var tables []string
			var usings []string
			for _, node := range nodes {
				policy, ok := node.(*ast.CreatePolicyNode)
				c.Assert(ok, qt.IsTrue)
				c.Assert(policy.Name, qt.Equals, "tenant_isolation")
				tables = append(tables, policy.Table)
				usings = append(usings, policy.UsingExpression)
			}
			c.Assert(tables, qt.DeepEquals, test.wantTable)
			c.Assert(usings, qt.DeepEquals, test.wantUsing)
		})
	}
}

// TestPlanner_RLSPolicyRefs_SkipsAPolicyTheDesiredSchemaDoesNotHold is the
// control for the lookup becoming exact: a reference the desired schema cannot
// resolve must produce nothing rather than fall back to a same-named policy on
// some other table, which is what the name-only lookup did on a down
// migration.
func TestPlanner_RLSPolicyRefs_SkipsAPolicyTheDesiredSchemaDoesNotHold(t *testing.T) {
	c := qt.New(t)
	diff := &types.SchemaDiff{
		RLSPoliciesAdded: []types.RLSPolicyRef{
			{PolicyName: "tenant_isolation", TableName: "omega_orders"},
		},
	}

	nodes, err := postgres.New().GenerateMigrationASTChecked(diff, generatedSharedPolicyName())
	c.Assert(err, qt.IsNil)
	c.Assert(nodes, qt.HasLen, 0)
}
