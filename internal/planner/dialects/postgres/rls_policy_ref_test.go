package postgres_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/ptaherr"
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

// TestPlanner_RLSPolicyRefs_RefusesAPolicyTheDesiredSchemaDoesNotHold is the
// control for the lookup becoming exact: a reference the desired schema cannot
// resolve must not fall back to a same-named policy on some other table, which
// is what the name-only lookup did on a down migration.
//
// It must also not fall back to SILENCE, which is what this test asserted until
// stokaro/ptah#1311 was reviewed. The checked planner returned a successful
// plan with the CREATE POLICY simply absent from it, so a stale or hand-built
// diff produced valid-looking SQL that left the database without an
// access-control protection the migration claimed to add -- while the public
// planning contract promises an invalid schema diff is rejected with
// ptaherr.ErrInvalidSchemaDiff. The refusal is the assertion now.
//
// An addition and a modification both build their CREATE POLICY from a
// declaration and so need one; a reference that names no table at all cannot be
// resolved in either direction. The one category that is planned rather than
// refused is a removal naming its table, which
// TestPlanner_RLSPolicyRefs_PlansARemovalThatNeedsNoDeclaration measures.
func TestPlanner_RLSPolicyRefs_RefusesAPolicyTheDesiredSchemaDoesNotHold(t *testing.T) {
	tests := []struct {
		name string
		diff *types.SchemaDiff
		// wantErr is the whole discrimination between these rows: the category
		// and the position have to reach the operator, or an unresolvable
		// reference is reported as some other one.
		wantErr string
	}{
		{
			name: "an addition naming an undeclared table is refused",
			diff: &types.SchemaDiff{
				RLSPoliciesAdded: []types.RLSPolicyRef{
					{PolicyName: "tenant_isolation", TableName: "omega_orders"},
				},
			},
			wantErr: `.*added RLS policy tenant_isolation on table omega_orders at position 0 is missing from the target schema`,
		},
		{
			name: "a modification naming an undeclared policy is refused",
			diff: &types.SchemaDiff{
				RLSPoliciesModified: []types.RLSPolicyDiff{{
					PolicyName: "tenant_isolation",
					TableName:  "omega_orders",
					Changes:    map[string]string{"using_expression": "a -> b"},
				}},
			},
			wantErr: `.*modified RLS policy tenant_isolation on table omega_orders at position 0 is missing from the target schema`,
		},
		{
			name: "a reference with no owning table is refused",
			diff: &types.SchemaDiff{
				RLSPoliciesAdded: []types.RLSPolicyRef{{PolicyName: "tenant_isolation"}},
			},
			wantErr: `.*added RLS policy reference at position 0 requires a policy name and owning table`,
		},
		{
			name: "a removal with no owning table is refused",
			diff: &types.SchemaDiff{
				RLSPoliciesRemoved: []types.RLSPolicyRef{{PolicyName: "tenant_isolation"}},
			},
			wantErr: `.*removed RLS policy reference at position 0 requires a policy name and owning table`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			nodes, err := postgres.New().GenerateMigrationASTChecked(test.diff, generatedSharedPolicyName())

			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(nodes, qt.IsNil)
		})
	}
}

// TestPlanner_RLSPolicyRefs_PlansARemovalThatNeedsNoDeclaration is the category
// the refusal above must not swallow.
//
// A removal renders `DROP POLICY name ON table` out of the reference itself, so
// a desired schema that no longer declares the policy is exactly the state a
// removal is planned from -- refusing it would make every dropped policy
// unplannable. Only its shape can be checked, and that is what is checked.
func TestPlanner_RLSPolicyRefs_PlansARemovalThatNeedsNoDeclaration(t *testing.T) {
	c := qt.New(t)
	diff := &types.SchemaDiff{
		RLSPoliciesRemoved: []types.RLSPolicyRef{
			{PolicyName: "tenant_isolation", TableName: "omega_orders"},
		},
	}

	nodes, err := postgres.New().GenerateMigrationASTChecked(diff, generatedSharedPolicyName())

	c.Assert(err, qt.IsNil)
	c.Assert(nodes, qt.HasLen, 2)
	drop, ok := nodes[0].(*ast.DropPolicyNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(drop.Name, qt.Equals, "tenant_isolation")
	c.Assert(drop.Table, qt.Equals, "omega_orders")
}

// TestPlanner_RLSPolicyRefs_ResolvesTheDefaultSchemaSpelling is the pair to the
// refusal: a reference the target schema DOES hold under the dialect's
// identifier rules must resolve, not be refused for spelling its table
// differently.
//
// `orders` and `public.orders` are one table on PostgreSQL, which is why the
// comparator normalizes them -- and why the planner has to as well, in both
// directions, or a diff the comparator produced would be rejected by the
// planner that consumes it.
func TestPlanner_RLSPolicyRefs_ResolvesTheDefaultSchemaSpelling(t *testing.T) {
	tests := []struct {
		name      string
		declared  string
		reference string
	}{
		{name: "declared bare, referenced qualified", declared: "orders", reference: "public.orders"},
		{name: "declared qualified, referenced bare", declared: "public.orders", reference: "orders"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			generated := &goschema.Database{
				Tables: []goschema.Table{{Name: "orders", StructName: "Order"}},
				RLSPolicies: []goschema.RLSPolicy{{
					Name:            "tenant_isolation",
					Table:           test.declared,
					PolicyFor:       "ALL",
					ToRoles:         "PUBLIC",
					UsingExpression: "tenant_id = 1",
				}},
			}
			diff := &types.SchemaDiff{
				RLSPoliciesAdded: []types.RLSPolicyRef{
					{PolicyName: "tenant_isolation", TableName: test.reference},
				},
			}

			nodes, err := postgres.New().GenerateMigrationASTChecked(diff, generated)

			c.Assert(err, qt.IsNil)
			c.Assert(nodes, qt.HasLen, 1)
			policy, ok := nodes[0].(*ast.CreatePolicyNode)
			c.Assert(ok, qt.IsTrue)
			// The declared spelling is what is rendered: only the matching is
			// normalized.
			c.Assert(policy.Table, qt.Equals, test.declared)
			c.Assert(policy.UsingExpression, qt.Equals, "tenant_id = 1")
		})
	}
}
