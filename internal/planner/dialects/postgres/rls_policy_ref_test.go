package postgres_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/planner/dialects/postgres"
	"ptah.run/migration/schemadiff"
	"ptah.run/migration/schemadiff/difftypes"
)

// sharedNamePolicy is one of two policies that share a name across two tables,
// which PostgreSQL permits: a policy name is scoped to its table, so
// tenant_isolation can exist on alpha_orders and zeta_orders at once. Each
// entry below carries the one it renders (stokaro/ptah#2315), which is what
// keeps two policies of one name apart without a lookup.
func sharedNamePolicy(table, using string) schemamodel.RLSPolicy {
	return schemamodel.RLSPolicy{
		Name: "tenant_isolation", Table: table,
		PolicyFor: "ALL", ToRoles: "PUBLIC", UsingExpression: using,
	}
}

func TestPlanner_RLSPolicyRefs_CreatesThePolicyOnTheNamedTable(t *testing.T) {
	tests := []struct {
		name      string
		added     []difftypes.RLSPolicyRef
		wantTable []string
		wantUsing []string
	}{
		{
			name: "only the second table is missing its policy",
			added: []difftypes.RLSPolicyRef{{
				PolicyName: "tenant_isolation", TableName: "zeta_orders",
				Desired: sharedNamePolicy("zeta_orders", "tenant_id = 2"),
			}},
			wantTable: []string{"zeta_orders"},
			wantUsing: []string{"tenant_id = 2"},
		},
		{
			name: "only the first table is missing its policy",
			added: []difftypes.RLSPolicyRef{{
				PolicyName: "tenant_isolation", TableName: "alpha_orders",
				Desired: sharedNamePolicy("alpha_orders", "tenant_id = 1"),
			}},
			wantTable: []string{"alpha_orders"},
			wantUsing: []string{"tenant_id = 1"},
		},
		{
			name: "both tables are missing their policy",
			added: []difftypes.RLSPolicyRef{
				{
					PolicyName: "tenant_isolation", TableName: "alpha_orders",
					Desired: sharedNamePolicy("alpha_orders", "tenant_id = 1"),
				},
				{
					PolicyName: "tenant_isolation", TableName: "zeta_orders",
					Desired: sharedNamePolicy("zeta_orders", "tenant_id = 2"),
				},
			},
			wantTable: []string{"alpha_orders", "zeta_orders"},
			wantUsing: []string{"tenant_id = 1", "tenant_id = 2"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{RLSPoliciesAdded: test.added}

			nodes, err := postgres.New().GenerateMigrationAST(diff)
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
		diff *difftypes.SchemaDiff
		// wantErr is the whole discrimination between these rows: the category
		// and the position have to reach the operator, or an unresolvable
		// reference is reported as some other one.
		wantErr string
	}{
		{
			name: "an addition carrying no declaration is refused",
			diff: &difftypes.SchemaDiff{
				RLSPoliciesAdded: []difftypes.RLSPolicyRef{
					{PolicyName: "tenant_isolation", TableName: "omega_orders"},
				},
			},
			wantErr: `.*added RLS policy tenant_isolation on table omega_orders carries no declaration to render it from`,
		},
		{
			name: "a modification carrying no declaration is refused",
			diff: &difftypes.SchemaDiff{
				RLSPoliciesModified: []difftypes.RLSPolicyDiff{{
					PolicyName: "tenant_isolation",
					TableName:  "omega_orders",
					Changes:    map[string]string{"using_expression": "a -> b"},
				}},
			},
			wantErr: `.*modified RLS policy tenant_isolation on table omega_orders carries no declaration to render it from`,
		},
		{
			name: "a reference with no owning table is refused",
			diff: &difftypes.SchemaDiff{
				RLSPoliciesAdded: []difftypes.RLSPolicyRef{{PolicyName: "tenant_isolation"}},
			},
			wantErr: `.*added RLS policy reference at position 0 requires a policy name and owning table`,
		},
		{
			name: "a removal with no owning table is refused",
			diff: &difftypes.SchemaDiff{
				RLSPoliciesRemoved: []difftypes.RLSPolicyRef{{PolicyName: "tenant_isolation"}},
			},
			wantErr: `.*removed RLS policy reference at position 0 requires a policy name and owning table`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			nodes, err := postgres.New().GenerateMigrationAST(test.diff)

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
	diff := &difftypes.SchemaDiff{
		RLSPoliciesRemoved: []difftypes.RLSPolicyRef{
			{PolicyName: "tenant_isolation", TableName: "omega_orders"},
		},
	}

	nodes, err := postgres.New().GenerateMigrationAST(diff)

	c.Assert(err, qt.IsNil)
	c.Assert(nodes, qt.HasLen, 2)
	drop, ok := nodes[0].(*ast.DropPolicyNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(drop.Name, qt.Equals, "tenant_isolation")
	c.Assert(drop.Table, qt.Equals, "omega_orders")
}

// TestCompare_AnRLSPolicyResolvesTheDefaultSchemaSpelling is the pair to the
// refusal above, measured where it now happens.
//
// `orders` and `public.orders` are one table on PostgreSQL. The planner used to
// reconcile the two spellings by looking the policy up; the comparison does it
// now, and the entry carries the declaration it resolved to
// (stokaro/ptah#2315). Driving the comparison rather than hand-building the
// diff is the point: a hand-built one bypasses the resolution, which is exactly
// what the planner no longer performs.
//
// The declared spelling is what reaches the DDL either way -- only the matching
// is normalized -- and that is the half this still asserts through the planner.
func TestCompare_AnRLSPolicyResolvesTheDefaultSchemaSpelling(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		reported string
	}{
		{name: "declared bare, reported qualified", declared: "orders", reported: "public.orders"},
		{name: "declared qualified, reported bare", declared: "public.orders", reported: "orders"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := &schemamodel.Database{
				Tables: []schemamodel.Table{{Name: "orders", StructName: "Order"}},
				RLSPolicies: []schemamodel.RLSPolicy{{
					Name:            "tenant_isolation",
					Table:           test.declared,
					PolicyFor:       "ALL",
					ToRoles:         "PUBLIC",
					UsingExpression: "tenant_id = 1",
				}},
			}
			// The table is on both sides, so the only difference is the policy
			// and the plan is the one statement this test is about.
			database := &catalog.Database{
				Tables: []catalog.Table{{Schema: "public", Name: "orders"}},
				RLSPolicies: []catalog.RLSPolicy{{
					Name: "tenant_isolation", Table: test.reported,
					PolicyFor: "ALL", ToRoles: "PUBLIC", UsingExpression: "tenant_id = 99",
				}},
			}

			diff := schemadiff.CompareWithDialect(desired, database, "postgres")

			c.Assert(diff.RLSPoliciesAdded, qt.HasLen, 0,
				qt.Commentf("the two spellings are one policy, not one to create and one to drop"))
			c.Assert(diff.RLSPoliciesRemoved, qt.HasLen, 0)
			c.Assert(diff.RLSPoliciesModified, qt.HasLen, 1)

			nodes, err := postgres.New().GenerateMigrationAST(diff)

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
