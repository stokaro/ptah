package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/migration/schemadiff/internal/compare"
	difftypes "go.5x5.cz/ptah/migration/schemadiff/types"
)

// generatedSharedPolicyName is the desired schema used by the addition and
// removal tables below: one policy name carried by two different tables, which
// PostgreSQL permits because a policy name is scoped to its table. Measured on
// PostgreSQL 17.10, CREATE POLICY tenant_isolation succeeds on both
// public.alpha_orders and public.zeta_orders and leaves two rows in pg_policy.
func generatedSharedPolicyName() *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{
			{Name: "alpha_orders", StructName: "AlphaOrder"},
			{Name: "zeta_orders", StructName: "ZetaOrder"},
		},
		RLSPolicies: []goschema.RLSPolicy{
			{Name: "tenant_isolation", Table: "alpha_orders", PolicyFor: "ALL", ToRoles: "PUBLIC", UsingExpression: "tenant_id = 1"},
			{Name: "tenant_isolation", Table: "zeta_orders", PolicyFor: "ALL", ToRoles: "PUBLIC", UsingExpression: "tenant_id = 1"},
		},
	}
}

func TestRLSPoliciesWithSemantics_TableQualifiedAdditions(t *testing.T) {
	tests := []struct {
		name     string
		database *types.DBSchema
		want     []difftypes.RLSPolicyRef
	}{
		{
			name:     "both policies missing",
			database: &types.DBSchema{},
			want: []difftypes.RLSPolicyRef{
				{PolicyName: "tenant_isolation", TableName: "alpha_orders"},
				{PolicyName: "tenant_isolation", TableName: "zeta_orders"},
			},
		},
		{
			name: "one table has the policy",
			database: &types.DBSchema{
				RLSPolicies: []types.DBRLSPolicy{
					{Name: "tenant_isolation", Table: "public.alpha_orders", PolicyFor: "ALL", ToRoles: "PUBLIC", UsingExpression: "tenant_id = 1"},
				},
			},
			want: []difftypes.RLSPolicyRef{
				{PolicyName: "tenant_isolation", TableName: "zeta_orders"},
			},
		},
		{
			name: "both tables have the policy",
			database: &types.DBSchema{
				RLSPolicies: []types.DBRLSPolicy{
					{Name: "tenant_isolation", Table: "public.alpha_orders", PolicyFor: "ALL", ToRoles: "PUBLIC", UsingExpression: "tenant_id = 1"},
					{Name: "tenant_isolation", Table: "public.zeta_orders", PolicyFor: "ALL", ToRoles: "PUBLIC", UsingExpression: "tenant_id = 1"},
				},
			},
			want: nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{}

			compare.RLSPoliciesWithSemantics(
				generatedSharedPolicyName(),
				test.database,
				diff,
				identifier.ForDialect("postgres"),
				compare.Coverage{},
			)

			c.Assert(diff.RLSPoliciesAdded, qt.DeepEquals, test.want)
			c.Assert(diff.RLSPoliciesRemoved, qt.HasLen, 0)
			c.Assert(diff.RLSPoliciesModified, qt.HasLen, 0)
		})
	}
}

func TestRLSPoliciesWithSemantics_TableQualifiedRemovals(t *testing.T) {
	generated := &goschema.Database{
		Tables: []goschema.Table{
			{Name: "alpha_orders", StructName: "AlphaOrder"},
			{Name: "zeta_orders", StructName: "ZetaOrder"},
		},
		RLSPolicies: []goschema.RLSPolicy{
			{Name: "tenant_isolation", Table: "alpha_orders", PolicyFor: "ALL", ToRoles: "PUBLIC", UsingExpression: "tenant_id = 1"},
		},
	}

	tests := []struct {
		name     string
		database *types.DBSchema
		want     []difftypes.RLSPolicyRef
	}{
		{
			name: "same name on an undeclared table is removed",
			database: &types.DBSchema{
				RLSPolicies: []types.DBRLSPolicy{
					{Name: "tenant_isolation", Table: "public.alpha_orders", PolicyFor: "ALL", ToRoles: "PUBLIC", UsingExpression: "tenant_id = 1"},
					{Name: "tenant_isolation", Table: "public.zeta_orders", PolicyFor: "ALL", ToRoles: "PUBLIC", UsingExpression: "tenant_id = 1"},
				},
			},
			want: []difftypes.RLSPolicyRef{
				{PolicyName: "tenant_isolation", TableName: "public.zeta_orders"},
			},
		},
		{
			name: "distinct name on an undeclared table is removed",
			database: &types.DBSchema{
				RLSPolicies: []types.DBRLSPolicy{
					{Name: "tenant_isolation", Table: "public.alpha_orders", PolicyFor: "ALL", ToRoles: "PUBLIC", UsingExpression: "tenant_id = 1"},
					{Name: "zeta_only", Table: "public.zeta_orders", PolicyFor: "ALL", ToRoles: "PUBLIC", UsingExpression: "tenant_id = 1"},
				},
			},
			want: []difftypes.RLSPolicyRef{
				{PolicyName: "zeta_only", TableName: "public.zeta_orders"},
			},
		},
		{
			name: "the declared policy alone is kept",
			database: &types.DBSchema{
				RLSPolicies: []types.DBRLSPolicy{
					{Name: "tenant_isolation", Table: "public.alpha_orders", PolicyFor: "ALL", ToRoles: "PUBLIC", UsingExpression: "tenant_id = 1"},
				},
			},
			want: nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{}

			compare.RLSPoliciesWithSemantics(generated, test.database, diff, identifier.ForDialect("postgres"), compare.Coverage{})

			c.Assert(diff.RLSPoliciesRemoved, qt.DeepEquals, test.want)
			c.Assert(diff.RLSPoliciesAdded, qt.HasLen, 0)
			c.Assert(diff.RLSPoliciesModified, qt.HasLen, 0)
		})
	}
}

// TestRLSPoliciesWithSemantics_ModificationMatchesTheSameTable is the row the
// name-only key got backwards rather than merely incomplete: the desired
// policy on alpha_orders was compared against the database's policy on
// zeta_orders, the two agreed, and a stale USING expression on alpha_orders
// was reported as no change at all.
func TestRLSPoliciesWithSemantics_ModificationMatchesTheSameTable(t *testing.T) {
	c := qt.New(t)

	generated := &goschema.Database{
		Tables: []goschema.Table{
			{Name: "alpha_orders", StructName: "AlphaOrder"},
			{Name: "zeta_orders", StructName: "ZetaOrder"},
		},
		RLSPolicies: []goschema.RLSPolicy{
			{Name: "tenant_isolation", Table: "alpha_orders", PolicyFor: "ALL", ToRoles: "PUBLIC", UsingExpression: "tenant_id = 1"},
		},
	}
	database := &types.DBSchema{
		RLSPolicies: []types.DBRLSPolicy{
			{Name: "tenant_isolation", Table: "public.alpha_orders", PolicyFor: "ALL", ToRoles: "PUBLIC", UsingExpression: "tenant_id = 99"},
			{Name: "tenant_isolation", Table: "public.zeta_orders", PolicyFor: "ALL", ToRoles: "PUBLIC", UsingExpression: "tenant_id = 1"},
		},
	}
	diff := &difftypes.SchemaDiff{}

	compare.RLSPoliciesWithSemantics(generated, database, diff, identifier.ForDialect("postgres"), compare.Coverage{})

	c.Assert(diff.RLSPoliciesModified, qt.HasLen, 1)
	c.Assert(diff.RLSPoliciesModified[0].PolicyName, qt.Equals, "tenant_isolation")
	c.Assert(diff.RLSPoliciesModified[0].TableName, qt.Equals, "alpha_orders")
	c.Assert(diff.RLSPoliciesModified[0].Changes["using_expression"], qt.Equals, "tenant_id = 99 -> tenant_id = 1")
	c.Assert(diff.RLSPoliciesRemoved, qt.DeepEquals, []difftypes.RLSPolicyRef{
		{PolicyName: "tenant_isolation", TableName: "public.zeta_orders"},
	})
	c.Assert(diff.RLSPoliciesAdded, qt.HasLen, 0)
}

// TestRLSPoliciesWithSemantics_ImplicitSchemaStillMatches is the control for
// the normalization itself: the database reports the table without a schema
// where the engine treats it as implicit, and that must still match the
// desired schema's qualified spelling rather than reading as a removal plus an
// addition.
func TestRLSPoliciesWithSemantics_ImplicitSchemaStillMatches(t *testing.T) {
	c := qt.New(t)

	generated := &goschema.Database{
		Tables: []goschema.Table{{Name: "alpha_orders", Schema: "public", StructName: "AlphaOrder"}},
		RLSPolicies: []goschema.RLSPolicy{
			{Name: "tenant_isolation", Table: "public.alpha_orders", PolicyFor: "ALL", ToRoles: "PUBLIC", UsingExpression: "tenant_id = 1"},
		},
	}
	database := &types.DBSchema{
		RLSPolicies: []types.DBRLSPolicy{
			{Name: "tenant_isolation", Table: "alpha_orders", PolicyFor: "ALL", ToRoles: "PUBLIC", UsingExpression: "tenant_id = 1"},
		},
	}
	diff := &difftypes.SchemaDiff{}

	compare.RLSPoliciesWithSemantics(generated, database, diff, identifier.ForDialect("postgres"), compare.Coverage{})

	c.Assert(diff.RLSPoliciesAdded, qt.HasLen, 0)
	c.Assert(diff.RLSPoliciesRemoved, qt.HasLen, 0)
	c.Assert(diff.RLSPoliciesModified, qt.HasLen, 0)
}

// TestRLSPolicies_DelegatesWithDialectlessSemantics pins the thin wrapper to
// the same behavior the package's other name-scoped comparisons expose, so a
// caller that has no dialect still gets table-scoped matching.
func TestRLSPolicies_DelegatesWithDialectlessSemantics(t *testing.T) {
	c := qt.New(t)

	database := &types.DBSchema{
		RLSPolicies: []types.DBRLSPolicy{
			{Name: "tenant_isolation", Table: "alpha_orders", PolicyFor: "ALL", ToRoles: "PUBLIC", UsingExpression: "tenant_id = 1"},
		},
	}
	diff := &difftypes.SchemaDiff{}

	compare.RLSPolicies(generatedSharedPolicyName(), database, diff, compare.Coverage{})

	c.Assert(diff.RLSPoliciesAdded, qt.DeepEquals, []difftypes.RLSPolicyRef{
		{PolicyName: "tenant_isolation", TableName: "zeta_orders"},
	})
	c.Assert(diff.RLSPoliciesRemoved, qt.HasLen, 0)
	c.Assert(diff.RLSPoliciesModified, qt.HasLen, 0)
}

// TestRLSPoliciesWithSemantics_OrdersRefsByTableFirst pins the sort key. The
// policy name alone stopped being a total order the moment two tables could
// share one, so the table leads and the name breaks ties within it. This
// fixture separates the two orders: sorting by the name alone would lead with
// `alpha_policy` on `zeta_orders`, while the shipped order groups the refs
// under the table that owns them.
//
// Order is what a caller diffs between runs -- the shadow report, the drift
// JSON, and the planned SQL all read these slices in sequence -- so an order
// nothing pins shows up later as churn in artifacts nothing changed in.
func TestRLSPoliciesWithSemantics_OrdersRefsByTableFirst(t *testing.T) {
	c := qt.New(t)

	generated := &goschema.Database{
		Tables: []goschema.Table{
			{Name: "alpha_orders", StructName: "AlphaOrder"},
			{Name: "zeta_orders", StructName: "ZetaOrder"},
		},
		RLSPolicies: []goschema.RLSPolicy{
			{Name: "zeta_policy", Table: "alpha_orders", PolicyFor: "ALL", ToRoles: "PUBLIC", UsingExpression: "tenant_id = 1"},
			{Name: "alpha_policy", Table: "zeta_orders", PolicyFor: "ALL", ToRoles: "PUBLIC", UsingExpression: "tenant_id = 2"},
			{Name: "alpha_policy", Table: "alpha_orders", PolicyFor: "ALL", ToRoles: "PUBLIC", UsingExpression: "tenant_id = 3"},
		},
	}
	database := &types.DBSchema{
		RLSPolicies: []types.DBRLSPolicy{
			{Name: "zeta_policy", Table: "public.zeta_orders", PolicyFor: "ALL", ToRoles: "PUBLIC", UsingExpression: "tenant_id = 4"},
			{Name: "alpha_extra", Table: "public.alpha_orders", PolicyFor: "ALL", ToRoles: "PUBLIC", UsingExpression: "tenant_id = 5"},
		},
	}
	diff := &difftypes.SchemaDiff{}

	compare.RLSPoliciesWithSemantics(generated, database, diff, identifier.ForDialect("postgres"), compare.Coverage{})

	c.Assert(diff.RLSPoliciesAdded, qt.DeepEquals, []difftypes.RLSPolicyRef{
		{PolicyName: "alpha_policy", TableName: "alpha_orders"},
		{PolicyName: "zeta_policy", TableName: "alpha_orders"},
		{PolicyName: "alpha_policy", TableName: "zeta_orders"},
	})
	c.Assert(diff.RLSPoliciesRemoved, qt.DeepEquals, []difftypes.RLSPolicyRef{
		{PolicyName: "alpha_extra", TableName: "public.alpha_orders"},
		{PolicyName: "zeta_policy", TableName: "public.zeta_orders"},
	})
}
