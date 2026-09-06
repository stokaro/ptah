package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/difftypes"
	"ptah.run/migration/schemadiff/internal/compare"
)

// generatedSharedPolicyName is the desired schema used by the addition and
// removal tables below: one policy name carried by two different tables, which
// PostgreSQL permits because a policy name is scoped to its table. Measured on
// PostgreSQL 17.10, CREATE POLICY tenant_isolation succeeds on both
// public.alpha_orders and public.zeta_orders and leaves two rows in pg_policy.
func generatedSharedPolicyName() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{
			{Name: "alpha_orders", StructName: "AlphaOrder"},
			{Name: "zeta_orders", StructName: "ZetaOrder"},
		},
		RLSPolicies: []schemamodel.RLSPolicy{
			{Name: "tenant_isolation", Table: "alpha_orders", PolicyFor: "ALL", ToRoles: "PUBLIC", UsingExpression: "tenant_id = 1"},
			{Name: "tenant_isolation", Table: "zeta_orders", PolicyFor: "ALL", ToRoles: "PUBLIC", UsingExpression: "tenant_id = 1"},
		},
	}
}

func TestRLSPoliciesWithSemantics_TableQualifiedAdditions(t *testing.T) {
	tests := []struct {
		name     string
		database *catalog.Database
		want     [][2]string
	}{
		{
			name:     "both policies missing",
			database: &catalog.Database{},
			want: [][2]string{
				{"tenant_isolation", "alpha_orders"},
				{"tenant_isolation", "zeta_orders"},
			},
		},
		{
			name: "one table has the policy",
			database: &catalog.Database{
				RLSPolicies: []catalog.RLSPolicy{
					{Name: "tenant_isolation", Table: "public.alpha_orders", PolicyFor: "ALL", ToRoles: "PUBLIC", UsingExpression: "tenant_id = 1"},
				},
			},
			want: [][2]string{
				{"tenant_isolation", "zeta_orders"},
			},
		},
		{
			name: "both tables have the policy",
			database: &catalog.Database{
				RLSPolicies: []catalog.RLSPolicy{
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
				nil,
			)

			c.Assert(policyRefPairs(diff.RLSPoliciesAdded), qt.DeepEquals, test.want)
			c.Assert(diff.RLSPoliciesRemoved, qt.HasLen, 0)
			c.Assert(diff.RLSPoliciesModified, qt.HasLen, 0)
		})
	}
}

func TestRLSPoliciesWithSemantics_TableQualifiedRemovals(t *testing.T) {
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{
			{Name: "alpha_orders", StructName: "AlphaOrder"},
			{Name: "zeta_orders", StructName: "ZetaOrder"},
		},
		RLSPolicies: []schemamodel.RLSPolicy{
			{Name: "tenant_isolation", Table: "alpha_orders", PolicyFor: "ALL", ToRoles: "PUBLIC", UsingExpression: "tenant_id = 1"},
		},
	}

	tests := []struct {
		name     string
		database *catalog.Database
		want     []difftypes.RLSPolicyRef
	}{
		{
			name: "same name on an undeclared table is removed",
			database: &catalog.Database{
				RLSPolicies: []catalog.RLSPolicy{
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
			database: &catalog.Database{
				RLSPolicies: []catalog.RLSPolicy{
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
			database: &catalog.Database{
				RLSPolicies: []catalog.RLSPolicy{
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

			compare.RLSPoliciesWithSemantics(desired, test.database, diff, identifier.ForDialect("postgres"), compare.Coverage{}, nil)

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

	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{
			{Name: "alpha_orders", StructName: "AlphaOrder"},
			{Name: "zeta_orders", StructName: "ZetaOrder"},
		},
		RLSPolicies: []schemamodel.RLSPolicy{
			{Name: "tenant_isolation", Table: "alpha_orders", PolicyFor: "ALL", ToRoles: "PUBLIC", UsingExpression: "tenant_id = 1"},
		},
	}
	database := &catalog.Database{
		RLSPolicies: []catalog.RLSPolicy{
			{Name: "tenant_isolation", Table: "public.alpha_orders", PolicyFor: "ALL", ToRoles: "PUBLIC", UsingExpression: "tenant_id = 99"},
			{Name: "tenant_isolation", Table: "public.zeta_orders", PolicyFor: "ALL", ToRoles: "PUBLIC", UsingExpression: "tenant_id = 1"},
		},
	}
	diff := &difftypes.SchemaDiff{}

	compare.RLSPoliciesWithSemantics(desired, database, diff, identifier.ForDialect("postgres"), compare.Coverage{}, nil)

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

	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: "alpha_orders", Schema: "public", StructName: "AlphaOrder"}},
		RLSPolicies: []schemamodel.RLSPolicy{
			{Name: "tenant_isolation", Table: "public.alpha_orders", PolicyFor: "ALL", ToRoles: "PUBLIC", UsingExpression: "tenant_id = 1"},
		},
	}
	database := &catalog.Database{
		RLSPolicies: []catalog.RLSPolicy{
			{Name: "tenant_isolation", Table: "alpha_orders", PolicyFor: "ALL", ToRoles: "PUBLIC", UsingExpression: "tenant_id = 1"},
		},
	}
	diff := &difftypes.SchemaDiff{}

	compare.RLSPoliciesWithSemantics(desired, database, diff, identifier.ForDialect("postgres"), compare.Coverage{}, nil)

	c.Assert(diff.RLSPoliciesAdded, qt.HasLen, 0)
	c.Assert(diff.RLSPoliciesRemoved, qt.HasLen, 0)
	c.Assert(diff.RLSPoliciesModified, qt.HasLen, 0)
}

// TestRLSPolicies_DelegatesWithDialectlessSemantics pins the thin wrapper to
// the same behavior the package's other name-scoped comparisons expose, so a
// caller that has no dialect still gets table-scoped matching.
func TestRLSPolicies_DelegatesWithDialectlessSemantics(t *testing.T) {
	c := qt.New(t)

	database := &catalog.Database{
		RLSPolicies: []catalog.RLSPolicy{
			{Name: "tenant_isolation", Table: "alpha_orders", PolicyFor: "ALL", ToRoles: "PUBLIC", UsingExpression: "tenant_id = 1"},
		},
	}
	diff := &difftypes.SchemaDiff{}

	compare.RLSPolicies(generatedSharedPolicyName(), database, diff, compare.Coverage{})

	c.Assert(policyRefPairs(diff.RLSPoliciesAdded), qt.DeepEquals, [][2]string{
		{"tenant_isolation", "zeta_orders"},
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

	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{
			{Name: "alpha_orders", StructName: "AlphaOrder"},
			{Name: "zeta_orders", StructName: "ZetaOrder"},
		},
		RLSPolicies: []schemamodel.RLSPolicy{
			{Name: "zeta_policy", Table: "alpha_orders", PolicyFor: "ALL", ToRoles: "PUBLIC", UsingExpression: "tenant_id = 1"},
			{Name: "alpha_policy", Table: "zeta_orders", PolicyFor: "ALL", ToRoles: "PUBLIC", UsingExpression: "tenant_id = 2"},
			{Name: "alpha_policy", Table: "alpha_orders", PolicyFor: "ALL", ToRoles: "PUBLIC", UsingExpression: "tenant_id = 3"},
		},
	}
	database := &catalog.Database{
		RLSPolicies: []catalog.RLSPolicy{
			{Name: "zeta_policy", Table: "public.zeta_orders", PolicyFor: "ALL", ToRoles: "PUBLIC", UsingExpression: "tenant_id = 4"},
			{Name: "alpha_extra", Table: "public.alpha_orders", PolicyFor: "ALL", ToRoles: "PUBLIC", UsingExpression: "tenant_id = 5"},
		},
	}
	diff := &difftypes.SchemaDiff{}

	compare.RLSPoliciesWithSemantics(desired, database, diff, identifier.ForDialect("postgres"), compare.Coverage{}, nil)

	// The pairs rather than the whole entries: this test is about the ORDER,
	// and an addition also carries the declaration it renders from, which
	// TestRLSPoliciesWithSemantics_AnAdditionCarriesItsDeclaration pins.
	c.Assert(policyRefPairs(diff.RLSPoliciesAdded), qt.DeepEquals, [][2]string{
		{"alpha_policy", "alpha_orders"},
		{"zeta_policy", "alpha_orders"},
		{"alpha_policy", "zeta_orders"},
	})
	c.Assert(policyRefPairs(diff.RLSPoliciesRemoved), qt.DeepEquals, [][2]string{
		{"alpha_extra", "public.alpha_orders"},
		{"zeta_policy", "public.zeta_orders"},
	})
}

// policyRefPairs reads the identity halves off a reference list, which is what
// the ordering and membership assertions above are about. An addition also
// carries the declaration it renders from; that is a different question and has
// its own test.
func policyRefPairs(refs []difftypes.RLSPolicyRef) [][2]string {
	if len(refs) == 0 {
		return nil
	}
	pairs := make([][2]string, 0, len(refs))
	for _, ref := range refs {
		pairs = append(pairs, [2]string{ref.PolicyName, ref.TableName})
	}
	return pairs
}

// TestRLSPoliciesWithSemantics_AnAdditionCarriesItsDeclaration pins the operand
// half the tables above deliberately leave out.
//
// The planner renders CREATE POLICY from this and reads no schema, so an
// addition that arrived with only its two names would be refused rather than
// planned (stokaro/ptah#2315). A removal carries none, because `DROP POLICY
// name ON table` is written from the names.
func TestRLSPoliciesWithSemantics_AnAdditionCarriesItsDeclaration(t *testing.T) {
	c := qt.New(t)

	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: "orders", StructName: "Order", Schema: "sales"}},
		RLSPolicies: []schemamodel.RLSPolicy{{
			Name: "tenant_isolation", Table: "orders",
			PolicyFor: "ALL", ToRoles: "PUBLIC", UsingExpression: "tenant_id = 1",
		}},
	}
	database := &catalog.Database{RLSPolicies: []catalog.RLSPolicy{{
		Name: "legacy", Table: "public.orders", PolicyFor: "ALL", UsingExpression: "true",
	}}}
	diff := &difftypes.SchemaDiff{}

	compare.RLSPoliciesWithSemantics(
		desired, database, diff, identifier.ForDialect("postgres"), compare.Coverage{}, nil)

	c.Assert(diff.RLSPoliciesAdded, qt.HasLen, 1)
	c.Assert(diff.RLSPoliciesAdded[0].Desired.UsingExpression, qt.Equals, "tenant_id = 1")
	c.Assert(diff.RLSPoliciesAdded[0].TableSchema, qt.Equals, "sales",
		qt.Commentf("the schema the owning table is declared under, which SQL Server addresses the policy by"))

	c.Assert(diff.RLSPoliciesRemoved, qt.HasLen, 1)
	c.Assert(diff.RLSPoliciesRemoved[0].Desired, qt.DeepEquals, schemamodel.RLSPolicy{},
		qt.Commentf("a DROP is written from the two names"))
}

// TestRLSPoliciesWithSemantics_TwoDeclarationsOfOneIdentityAreRecorded pins what
// the comparison does with a collision it cannot report any other way.
//
// `orders` and `public.orders` are one table under postgres semantics, so these
// two declarations resolve to one identity. Keying by that identity is what
// pairs a declaration with the database, so the map keeps one of them -- and the
// plan would then apply whichever it kept. The pair is recorded instead, for a
// planner to refuse on (stokaro/ptah#2440).
func TestRLSPoliciesWithSemantics_TwoDeclarationsOfOneIdentityAreRecorded(t *testing.T) {
	c := qt.New(t)

	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: "orders", StructName: "Order"}},
		RLSPolicies: []schemamodel.RLSPolicy{
			{Name: "tenant", Table: "orders", PolicyFor: "ALL", UsingExpression: "a = 1"},
			{Name: "tenant", Table: "public.orders", PolicyFor: "ALL", UsingExpression: "b = 2"},
		},
	}
	diff := &difftypes.SchemaDiff{}

	compare.RLSPoliciesWithSemantics(
		desired, &catalog.Database{}, diff, identifier.ForDialect("postgres"), compare.Coverage{}, nil)

	c.Assert(diff.RLSPolicyIdentityConflicts, qt.HasLen, 1)
	c.Assert(diff.RLSPolicyIdentityConflicts[0].First.UsingExpression, qt.Equals, "a = 1")
	c.Assert(diff.RLSPolicyIdentityConflicts[0].Second.UsingExpression, qt.Equals, "b = 2")
	c.Assert(diff.RLSPolicyIdentityConflicts[0].First.Table, qt.Equals, "orders",
		qt.Commentf("each side keeps the spelling its declaration supplied, because the refusal names them"))
	c.Assert(diff.RLSPolicyIdentityConflicts[0].Second.Table, qt.Equals, "public.orders")
}

// TestRLSPoliciesWithSemantics_TwoTablesShareAPolicyNameWithoutConflict is the
// control the test above needs.
//
// A policy name is scoped to its table, so `tenant` on two DIFFERENT tables is
// two policies. Recording that as a conflict would refuse a schema PostgreSQL
// accepts -- measured on 17.10, both CREATE POLICY statements succeed and
// pg_policy holds two rows (stokaro/ptah#1276).
func TestRLSPoliciesWithSemantics_TwoTablesShareAPolicyNameWithoutConflict(t *testing.T) {
	c := qt.New(t)

	diff := &difftypes.SchemaDiff{}

	compare.RLSPoliciesWithSemantics(
		generatedSharedPolicyName(), &catalog.Database{}, diff,
		identifier.ForDialect("postgres"), compare.Coverage{}, nil)

	c.Assert(diff.RLSPolicyIdentityConflicts, qt.HasLen, 0)
	c.Assert(diff.RLSPoliciesAdded, qt.HasLen, 2)
}
