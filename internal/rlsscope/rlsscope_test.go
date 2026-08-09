package rlsscope_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/internal/rlsscope"
	"go.5x5.cz/ptah/migration/schemadiff/types"
)

// TestNewResolver_RefusesADiffItCannotPlan covers the refusals the PostgreSQL
// planner delegates here, including the two a planner cannot reach on its own:
// a nil diff, and a target schema whose declarations collapse onto one identity.
func TestNewResolver_RefusesADiffItCannotPlan(t *testing.T) {
	declared := func(policies ...goschema.RLSPolicy) *goschema.Database {
		return &goschema.Database{RLSPolicies: policies}
	}

	tests := []struct {
		name      string
		diff      *types.SchemaDiff
		generated *goschema.Database
		wantError string
	}{
		{
			name:      "a nil diff",
			diff:      nil,
			generated: declared(),
			wantError: `invalid schema diff: schema diff is nil`,
		},
		{
			name: "an addition with no owning table",
			diff: &types.SchemaDiff{
				RLSPoliciesAdded: []types.RLSPolicyRef{{PolicyName: "p"}},
			},
			generated: declared(),
			wantError: `invalid schema diff: added RLS policy reference at position 0 requires a policy name and owning table`,
		},
		{
			name: "an addition with no policy name",
			diff: &types.SchemaDiff{
				RLSPoliciesAdded: []types.RLSPolicyRef{{TableName: "orders"}},
			},
			generated: declared(),
			wantError: `invalid schema diff: added RLS policy reference at position 0 requires a policy name and owning table`,
		},
		{
			name: "an addition the target schema does not hold",
			diff: &types.SchemaDiff{
				RLSPoliciesAdded: []types.RLSPolicyRef{{PolicyName: "p", TableName: "orders"}},
			},
			generated: declared(),
			wantError: `invalid schema diff: added RLS policy p on table orders at position 0 is missing from the target schema`,
		},
		{
			// Two spellings of one table carrying one policy name are one
			// policy under PostgreSQL's rules, so the map would keep whichever
			// came last and the plan would depend on declaration order.
			name: "a target schema with two declarations of one policy",
			diff: &types.SchemaDiff{},
			generated: declared(
				goschema.RLSPolicy{Name: "p", Table: "orders"},
				goschema.RLSPolicy{Name: "p", Table: "public.orders"},
			),
			wantError: `invalid schema diff: target RLS policies p on orders and p on public.orders share one identity in postgres`,
		},
		{
			name:      "a target declaration with no owning table",
			diff:      &types.SchemaDiff{},
			generated: declared(goschema.RLSPolicy{Name: "p"}),
			wantError: `invalid schema diff: target RLS policy reference at position 0 requires a policy name and owning table`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			resolver, err := rlsscope.NewResolver("postgres", test.diff, test.generated)

			c.Assert(resolver, qt.IsNil)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(err, qt.ErrorMatches, test.wantError)
		})
	}
}

// TestResolver_ResolveOnANilResolver is the guard for a caller that ignored a
// construction error. Returning a zero policy without saying so would put an
// empty CREATE POLICY into a plan.
func TestResolver_ResolveOnANilResolver(t *testing.T) {
	c := qt.New(t)
	var resolver *rlsscope.Resolver

	policy, err := resolver.Resolve(types.RLSPolicyRef{PolicyName: "p", TableName: "orders"})

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(err, qt.ErrorMatches, `invalid schema diff: no validated target RLS policies are available`)
	c.Assert(policy, qt.DeepEquals, goschema.RLSPolicy{})
}

// TestResolver_ResolvesEitherDefaultSchemaSpelling pins the matching rule the
// comparator already uses: `orders` and `public.orders` are one table on
// PostgreSQL, in whichever direction the two sides happen to spell it.
func TestResolver_ResolvesEitherDefaultSchemaSpelling(t *testing.T) {
	tests := []struct {
		name      string
		declared  string
		reference string
	}{
		{name: "declared bare, referenced qualified", declared: "orders", reference: "public.orders"},
		{name: "declared qualified, referenced bare", declared: "public.orders", reference: "orders"},
		{name: "both bare", declared: "orders", reference: "orders"},
		{name: "both qualified", declared: "public.orders", reference: "public.orders"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			generated := &goschema.Database{
				RLSPolicies: []goschema.RLSPolicy{
					{Name: "p", Table: test.declared, UsingExpression: "tenant_id = 1"},
				},
			}
			ref := types.RLSPolicyRef{PolicyName: "p", TableName: test.reference}

			resolver, err := rlsscope.NewResolverWithSemantics(
				"postgres",
				identifier.ForDialect("postgres"),
				&types.SchemaDiff{RLSPoliciesAdded: []types.RLSPolicyRef{ref}},
				generated,
			)
			c.Assert(err, qt.IsNil)

			policy, err := resolver.Resolve(ref)

			c.Assert(err, qt.IsNil)
			// The declared spelling survives: only the matching is normalized,
			// because the planner renders whatever it is handed.
			c.Assert(policy.Table, qt.Equals, test.declared)
			c.Assert(policy.UsingExpression, qt.Equals, "tenant_id = 1")
		})
	}
}

// TestResolver_KeepsOnePolicyNamePerTable is the control the identity must not
// swallow: two tables in one schema may each carry a policy called
// tenant_isolation, and each reference must reach its own.
func TestResolver_KeepsOnePolicyNamePerTable(t *testing.T) {
	tests := []struct {
		name      string
		table     string
		wantUsing string
	}{
		{name: "alpha", table: "alpha_orders", wantUsing: "tenant_id = 1"},
		{name: "zeta", table: "zeta_orders", wantUsing: "tenant_id = 2"},
	}

	generated := &goschema.Database{
		RLSPolicies: []goschema.RLSPolicy{
			{Name: "tenant_isolation", Table: "alpha_orders", UsingExpression: "tenant_id = 1"},
			{Name: "tenant_isolation", Table: "zeta_orders", UsingExpression: "tenant_id = 2"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ref := types.RLSPolicyRef{PolicyName: "tenant_isolation", TableName: test.table}

			resolver, err := rlsscope.NewResolver("postgres", &types.SchemaDiff{}, generated)
			c.Assert(err, qt.IsNil)

			policy, err := resolver.Resolve(ref)

			c.Assert(err, qt.IsNil)
			c.Assert(policy.UsingExpression, qt.Equals, test.wantUsing)
		})
	}
}
