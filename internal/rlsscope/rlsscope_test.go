package rlsscope_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/rlsscope"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

// TestValidate_RefusesADiffItCannotPlan covers the refusals the PostgreSQL
// planner delegates here.
//
// Every one is answered from the diff alone. A reference missing half its
// identity is unplannable whatever the schema says, and a pair of declarations
// that resolved to one identity reaches here as a recorded conflict rather than
// as something to re-derive: the comparison already reduced them to one entry,
// so this is the only place the pair still exists (stokaro/ptah#2440).
func TestValidate_RefusesADiffItCannotPlan(t *testing.T) {
	tests := []struct {
		name      string
		diff      *difftypes.SchemaDiff
		wantError string
	}{
		{
			name:      "a nil diff",
			diff:      nil,
			wantError: `invalid schema diff: schema diff is nil`,
		},
		{
			name: "an addition with no owning table",
			diff: &difftypes.SchemaDiff{
				RLSPoliciesAdded: []difftypes.RLSPolicyRef{{PolicyName: "p"}},
			},
			wantError: `invalid schema diff: added RLS policy reference at position 0 requires a policy name and owning table`,
		},
		{
			name: "an addition with no policy name",
			diff: &difftypes.SchemaDiff{
				RLSPoliciesAdded: []difftypes.RLSPolicyRef{{TableName: "orders"}},
			},
			wantError: `invalid schema diff: added RLS policy reference at position 0 requires a policy name and owning table`,
		},
		{
			name: "a removal with no owning table",
			diff: &difftypes.SchemaDiff{
				RLSPoliciesRemoved: []difftypes.RLSPolicyRef{{PolicyName: "p"}},
			},
			wantError: `invalid schema diff: removed RLS policy reference at position 0 requires a policy name and owning table`,
		},
		{
			name: "a modification with no owning table",
			diff: &difftypes.SchemaDiff{
				RLSPoliciesModified: []difftypes.RLSPolicyDiff{{PolicyName: "p"}},
			},
			wantError: `invalid schema diff: modified RLS policy reference at position 0 requires a policy name and owning table`,
		},
		{
			// Two spellings of one table carrying one policy name are one
			// policy under PostgreSQL's rules, so the comparison kept whichever
			// came last and the plan would depend on declaration order.
			name: "two declarations that resolved to one identity",
			diff: &difftypes.SchemaDiff{
				RLSPolicyIdentityConflicts: []difftypes.RLSPolicyConflict{{
					First:  schemamodel.RLSPolicy{Name: "p", Table: "orders"},
					Second: schemamodel.RLSPolicy{Name: "p", Table: "public.orders"},
				}},
			},
			wantError: `invalid schema diff: target RLS policies p on orders and p on public.orders share one identity in postgres`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := rlsscope.Validate("postgres", test.diff)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(err, qt.ErrorMatches, test.wantError)
		})
	}
}

// TestValidate_AcceptsADiffItCanPlan is the control the refusals need.
//
// Without it, a Validate that refused everything would satisfy every row above.
// The rows here are the three shapes that must pass: a diff with nothing in it,
// one whose entries carry both halves of their identity, and one whose REMOVAL
// carries no declaration -- which is not a defect, because `DROP POLICY name ON
// table` is written from the two names.
func TestValidate_AcceptsADiffItCanPlan(t *testing.T) {
	tests := []struct {
		name string
		diff *difftypes.SchemaDiff
	}{
		{
			name: "an empty diff",
			diff: &difftypes.SchemaDiff{},
		},
		{
			name: "entries carrying both halves of their identity",
			diff: &difftypes.SchemaDiff{
				RLSPoliciesAdded: []difftypes.RLSPolicyRef{{
					PolicyName: "p", TableName: "orders",
					Desired: schemamodel.RLSPolicy{Name: "p", Table: "orders", PolicyFor: "ALL"},
				}},
				RLSPoliciesModified: []difftypes.RLSPolicyDiff{{
					PolicyName: "q", TableName: "orders",
					Desired: schemamodel.RLSPolicy{Name: "q", Table: "orders", PolicyFor: "ALL"},
				}},
			},
		},
		{
			name: "a removal carrying no declaration",
			diff: &difftypes.SchemaDiff{
				RLSPoliciesRemoved: []difftypes.RLSPolicyRef{{PolicyName: "p", TableName: "orders"}},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(rlsscope.Validate("postgres", test.diff), qt.IsNil)
		})
	}
}

// TestValidate_ReadsTheDiffAndNothingElse is the property the conversion is
// about.
//
// The refusals above used to be derived by indexing the desired schema, which
// is why the planner had to be handed one. Nothing here takes a schema any
// more, and the way to say so in a test is to show that a diff whose entries
// name policies NO schema could hold is accepted: the entries carry their own
// declarations, so there is nothing left to look up (stokaro/ptah#2315).
func TestValidate_ReadsTheDiffAndNothingElse(t *testing.T) {
	c := qt.New(t)

	diff := &difftypes.SchemaDiff{
		RLSPoliciesAdded: []difftypes.RLSPolicyRef{{
			PolicyName: "invented", TableName: "nowhere",
			Desired: schemamodel.RLSPolicy{Name: "invented", Table: "nowhere", PolicyFor: "ALL"},
		}},
	}

	c.Assert(rlsscope.Validate("postgres", diff), qt.IsNil)
}
