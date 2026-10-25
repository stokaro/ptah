package pgpolicy_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/feature/pgpolicy"
)

// policyRead is one question to [pgpolicy.ObservedPolicy.ReadsRelationIn]
// about a policy whose USING expression is using, as PostgreSQL prints it.
type policyRead struct {
	name          string
	using         string
	relation      string
	defaultSchema string
}

// TestObservedPolicy_ReadsRelationIn_Reads pins the policies a plan that drops
// relation with CASCADE has to refuse, because the drop takes them. A test that
// matches the qualified name as one piece of text misses the quoted spellings,
// and the policy is dropped with no refusal (stokaro/ptah#5004).
func TestObservedPolicy_ReadsRelationIn_Reads(t *testing.T) {
	tests := []policyRead{
		{
			name: "a quoted qualified reference", using: `(EXISTS ( SELECT 1 FROM "reporting"."order_report"))`,
			relation: "reporting.order_report", defaultSchema: "public",
		},
		{
			name: "a quoted name", using: `(EXISTS ( SELECT 1 FROM reporting."order_report"))`,
			relation: "reporting.order_report", defaultSchema: "public",
		},
		{
			name:     "a bare reference to a relation declared in the default schema",
			using:    "(id IN ( SELECT item_skus.id FROM item_skus))",
			relation: "public.item_skus", defaultSchema: "public",
		},
		{
			name:     "a reference in the default schema to a relation declared bare",
			using:    "(id IN ( SELECT item_skus.id FROM public.item_skus))",
			relation: "item_skus", defaultSchema: "public",
		},
		{
			name:     "a bare reference in a non-public default schema",
			using:    "(id IN ( SELECT item_skus.id FROM item_skus))",
			relation: "app.item_skus", defaultSchema: "app",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			policy := pgpolicy.ObservedPolicy{Using: &test.using}
			c.Assert(policy.ReadsRelationIn(test.relation, test.defaultSchema), qt.IsTrue)
		})
	}
}

// TestObservedPolicy_ReadsRelationIn_DoesNotRead pins the policies a plan must
// not refuse: each reads a relation of the dropped one's name in another
// schema, and a refusal there stops a column type change under
// public.item_skus that takes no policy with it (stokaro/ptah#5005).
func TestObservedPolicy_ReadsRelationIn_DoesNotRead(t *testing.T) {
	tests := []policyRead{
		{
			name:     "a relation declared bare and a reference into another schema",
			using:    "(id IN ( SELECT item_skus.id FROM reporting.item_skus))",
			relation: "item_skus", defaultSchema: "public",
		},
		{
			name:     "a qualified relation and a reference into another schema",
			using:    "(id IN ( SELECT item_skus.id FROM reporting.item_skus))",
			relation: "public.item_skus", defaultSchema: "public",
		},
		{
			name:     "a relation outside the default schema and a bare reference",
			using:    "(id IN ( SELECT item_skus.id FROM item_skus))",
			relation: "reporting.item_skus", defaultSchema: "public",
		},
		{
			name: "another relation", using: "(id IN ( SELECT tenants.id FROM tenants))",
			relation: "item_skus", defaultSchema: "public",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			policy := pgpolicy.ObservedPolicy{Using: &test.using}
			c.Assert(policy.ReadsRelationIn(test.relation, test.defaultSchema), qt.IsFalse)
		})
	}
}

// TestObservedPolicy_AnswersWithTheDefaultSchema pins the path a planner takes,
// [schemaext.ReadsRelationIn]: the policy is told the default schema. Asked
// without it, the same policy reads the relation, so a policy the helper did
// not recognize as schema-aware would answer true here.
func TestObservedPolicy_AnswersWithTheDefaultSchema(t *testing.T) {
	c := qt.New(t)
	using := "(id IN ( SELECT item_skus.id FROM reporting.item_skus))"
	policy := &pgpolicy.ObservedPolicy{Using: &using}

	c.Assert(policy.ReadsRelation("item_skus"), qt.IsTrue)
	c.Assert(schemaext.ReadsRelationIn(policy, "item_skus", "public"), qt.IsFalse)
}

// TestObservedPolicy_ReadsRelationIn_NilPolicy pins the nil receiver: a policy
// that is not there reads nothing.
func TestObservedPolicy_ReadsRelationIn_NilPolicy(t *testing.T) {
	c := qt.New(t)
	var policy *pgpolicy.ObservedPolicy

	c.Assert(policy.ReadsRelationIn("item_skus", "public"), qt.IsFalse)
}
