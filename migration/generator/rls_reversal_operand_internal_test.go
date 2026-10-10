package generator

// White-box testing required: what this pins is which policy the reversal hands
// each direction, and the reversal is unexported. Through the public API a
// rollback that recreates the wrong predicate and one that recreates the right
// one are both just SQL that applies -- and the direction that recreates
// nothing is a plan that succeeds.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/difftypes"
)

// TestReverseSchemaDiff_ARolledBackRLSModificationRestoresThePriorPredicate is
// the second of the three directions.
//
// A modification renders CREATE POLICY from its operand, so reversing the
// change map without reversing the operand would have the down direction
// re-apply the predicate it is undoing.
func TestReverseSchemaDiff_ARolledBackRLSModificationRestoresThePriorPredicate(t *testing.T) {
	c := qt.New(t)

	prior := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Order", Name: "orders", Schema: "sales"}},
		RLSPolicies: []schemamodel.RLSPolicy{{
			Name: "tenant", Table: "orders", PolicyFor: "ALL", UsingExpression: "tenant_id = 1",
		}},
	}

	reversed := reverseRLSPolicyDiffs([]difftypes.RLSPolicyDiff{{
		PolicyName: "tenant", TableName: "public.orders",
		Changes: map[string]string{"using_expression": "tenant_id = 1 -> tenant_id = 2"},
		Desired: schemamodel.RLSPolicy{
			Name: "tenant", Table: "orders", PolicyFor: "ALL", UsingExpression: "tenant_id = 2",
		},
	}}, prior, identifier.ForDialect(platform.Postgres))

	c.Assert(reversed, qt.HasLen, 1)
	c.Assert(reversed[0].Changes["using_expression"], qt.Equals, "tenant_id = 2 -> tenant_id = 1")
	c.Assert(reversed[0].Desired.UsingExpression, qt.Equals, "tenant_id = 1",
		qt.Commentf("the rollback recreates the predicate the database held"))
	c.Assert(reversed[0].TableSchema, qt.Equals, "sales",
		qt.Commentf("and the schema its table is declared under there, which SQL Server addresses it by"))
}

// TestReverseSchemaDiff_ARolledBackRLSAdditionCarriesNoOperand is the third.
//
// A DROP is written from the two names, so the declaration is dropped rather
// than carried across: an entry holding a policy nothing reads tells the next
// reader that something does.
//
// It drives reverseSchemaDiffWithPrior rather than the helper underneath,
// which is the difference between pinning what the helper answers and pinning
// that the reversal calls it. Nothing downstream renders differently either
// way, so the helper is the only place this is observable AND the reversal is
// the only place it matters -- both halves have to be in the assertion.
func TestReverseSchemaDiff_ARolledBackRLSAdditionCarriesNoOperand(t *testing.T) {
	c := qt.New(t)

	forward := &difftypes.SchemaDiff{
		RLSPoliciesAdded: []difftypes.RLSPolicyRef{{
			PolicyName: "tenant", TableName: "orders",
			Desired:     schemamodel.RLSPolicy{Name: "tenant", Table: "orders", PolicyFor: "ALL"},
			TableSchema: "sales",
		}},
	}

	reversed := reverseForTest(t,
		forward, &schemamodel.Database{}, &catalog.Database{}, "postgres",
	)

	c.Assert(reversed.RLSPoliciesRemoved, qt.HasLen, 1)
	c.Assert(reversed.RLSPoliciesRemoved[0].PolicyName, qt.Equals, "tenant")
	c.Assert(reversed.RLSPoliciesRemoved[0].TableName, qt.Equals, "orders")
	c.Assert(reversed.RLSPoliciesRemoved[0].Desired, qt.DeepEquals, schemamodel.RLSPolicy{})
	c.Assert(reversed.RLSPoliciesRemoved[0].TableSchema, qt.Equals, "")
}
