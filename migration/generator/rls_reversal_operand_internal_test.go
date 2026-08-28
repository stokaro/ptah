package generator

// White-box testing required: what this pins is which policy the reversal hands
// each direction, and the reversal is unexported. Through the public API a
// rollback that recreates the wrong predicate and one that recreates the right
// one are both just SQL that applies -- and the direction that recreates
// nothing is a plan that succeeds.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/migration/schemadiff"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

// TestGenerateDownMigrationSQL_RecreatesAnRLSPolicyTheUpDirectionDropped drives
// the whole pipeline, which is the only way this direction can be pinned.
//
// A forward removal holds two names, which is all `DROP POLICY name ON table`
// needs. Reversed it becomes an addition, and CREATE POLICY needs a
// declaration -- so the operand has to be recovered from the pre-change
// database. Restoring the plain exchange leaves the rollback with an entry
// carrying nothing, and the planner then refuses it; before the refusal existed
// it emitted nothing and reported success, which is the shape
// stokaro/ptah#1311 was reviewed for.
func TestGenerateDownMigrationSQL_RecreatesAnRLSPolicyTheUpDirectionDropped(t *testing.T) {
	c := qt.New(t)

	const priorPredicate = "tenant_id = current_setting('app.tenant')::uuid"

	// The declaration does not name the policy; the database has it. That is
	// what puts it in RLSPoliciesRemoved.
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Order", Name: "orders"}},
		Fields: []schemamodel.Field{{StructName: "Order", Name: "id", Type: "SERIAL", Primary: true}},
	}
	database := &catalog.Database{
		Tables: []catalog.Table{{Schema: "public", Name: "orders"}},
		RLSPolicies: []catalog.RLSPolicy{{
			Name: "tenant_isolation", Table: "public.orders",
			PolicyFor: "ALL", ToRoles: "app", UsingExpression: priorPredicate,
		}},
	}

	caps := capability.Postgres17()
	upDiff := schemadiff.CompareWithDialect(desired, database, platform.Postgres)
	c.Assert(upDiff.RLSPoliciesRemoved, qt.HasLen, 1)
	c.Assert(upDiff.RLSPoliciesAdded, qt.HasLen, 0)

	up, err := generateUpMigrationSQL(upDiff, desired, platform.Postgres, caps)
	c.Assert(err, qt.IsNil)
	c.Assert(up, qt.Contains, "DROP POLICY")

	down, err := generateDownMigrationSQL(upDiff, desired, database, platform.Postgres, caps)
	c.Assert(err, qt.IsNil)
	c.Assert(down, qt.Contains, "CREATE POLICY",
		qt.Commentf("the rollback puts back the policy the up direction dropped\n%s", down))
	c.Assert(down, qt.Contains, priorPredicate,
		qt.Commentf("with the predicate the database held\n%s", down))
}

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
// It drives reverseSchemaDiffWithSchema rather than the helper underneath,
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

	reversed := reverseSchemaDiffWithSchema(forward, &schemamodel.Database{}, &catalog.Database{})

	c.Assert(reversed.RLSPoliciesRemoved, qt.HasLen, 1)
	c.Assert(reversed.RLSPoliciesRemoved[0].PolicyName, qt.Equals, "tenant")
	c.Assert(reversed.RLSPoliciesRemoved[0].TableName, qt.Equals, "orders")
	c.Assert(reversed.RLSPoliciesRemoved[0].Desired, qt.DeepEquals, schemamodel.RLSPolicy{})
	c.Assert(reversed.RLSPoliciesRemoved[0].TableSchema, qt.Equals, "")
}
