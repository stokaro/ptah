package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/coverage"
	"ptah.run/core/schemaext"
	"ptah.run/engine/builtin"
	"ptah.run/feature/pgpolicy"
	"ptah.run/migration/schemadiff"
)

// TestCompare_OwnedPolicyHonorsTheDescriptionsLimits pins that a description
// declining policies declines the row-security owner's policies. The
// compatibility surface leaves policy blocks out of an inspected document and
// says so in its header; applying that document back to the database it came
// from planned DROP POLICY for every policy the database held.
func TestCompare_OwnedPolicyHonorsTheDescriptionsLimits(t *testing.T) {
	tests := []struct {
		name    string
		limited coverage.Set
	}{
		{name: "a record naming every policy", limited: coverage.Set{}.With(coverage.Object{
			Kind: coverage.Policy, Reason: coverage.SuppressedByPolicy, Provenance: coverage.Defaulted})},
		{name: "a record naming the policy", limited: coverage.Set{}.WithObject(coverage.Policy, "tenant")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := ordersDeclaringPolicies()
			desired.NotDescribed = test.limited

			diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), desired, ordersHoldingTenant("public", "public"),
				pgConnection("public"), nil, must.Must(builtin.New()))

			c.Assert(err, qt.IsNil)
			c.Assert(featureChanges(diff), qt.HasLen, 0)
		})
	}
}

// TestCompare_OwnedPolicyLimitsStillPlanWhatTheyDoNotName is the control: a
// record naming another policy, or another kind, leaves this policy's drop
// planned.
func TestCompare_OwnedPolicyLimitsStillPlanWhatTheyDoNotName(t *testing.T) {
	tests := []struct {
		name    string
		limited coverage.Set
	}{
		{name: "a record naming another policy", limited: coverage.Set{}.WithObject(coverage.Policy, "audit")},
		{name: "a record naming another kind", limited: coverage.Set{}.WithKind(coverage.Sequence)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := ordersDeclaringPolicies()
			desired.NotDescribed = test.limited

			diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), desired, ordersHoldingTenant("public", "public"),
				pgConnection("public"), nil, must.Must(builtin.New()))

			c.Assert(err, qt.IsNil)
			changes := featureChanges(diff)
			c.Assert(changes, qt.HasLen, 1)
			c.Assert(changes[0].Subject.Key(), qt.Equals, pgpolicy.PolicyRef("public", "orders", "tenant").Key())
			c.Assert(changes[0].Value.(*pgpolicy.PolicyChange).After, qt.IsNil)
		})
	}
}

// TestCompare_OwnedPolicyAdditionWaitsForARead pins the other side: a read
// that did not describe policies cannot say a declared policy is missing, so
// the comparison does not finish rather than plan its creation.
func TestCompare_OwnedPolicyAdditionWaitsForARead(t *testing.T) {
	declared := must.Must(pgpolicy.DesiredPolicyObject(pgpolicy.PolicyRef("", "orders", "tenant"), pgpolicy.DesiredPolicy{Using: new("tenant_id = 1")}))
	current := func(limited coverage.Set) *catalog.Database {
		database := ordersHoldingTenant("public", "public")
		database.FeatureObjects = schemaext.Objects{}
		database.NotDescribed = limited
		return database
	}

	t.Run("the read declines policies", func(t *testing.T) {
		c := qt.New(t)

		diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), ordersDeclaringPolicies(declared),
			current(coverage.Set{}.With(coverage.Refused(coverage.Policy))), pgConnection("public"), nil, must.Must(builtin.New()))

		c.Assert(err, qt.ErrorIs, schemadiff.ErrIncompleteComparison)
		c.Assert(diff, qt.IsNil)
	})

	t.Run("control: a read that describes them plans the creation", func(t *testing.T) {
		c := qt.New(t)

		diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), ordersDeclaringPolicies(declared),
			current(coverage.Set{}), pgConnection("public"), nil, must.Must(builtin.New()))

		c.Assert(err, qt.IsNil)
		changes := featureChanges(diff)
		c.Assert(changes, qt.HasLen, 1)
		c.Assert(changes[0].Value.(*pgpolicy.PolicyChange).Before, qt.IsNil)
	})
}
