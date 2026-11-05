package pgpolicyprovider_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/featureplan"
	"ptah.run/core/plangraph"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgpolicy"
)

// rebuildChange is a change of the policy name on orders planned around the column
// changes of its plan.
func rebuildChange(name string, before *pgpolicy.ObservedPolicy, after *pgpolicy.DesiredPolicy) schemaext.ChangeRecord {
	record := policyChange(name, before, after, false)
	record.Value = record.Value.(*pgpolicy.PolicyChange).AroundColumnChanges()
	return record
}

// TestPlanPolicies_RebuildsAroundColumnChanges pins the steps of a rebuild: the drop ahead
// of the column changes and the creation after them, in that order, and neither held by
// the access order, which a replacement in place would answer to.
func TestPlanPolicies_RebuildsAroundColumnChanges(t *testing.T) {
	commented := permissiveDeclared
	commented.Comment = "tenants"
	tests := []struct {
		name   string
		change schemaext.ChangeRecord
		want   map[string]featureplan.Phase
	}{
		{
			name:   "a policy that stays",
			change: rebuildChange("tenant", &permissiveObserved, &permissiveDeclared),
			want: map[string]featureplan.Phase{
				"policy/000000/drop": featureplan.PhaseLifecycle,
				"policy/000000":      featureplan.PhaseDependent,
			},
		},
		{
			name:   "a policy with a comment",
			change: rebuildChange("tenant", &permissiveObserved, &commented),
			want: map[string]featureplan.Phase{
				"policy/000000/drop":    featureplan.PhaseLifecycle,
				"policy/000000":         featureplan.PhaseDependent,
				"policy/000000/comment": featureplan.PhaseDependent,
			},
		},
		{
			name:   "a policy that leaves",
			change: rebuildChange("tenant", &restrictiveObserved, nil),
			want:   map[string]featureplan.Phase{"policy/000000/drop": featureplan.PhaseLifecycle},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := newRuntime(c).PlanFeatures(t.Context(), planning(test.change))

			c.Assert(err, qt.IsNil)
			c.Assert(result.Contributions, qt.HasLen, 1)
			got := make(map[string]featureplan.Phase)
			for _, step := range result.Contributions[0].Steps {
				got[step.ID.Name] = step.Payload.Phase
				c.Assert(step.Transaction, qt.Equals, plangraph.TransactionAllowed)
			}
			c.Assert(got, qt.DeepEquals, test.want)
			c.Assert(result.Changes[0].Steps, qt.HasLen, len(test.want))
		})
	}
}

// TestPlanPolicies_RebuildLeavesTheAccessOrder pins a restrictive policy that leaves in a
// rebuild beside one the plan creates. The access order would create the new one first,
// which a dependent step cannot do ahead of the lifecycle span.
func TestPlanPolicies_RebuildLeavesTheAccessOrder(t *testing.T) {
	c := qt.New(t)
	request := planning(
		rebuildChange("old", &restrictiveObserved, nil),
		policyChange("new", nil, &restrictiveDeclared, false),
	)

	result, err := newRuntime(c).PlanFeatures(t.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Contributions[0].Dependencies, qt.HasLen, 0)
}

// TestRebuildChange_FailurePath refuses a declaration of another model.
func TestRebuildChange_FailurePath(t *testing.T) {
	c := qt.New(t)
	before := permissiveObserved

	got, err := before.RebuildChange(&pgpolicy.ObservedPolicy{})

	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(got, qt.IsNil)
}
