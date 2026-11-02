package pgpolicyprovider_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/feature/pgpolicy"
)

// TestComparePolicies_CountsWhatItCompares pins the counts the inventory
// reports for policies: a policy both sides hold on a table both sides hold is
// matched whether or not it differs, a creation is added and a drop removed.
// A policy the comparison adopts, cannot decide, or leaves to its table's
// creation or removal is counted on neither side.
func TestComparePolicies_CountsWhatItCompares(t *testing.T) {
	described := must.Must(pgpolicy.CompleteCoverage(schemaext.Desired))
	read := must.Must(pgpolicy.CompleteCoverage(schemaext.Observed))
	notDescribed := must.Must(pgpolicy.Coverage(pgpolicy.PolicyKind, schemaext.Desired, schemaext.Knowledge{State: schemaext.Uninspected, Reason: "not read"}, nil))
	notRead := must.Must(pgpolicy.Coverage(pgpolicy.PolicyKind, schemaext.Observed, schemaext.Knowledge{State: schemaext.Uninspected, Reason: "not read"}, nil))
	tests := []struct {
		name     string
		parent   schemaext.ParentState
		declared []pgpolicy.DesiredPolicy
		observed []pgpolicy.ObservedPolicy
		desired  schemaext.Coverage
		current  schemaext.Coverage
		want     schemaext.ObjectCount
	}{
		{name: "a policy that agrees", parent: surviving("orders"), desired: described, current: read,
			declared: []pgpolicy.DesiredPolicy{{Using: new("tenant_id = 1")}}, observed: []pgpolicy.ObservedPolicy{held("tenant_id = 1")},
			want: schemaext.ObjectCount{Kind: pgpolicy.PolicyChangeKind, Matched: 1}},
		{name: "a policy that differs", parent: surviving("orders"), desired: described, current: read,
			declared: []pgpolicy.DesiredPolicy{{Using: new("tenant_id = 2")}}, observed: []pgpolicy.ObservedPolicy{held("tenant_id = 1")},
			want: schemaext.ObjectCount{Kind: pgpolicy.PolicyChangeKind, Matched: 1}},
		{name: "a creation", parent: surviving("orders"), desired: described, current: read,
			declared: []pgpolicy.DesiredPolicy{{Using: new("true")}},
			want:     schemaext.ObjectCount{Kind: pgpolicy.PolicyChangeKind, Added: 1}},
		{name: "a drop", parent: surviving("orders"), desired: described, current: read,
			observed: []pgpolicy.ObservedPolicy{held("true")},
			want:     schemaext.ObjectCount{Kind: pgpolicy.PolicyChangeKind, Removed: 1}},
		{name: "a policy the desired source could not describe", parent: surviving("orders"), desired: notDescribed, current: read,
			observed: []pgpolicy.ObservedPolicy{held("true")},
			want:     schemaext.ObjectCount{Kind: pgpolicy.PolicyChangeKind}},
		{name: "a declaration the read could not decide", parent: surviving("orders"), desired: described, current: notRead,
			declared: []pgpolicy.DesiredPolicy{{Using: new("true")}},
			want:     schemaext.ObjectCount{Kind: pgpolicy.PolicyChangeKind}},
		{name: "a table the plan creates", parent: schemaext.ParentState{Subject: tableRef("orders"), Desired: true}, desired: described, current: read,
			declared: []pgpolicy.DesiredPolicy{{Using: new("true")}},
			want:     schemaext.ObjectCount{Kind: pgpolicy.PolicyChangeKind}},
		{name: "a table the plan drops", parent: schemaext.ParentState{Subject: tableRef("orders"), Current: true}, desired: described, current: read,
			observed: []pgpolicy.ObservedPolicy{held("true")},
			want:     schemaext.ObjectCount{Kind: pgpolicy.PolicyChangeKind}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := comparison(c, declaredObjects(c, test.declared...), observedObjects(c, test.observed...), test.parent)
			request.Desired.Coverage, request.Current.Coverage = test.desired, test.current

			result, err := newRuntime(c).CompareObjects(t.Context(), request)

			c.Assert(err, qt.IsNil)
			c.Assert(result.Counts, qt.DeepEquals, []schemaext.ObjectCount{test.want})
		})
	}
}
