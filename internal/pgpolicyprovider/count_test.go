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

// TestCompareTableStates_CountsTheTablesRowSecurityGuards pins the counts the
// inventory reports for row-level security switches: a table with ENABLE or
// FORCE on is one object, matched where both sides have it on whether or not
// the switches differ, added where the plan switches it on and removed where
// the plan switches it off. A table off on both sides, one the comparison
// keeps for a source that cannot describe it, and one only one side holds are
// counted on neither side.
func TestCompareTableStates_CountsTheTablesRowSecurityGuards(t *testing.T) {
	notDescribed := must.Must(pgpolicy.Coverage(pgpolicy.TableStateKind, schemaext.Desired, schemaext.Knowledge{State: schemaext.Uninspected, Reason: "not read"}, nil))
	described := must.Must(pgpolicy.CompleteCoverage(schemaext.Desired))
	enabled := &pgpolicy.ObservedTableState{Enabled: true}
	tests := []struct {
		name     string
		declared []schemaext.Value
		observed []schemaext.Value
		coverage schemaext.Coverage
		owner    schemaext.ParentState
		want     schemaext.ObjectCount
	}{
		{name: "enabled on both sides", declared: []schemaext.Value{&pgpolicy.DesiredTableState{Enabled: true}}, observed: []schemaext.Value{enabled},
			coverage: described, owner: surviving("orders"), want: schemaext.ObjectCount{Kind: pgpolicy.TableStateChangeKind, Matched: 1}},
		{name: "forced on one side", declared: []schemaext.Value{&pgpolicy.DesiredTableState{Enabled: true, Forced: true}}, observed: []schemaext.Value{enabled},
			coverage: described, owner: surviving("orders"), want: schemaext.ObjectCount{Kind: pgpolicy.TableStateChangeKind, Matched: 1}},
		{name: "switched on", declared: []schemaext.Value{&pgpolicy.DesiredTableState{Enabled: true}},
			coverage: described, owner: surviving("orders"), want: schemaext.ObjectCount{Kind: pgpolicy.TableStateChangeKind, Added: 1}},
		{name: "switched off", observed: []schemaext.Value{enabled},
			coverage: described, owner: surviving("orders"), want: schemaext.ObjectCount{Kind: pgpolicy.TableStateChangeKind, Removed: 1}},
		{name: "force the server kept through a disable, switched off", observed: []schemaext.Value{&pgpolicy.ObservedTableState{Forced: true}},
			coverage: described, owner: surviving("orders"), want: schemaext.ObjectCount{Kind: pgpolicy.TableStateChangeKind, Removed: 1}},
		{name: "off on both sides", declared: []schemaext.Value{&pgpolicy.DesiredTableState{}}, observed: []schemaext.Value{&pgpolicy.ObservedTableState{}},
			coverage: described, owner: surviving("orders"), want: schemaext.ObjectCount{Kind: pgpolicy.TableStateChangeKind}},
		{name: "kept for a source that cannot describe it", observed: []schemaext.Value{enabled},
			coverage: notDescribed, owner: surviving("orders"), want: schemaext.ObjectCount{Kind: pgpolicy.TableStateChangeKind}},
		{name: "a table the plan creates", declared: []schemaext.Value{&pgpolicy.DesiredTableState{Enabled: true}},
			coverage: described, owner: schemaext.ParentState{Subject: tableRef("orders"), Desired: true}, want: schemaext.ObjectCount{Kind: pgpolicy.TableStateChangeKind}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := facetComparison(c, records(c, test.declared...), records(c, test.observed...), test.owner)
			request.Desired.Coverage = test.coverage

			result, err := newRuntime(c).CompareFacets(t.Context(), request)

			c.Assert(err, qt.IsNil)
			c.Assert(result.Counts, qt.DeepEquals, []schemaext.ObjectCount{test.want})
		})
	}
}
