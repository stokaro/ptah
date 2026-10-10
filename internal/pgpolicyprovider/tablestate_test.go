package pgpolicyprovider_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgpolicy"
	"ptah.run/feature/pgpolicy/policycompare"
)

// records attaches values to the orders table, or nothing for no values.
func records(c *qt.C, values ...schemaext.Value) []schemaext.FacetRecord {
	c.Helper()
	var result []schemaext.FacetRecord
	for _, value := range values {
		result = append(result, schemaext.FacetRecord{Subject: tableRef("orders"), Values: must.Must(schemaext.NewFacets(value))})
	}
	return result
}

// facetComparison compares the orders table on postgres with both sides
// complete unless a test replaces a coverage.
func facetComparison(c *qt.C, desired, current []schemaext.FacetRecord, owner schemaext.ParentState) schemaext.FacetComparisonRequest {
	c.Helper()
	return schemaext.FacetComparisonRequest{
		Target: "postgres", Identifiers: postgres,
		Desired: schemaext.FacetState{Records: desired, Coverage: complete(c, schemaext.Desired)},
		Current: schemaext.FacetState{Records: current, Coverage: complete(c, schemaext.Observed)},
		Owners:  []schemaext.ParentState{owner},
	}
}

// TestCompareTableStates_FindsChanges pins each switch change on a surviving
// table, with the access it can change. A table the description does not
// enable, where the description covers row security, has both switches off,
// and FORCE the server kept through a DISABLE compares like any other value.
func TestCompareTableStates_FindsChanges(t *testing.T) {
	off := pgpolicy.ObservedTableState{}
	enabled, forced := pgpolicy.ObservedTableState{Enabled: true}, pgpolicy.ObservedTableState{Enabled: true, Forced: true}
	keptForce := pgpolicy.ObservedTableState{Forced: true}
	tests := []struct {
		name     string
		declared []schemaext.Value
		observed []schemaext.Value
		want     *pgpolicy.TableStateChange
	}{
		{name: "enabled", declared: []schemaext.Value{&pgpolicy.DesiredTableState{Enabled: true}},
			want: &pgpolicy.TableStateChange{Before: &off, After: &pgpolicy.DesiredTableState{Enabled: true},
				Access: schemaext.AccessEffect{Access: schemaext.AccessNarrows, Reason: "enabling row security hides every row no policy admits"}}},
		{name: "forced", declared: []schemaext.Value{&pgpolicy.DesiredTableState{Enabled: true, Forced: true}}, observed: []schemaext.Value{&enabled},
			want: &pgpolicy.TableStateChange{Before: &enabled, After: &pgpolicy.DesiredTableState{Enabled: true, Forced: true},
				Access: schemaext.AccessEffect{Access: schemaext.AccessNarrows, Reason: "FORCE ROW LEVEL SECURITY subjects the table's owner to its policies"}}},
		{name: "no longer forced", declared: []schemaext.Value{&pgpolicy.DesiredTableState{Enabled: true}}, observed: []schemaext.Value{&forced},
			want: &pgpolicy.TableStateChange{Before: &forced, After: &pgpolicy.DesiredTableState{Enabled: true},
				Access: schemaext.AccessEffect{Access: schemaext.AccessWidens, Reason: "NO FORCE ROW LEVEL SECURITY exempts the table's owner from its policies"}}},
		{name: "disabled by a description that does not enable it", observed: []schemaext.Value{&enabled},
			want: &pgpolicy.TableStateChange{Before: &enabled, After: &pgpolicy.DesiredTableState{},
				Access: schemaext.AccessEffect{Access: schemaext.AccessWidens, Reason: "disabling row security reveals every row its policies hid"}}},
		{name: "enabled again without the FORCE the server kept", declared: []schemaext.Value{&pgpolicy.DesiredTableState{Enabled: true}},
			observed: []schemaext.Value{&keptForce},
			want: &pgpolicy.TableStateChange{Before: &keptForce, After: &pgpolicy.DesiredTableState{Enabled: true},
				Access: schemaext.AccessEffect{Access: schemaext.AccessNarrows, Reason: "enabling row security hides every row no policy admits"}}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := facetComparison(c, records(c, test.declared...), records(c, test.observed...), surviving("orders"))

			result, err := newRuntime(c).CompareFacets(t.Context(), request)

			c.Assert(err, qt.IsNil)
			c.Assert(result.Changes, qt.DeepEquals, []schemaext.FacetChange{{Kind: pgpolicy.TableStateKind,
				Change: schemaext.ChangeRecord{Subject: tableRef("orders"), Value: test.want}}})
			c.Assert(result.Undecided, qt.HasLen, 0)
		})
	}
}

// TestCompareTableStates_NoChange pins the states that ask for what the server
// holds, the lifecycle a table's creation and removal own, and a source that
// cannot describe the switches, which keeps what the server holds.
func TestCompareTableStates_NoChange(t *testing.T) {
	tests := []struct {
		name     string
		declared []schemaext.Value
		observed []schemaext.Value
		owner    schemaext.ParentState
	}{
		{name: "the same switches", declared: []schemaext.Value{&pgpolicy.DesiredTableState{Enabled: true, Forced: true}},
			observed: []schemaext.Value{&pgpolicy.ObservedTableState{Enabled: true, Forced: true}}, owner: surviving("orders")},
		{name: "both off on both sides", declared: []schemaext.Value{&pgpolicy.DesiredTableState{}}, owner: surviving("orders")},
		{name: "a comment and a struct name", declared: []schemaext.Value{&pgpolicy.DesiredTableState{Enabled: true, Comment: "c", StructName: "Order"}},
			observed: []schemaext.Value{&pgpolicy.ObservedTableState{Enabled: true}}, owner: surviving("orders")},
		{name: "a table the plan creates", declared: []schemaext.Value{&pgpolicy.DesiredTableState{Enabled: true}},
			owner: schemaext.ParentState{Subject: tableRef("orders"), Desired: true}},
		{name: "a table the plan drops", observed: []schemaext.Value{&pgpolicy.ObservedTableState{Enabled: true}},
			owner: schemaext.ParentState{Subject: tableRef("orders"), Current: true}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := newRuntime(c).CompareFacets(t.Context(), facetComparison(c, records(c, test.declared...), records(c, test.observed...), test.owner))

			c.Assert(err, qt.IsNil)
			c.Assert(result.Changes, qt.HasLen, 0)
			c.Assert(result.Undecided, qt.HasLen, 0)
		})
	}
}

// TestCompareTableStates_KeepsWhatNobodyDescribed pins that a source which
// cannot describe the switches leaves a secured table secured: the switches
// the server holds become the effective declaration, and nothing is disabled.
func TestCompareTableStates_KeepsWhatNobodyDescribed(t *testing.T) {
	c := qt.New(t)
	request := facetComparison(c, nil, records(c, &pgpolicy.ObservedTableState{Enabled: true, Forced: true}), surviving("orders"))
	request.Desired.Coverage = must.Must(pgpolicy.Coverage(pgpolicy.TableStateKind, schemaext.Desired,
		schemaext.Knowledge{State: schemaext.Uninspected, Reason: "not read"}, nil))

	result, err := newRuntime(c).CompareFacets(t.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Changes, qt.HasLen, 0)
	c.Assert(result.Undecided, qt.HasLen, 0)
	c.Assert(result.Desired.Records, qt.HasLen, 1)
	adopted, found := must.Must2(schemaext.FacetAs[*pgpolicy.DesiredTableState](result.Desired.Records[0].Values, pgpolicy.TableStateKind))
	c.Assert(found, qt.IsTrue)
	c.Assert(adopted, qt.DeepEquals, &pgpolicy.DesiredTableState{Enabled: true, Forced: true})
}

// TestCompareTableStates_LeavesAnExcludedTableAlone pins a declaration scoped
// to another target: on this one the table's switches are excluded from the
// comparison, so a read that skipped the switches leaves only the other table
// in the batch undecided.
func TestCompareTableStates_LeavesAnExcludedTableAlone(t *testing.T) {
	c := qt.New(t)
	scoped := must.Must(must.Must(schemaext.NewFacets(&pgpolicy.DesiredTableState{Enabled: true})).WithTargetScope(pgpolicy.TableStateKind, "cockroachdb"))
	invoices := must.Must(schemaext.NewFacets(&pgpolicy.DesiredTableState{Enabled: true}))
	request := facetComparison(c, []schemaext.FacetRecord{{Subject: tableRef("orders"), Values: scoped}, {Subject: tableRef("invoices"), Values: invoices}},
		nil, surviving("orders"))
	request.Owners = append(request.Owners, surviving("invoices"))
	request.Current.Coverage = must.Must(pgpolicy.Coverage(pgpolicy.TableStateKind, schemaext.Observed,
		schemaext.Knowledge{State: schemaext.Uninspected, Reason: "not read"}, nil))

	result, err := newRuntime(c).CompareFacets(t.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Changes, qt.HasLen, 0)
	c.Assert(result.Undecided, qt.DeepEquals, []schemaext.UndecidedChange{{Kind: pgpolicy.TableStateKind, Subject: tableRef("invoices"),
		Reason: "this table's row-security switches were not read: not read"}})
}

// TestCompareTableStates_UndecidedWhereNothingWasRead pins a declaration whose
// table's switches the read did not establish, and one the desired source
// could not describe in full.
func TestCompareTableStates_UndecidedWhereNothingWasRead(t *testing.T) {
	tests := []struct {
		name    string
		desired schemaext.Coverage
		current schemaext.Coverage
		want    string
	}{
		{name: "a read that skipped the switches", desired: must.Must(pgpolicy.CompleteCoverage(schemaext.Desired)),
			current: must.Must(pgpolicy.Coverage(pgpolicy.TableStateKind, schemaext.Observed, schemaext.Knowledge{State: schemaext.Uninspected, Reason: "not read"}, nil)),
			want:    "this table's row-security switches were not read: not read"},
		{name: "a declaration the source could not describe in full", desired: must.Must(pgpolicy.Coverage(pgpolicy.TableStateKind, schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete},
			[]schemaext.SubjectCoverage{{Kind: pgpolicy.TableStateKind, Subject: tableRef("orders"),
				Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "a switch it cannot spell"}}})),
			current: must.Must(pgpolicy.CompleteCoverage(schemaext.Observed)),
			want:    "the desired source could not describe this table's row-security switches"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := facetComparison(c, records(c, &pgpolicy.DesiredTableState{Enabled: true}), nil, surviving("orders"))
			request.Desired.Coverage, request.Current.Coverage = test.desired, test.current

			result, err := newRuntime(c).CompareFacets(t.Context(), request)

			c.Assert(err, qt.IsNil)
			c.Assert(result.Changes, qt.HasLen, 0)
			c.Assert(result.Undecided, qt.DeepEquals, []schemaext.UndecidedChange{{Kind: pgpolicy.TableStateKind, Subject: tableRef("orders"), Reason: test.want}})
		})
	}
}

// TestCompareTableStates_OnlyOnRowSecurityTargets pins that the owner claims
// no target without row security.
func TestCompareTableStates_OnlyOnRowSecurityTargets(t *testing.T) {
	c := qt.New(t)
	request := facetComparison(c, records(c, &pgpolicy.DesiredTableState{Enabled: true}), nil, surviving("orders"))
	request.Target = "spanner"
	request.Desired.Coverage, request.Current.Coverage = schemaext.Coverage{}, schemaext.Coverage{}

	result, err := newRuntime(c).CompareFacets(t.Context(), request)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(result.Changes, qt.HasLen, 0)

	request.Target, request.Kinds = "mysql", []schemaext.Kind{pgpolicy.TableStateKind}
	direct, err := policycompare.TableStateService{}.CompareFacets(t.Context(), request)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedDialect)
	c.Assert(direct.Changes, qt.HasLen, 0)
}

// defaultedCoverage is complete desired coverage of the switches, with the
// orders table's switches left to the owner's default.
func defaultedCoverage(c *qt.C) schemaext.Coverage {
	c.Helper()
	return must.Must(pgpolicy.Coverage(pgpolicy.TableStateKind, schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete},
		[]schemaext.SubjectCoverage{{Kind: pgpolicy.TableStateKind, Subject: tableRef("orders"), Knowledge: pgpolicy.DefaultedSwitches()}}))
}

// TestCompareTableStates_KeepsDefaultedSwitches pins stokaro/ptah#2048: a
// declaration that names a table's policies and not its switches neither
// enables nor disables the table, whatever the switches are, and a table whose
// switches were not read is not left undecided.
func TestCompareTableStates_KeepsDefaultedSwitches(t *testing.T) {
	tests := []struct {
		name     string
		observed []schemaext.Value
		adopted  []schemaext.Value
	}{
		{name: "row security on", observed: []schemaext.Value{&pgpolicy.ObservedTableState{Enabled: true, Forced: true}},
			adopted: []schemaext.Value{&pgpolicy.DesiredTableState{Enabled: true, Forced: true}}},
		{name: "row security off", adopted: []schemaext.Value{&pgpolicy.DesiredTableState{}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := facetComparison(c, nil, records(c, test.observed...), surviving("orders"))
			request.Desired.Coverage = defaultedCoverage(c)

			result, err := newRuntime(c).CompareFacets(t.Context(), request)

			c.Assert(err, qt.IsNil)
			c.Assert(result.Changes, qt.HasLen, 0)
			c.Assert(result.Undecided, qt.HasLen, 0)
			c.Assert(result.Desired.Records, qt.DeepEquals, records(c, test.adopted...))
		})
	}
}

// TestCompareTableStates_KeepsDefaultedSwitchesNobodyRead pins a defaulted
// table whose switches the read did not establish: there is nothing to keep
// and nothing to decide.
func TestCompareTableStates_KeepsDefaultedSwitchesNobodyRead(t *testing.T) {
	c := qt.New(t)
	request := facetComparison(c, nil, nil, surviving("orders"))
	request.Desired.Coverage = defaultedCoverage(c)
	request.Current.Coverage = must.Must(pgpolicy.Coverage(pgpolicy.TableStateKind, schemaext.Observed,
		schemaext.Knowledge{State: schemaext.Uninspected, Reason: "not read"}, nil))

	result, err := newRuntime(c).CompareFacets(t.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Changes, qt.HasLen, 0)
	c.Assert(result.Undecided, qt.HasLen, 0)
}

// TestCompareTableStates_DeclaredSwitchesOverrideTheDefault pins that a switch
// declared elsewhere for a table whose policies another declaration left
// defaulted is compared as declared.
func TestCompareTableStates_DeclaredSwitchesOverrideTheDefault(t *testing.T) {
	c := qt.New(t)
	request := facetComparison(c, records(c, &pgpolicy.DesiredTableState{}), records(c, &pgpolicy.ObservedTableState{Enabled: true}), surviving("orders"))
	request.Desired.Coverage = defaultedCoverage(c)

	result, err := newRuntime(c).CompareFacets(t.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Changes, qt.HasLen, 1)
	c.Assert(result.Changes[0].Change.Value.(*pgpolicy.TableStateChange).After, qt.DeepEquals, &pgpolicy.DesiredTableState{})
}
