package chcompare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chcompare"
	"ptah.run/dialect/clickhouse/chdiff"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/engine"
)

func refreshRuntime() *engine.Runtime {
	return must.Must(engine.New(engine.Provider{
		ID: "ptah.run/clickhouse", Targets: []engine.Target{{Name: "clickhouse"}},
		Codecs: append(chschema.RefreshCodecs(), chdiff.RefreshCodecs()...),
		FacetComparisons: []engine.FacetComparison{{Target: "clickhouse", OwnerKinds: []objectidentity.Kind{objectidentity.KindMatView},
			Kinds: []schemaext.Kind{chschema.RefreshKind}, ChangeKinds: []schemaext.Kind{chdiff.RefreshKind}, Service: chcompare.RefreshService{}}},
	}))
}

func every(interval string) *chschema.Schedule {
	return &chschema.Schedule{Mode: chschema.RefreshEvery, Interval: interval}
}

// refreshRequest compares one view that both sides hold. A nil schedule is a
// view without one; desiredState and currentState are each side's coverage of
// the kind, and schemaext.Uninspected records no coverage.
func refreshRequest(declared, stored *chschema.Schedule, desiredState, currentState schemaext.KnowledgeState) schemaext.FacetComparisonRequest {
	builder := objectidentity.NewBuilder(identifier.ForDialect("clickhouse"))
	desiredRef := builder.SchemaScopedParts(objectidentity.KindMatView, "", "mv")
	request := schemaext.FacetComparisonRequest{
		Target: "clickhouse", Kinds: []schemaext.Kind{chschema.RefreshKind},
		Owners: []schemaext.ParentState{{Subject: desiredRef, Desired: true, Current: true}},
	}
	if declared != nil {
		request.Desired.Records = []schemaext.FacetRecord{{Subject: desiredRef, Values: must.Must(schemaext.NewFacets(&chschema.DesiredRefresh{Schedule: *declared}))}}
	}
	if stored != nil {
		request.Current.Records = []schemaext.FacetRecord{{Subject: desiredRef, Values: must.Must(schemaext.NewFacets(&chschema.ObservedRefresh{Schedule: *stored}))}}
	}
	if desiredState != schemaext.Uninspected {
		request.Desired.Coverage = must.Must(chschema.RefreshCoverage(schemaext.Desired, schemaext.Knowledge{State: desiredState}, nil))
	}
	if currentState != schemaext.Uninspected {
		request.Current.Coverage = must.Must(chschema.RefreshCoverage(schemaext.Observed, schemaext.Knowledge{State: currentState}, nil))
	}
	return request
}

// TestRefreshComparison_ReadsTheDeclarationTheWayTheServerStoresIt is the
// reason internal/chrefresh exists at all.
//
// ClickHouse rewrites what it stores: a view created with `EVERY 60 MINUTE`
// reads back as `EVERY 1 HOUR`. Comparing the declaration as written would
// report a difference on every single run and plan a replacement for it, on an
// object whose drop takes every row it accumulated (stokaro/ptah#1802). A
// dependency is stored qualified by the view's database, so an unqualified one
// is read that way too.
//
// Every row here is a spelling measured to store as the stored schedule, so a
// comparison that skipped the canonicalization would fail all of them.
func TestRefreshComparison_ReadsTheDeclarationTheWayTheServerStoresIt(t *testing.T) {
	tests := []struct {
		name     string
		declared *chschema.Schedule
		stored   *chschema.Schedule
	}{
		{name: "already canonical", declared: every("1 HOUR"), stored: every("1 HOUR")},
		{name: "minutes", declared: every("60 MINUTE"), stored: every("1 HOUR")},
		{name: "seconds", declared: every("3600 SECOND"), stored: every("1 HOUR")},
		{name: "plural spelling", declared: every("60 MINUTES"), stored: every("1 HOUR")},
		{
			name:     "an unqualified dependency",
			declared: &chschema.Schedule{Mode: "EVERY", Interval: "1 HOUR", DependsOn: []string{"source"}},
			stored:   &chschema.Schedule{Mode: "EVERY", Interval: "1 HOUR", DependsOn: []string{"analytics.source"}},
		},
		{
			// Measured on 24.10.4.191: MODIFY REFRESH stores a dependency as
			// it was written, where CREATE qualifies it.
			name:     "a dependency the server stored unqualified",
			declared: &chschema.Schedule{Mode: "EVERY", Interval: "1 HOUR", DependsOn: []string{"analytics.source"}},
			stored:   &chschema.Schedule{Mode: "EVERY", Interval: "1 HOUR", DependsOn: []string{"source"}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := refreshRequest(test.declared, test.stored, schemaext.Complete, schemaext.Complete)
			stored := objectidentity.NewBuilder(identifier.ForDialect("clickhouse")).SchemaScopedParts(objectidentity.KindMatView, "analytics", "mv")
			request.Current.Records[0].Subject = stored
			request.Owners[0].Subject = stored
			request.Desired.Records[0].Subject = stored
			result, err := refreshRuntime().CompareFacets(t.Context(), request)
			c.Assert(err, qt.IsNil)
			c.Assert(result.Undecided, qt.HasLen, 0)
			c.Assert(result.Changes, qt.HasLen, 0)
		})
	}
}

// TestRefreshComparison_ReportsARealScheduleChange is the control: the
// canonicalization must not make every schedule compare equal. A schedule
// gained or lost reports that it replaces the view, which is how the host knows
// to drop and recreate it; a schedule changed to another is applied in place.
func TestRefreshComparison_ReportsARealScheduleChange(t *testing.T) {
	tests := []struct {
		name     string
		declared *chschema.Schedule
		stored   *chschema.Schedule
		replaces bool
	}{
		{name: "a different interval", declared: every("2 HOUR"), stored: every("1 HOUR")},
		{
			// Same interval, different meaning: EVERY is wall-clock and AFTER
			// counts from the end of the previous run.
			name:     "a different mode",
			declared: &chschema.Schedule{Mode: "AFTER", Interval: "1 HOUR"},
			stored:   every("1 HOUR"),
		},
		{name: "a schedule added to a plain view", declared: every("1 HOUR"), replaces: true},
		{name: "a schedule removed", stored: every("1 HOUR"), replaces: true},
		{
			// Measured on 24.10.4.191 and 26.9.14.10: MODIFY REFRESH refuses
			// to add or remove APPEND.
			name:     "APPEND added",
			declared: &chschema.Schedule{Mode: "EVERY", Interval: "1 HOUR", Append: true},
			stored:   every("1 HOUR"),
			replaces: true,
		},
		{
			name:     "a dependency added",
			declared: &chschema.Schedule{Mode: "EVERY", Interval: "1 HOUR", DependsOn: []string{"other"}},
			stored:   every("1 HOUR"),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			result, err := refreshRuntime().CompareFacets(t.Context(), refreshRequest(test.declared, test.stored, schemaext.Complete, schemaext.Complete))
			c.Assert(err, qt.IsNil)
			c.Assert(result.Undecided, qt.HasLen, 0)
			c.Assert(result.Changes, qt.HasLen, 1)
			change := result.Changes[0].Change.Value.(*chdiff.Refresh)
			c.Assert(change.After != nil, qt.Equals, test.declared != nil)
			c.Assert(change.Before != nil, qt.Equals, test.stored != nil)
			c.Assert(change.ReplacesOwner(), qt.Equals, test.replaces)
		})
	}
}

// A source that cannot state a schedule leaves the server's unmanaged: the
// observed schedule is adopted into the effective declaration, so a later
// replacement keeps it, and nothing is planned. A source that states schedules
// declares a plain view by leaving it out, which removes the server's.
func TestRefreshComparison_AnOmittedScheduleFollowsTheSourceCoverage(t *testing.T) {
	tests := []struct {
		name     string
		coverage schemaext.KnowledgeState
		changes  int
		adopted  bool
	}{
		{name: "no coverage adopts the server's schedule", adopted: true},
		{name: "complete coverage declares no schedule", coverage: schemaext.Complete, changes: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := refreshRequest(nil, every("1 HOUR"), test.coverage, schemaext.Complete)
			result, err := refreshRuntime().CompareFacets(t.Context(), request)
			c.Assert(err, qt.IsNil)
			c.Assert(result.Changes, qt.HasLen, test.changes)
			c.Assert(len(result.Desired.Records) == 1, qt.Equals, test.adopted)
			c.Assert(request.Desired.Records, qt.HasLen, 0)
		})
	}
}

// A schedule the reader could not read is unknown, not absent, so the
// comparison cannot decide whether the declaration matches it, and plans
// nothing that would replace the view.
func TestRefreshComparison_AnUnreadScheduleIsUndecided(t *testing.T) {
	for _, declared := range []*chschema.Schedule{nil, every("1 HOUR")} {
		c := qt.New(t)
		request := refreshRequest(declared, nil, schemaext.Complete, schemaext.Complete)
		request.Current.Coverage = must.Must(chschema.RefreshCoverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete},
			[]schemaext.SubjectCoverage{{Kind: chschema.RefreshKind, Subject: request.Owners[0].Subject, Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "unreadable"}}}))
		result, err := refreshRuntime().CompareFacets(t.Context(), request)
		c.Assert(err, qt.IsNil)
		c.Assert(result.Changes, qt.HasLen, 0)
		c.Assert(result.Undecided, qt.HasLen, 1)
	}
}

// A current side that did not inspect schedules states none: a document read
// as the current state, or an account that may not read the catalog of
// schedules. A declaration of a plain view asks for nothing a plan could do
// there, so it is neither a change nor undecided; one declaring a schedule
// cannot be checked, so it is undecided (stokaro/ptah#4278).
func TestRefreshComparison_AnUninspectedSideStatesNoSchedule(t *testing.T) {
	builder := objectidentity.NewBuilder(identifier.ForDialect("clickhouse"))
	view := builder.SchemaScopedParts(objectidentity.KindMatView, "", "mv")
	readLimit := must.Must(chschema.RefreshCoverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete},
		[]schemaext.SubjectCoverage{{Kind: chschema.RefreshKind, Subject: view, Knowledge: schemaext.Knowledge{State: schemaext.Uninspected, Reason: "not read"}}}))
	tests := []struct {
		name      string
		declared  *chschema.Schedule
		current   schemaext.Coverage
		undecided int
	}{
		{name: "a plain declaration against a side with no coverage"},
		{name: "a plain declaration against a view the read did not inspect", current: readLimit},
		{name: "a schedule against a side with no coverage", declared: every("1 HOUR"), undecided: 1},
		{name: "a schedule against a view the read did not inspect", declared: every("1 HOUR"), current: readLimit, undecided: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := refreshRequest(test.declared, nil, schemaext.Complete, schemaext.Uninspected)
			request.Current.Coverage = test.current

			result, err := refreshRuntime().CompareFacets(t.Context(), request)

			c.Assert(err, qt.IsNil)
			c.Assert(result.Changes, qt.HasLen, 0)
			c.Assert(result.Undecided, qt.HasLen, test.undecided)
		})
	}
}

// A view only one side holds is created with its declared schedule or removed
// with its own, so the comparison reports no schedule change for it.
func TestRefreshComparison_LeavesNewAndRemovedViewsToTheirLifecycle(t *testing.T) {
	tests := []struct {
		name     string
		declared *chschema.Schedule
		stored   *chschema.Schedule
		owner    schemaext.ParentState
	}{
		{name: "a new view", declared: every("1 HOUR"), owner: schemaext.ParentState{Desired: true}},
		{name: "a removed view", stored: every("2 HOUR"), owner: schemaext.ParentState{Current: true}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := refreshRequest(test.declared, test.stored, schemaext.Complete, schemaext.Complete)
			test.owner.Subject = request.Owners[0].Subject
			request.Owners[0] = test.owner
			result, err := refreshRuntime().CompareFacets(t.Context(), request)
			c.Assert(err, qt.IsNil)
			c.Assert(result.Changes, qt.HasLen, 0)
			c.Assert(result.Undecided, qt.HasLen, 0)
		})
	}
}
