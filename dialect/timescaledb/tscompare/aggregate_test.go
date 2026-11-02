package tscompare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/timescaledb/tscompare"
	"ptah.run/dialect/timescaledb/tsdiff"
	"ptah.run/dialect/timescaledb/tsschema"
	"ptah.run/engine"
)

// aggregateRuntime selects the continuous aggregate owner on PostgreSQL and
// nothing else, so the engine checks each count the owner states against its
// changes.
func aggregateRuntime(c *qt.C) *engine.Runtime {
	c.Helper()
	return must.Must(engine.New(engine.Provider{ID: tsschema.Owner, Targets: []engine.Target{{Name: platform.Postgres}},
		Codecs: append(tsschema.Codecs(), tsdiff.Codecs()...),
		Comparisons: []engine.ObjectComparison{{Target: platform.Postgres, Kinds: []schemaext.Kind{tsschema.ContinuousAggregateKind},
			ChangeKinds: []schemaext.Kind{tsdiff.ContinuousAggregateKind}, Service: tscompare.AggregateService{}}},
	}))
}

// aggregateState is a side holding objects, with knowledge as its claim about
// every aggregate except the ones subjects names.
func aggregateState(representation schemaext.Representation, knowledge schemaext.Knowledge, subjects []schemaext.SubjectCoverage,
	objects ...schemaext.Object,
) schemaext.ObjectState {
	return schemaext.ObjectState{Objects: must.Must(schemaext.NewObjects(objects...)),
		Coverage: must.Must(tsschema.Coverage(tsschema.ContinuousAggregateKind, representation, knowledge, subjects))}
}

// declaredAggregate and heldAggregate are the aggregate app.name, declared
// and read with the option the server reports.
func declaredAggregate(name string, materializedOnly *bool) schemaext.Object {
	return tsschema.DesiredContinuousAggregateObject("app", name, tsschema.DesiredContinuousAggregate{Body: "SELECT 1", MaterializedOnly: materializedOnly})
}

func heldAggregate(name string) schemaext.Object {
	return tsschema.ObservedContinuousAggregateObject("app", name, tsschema.ObservedContinuousAggregate{Definition: "SELECT 1;",
		MaterializedOnly: new(true), HypertableSchema: "app", HypertableName: "readings"})
}

// TestAggregateComparison_CountsWhatItCompares pins the counts the inventory
// reports for continuous aggregates: an aggregate both sides hold is matched
// whether or not it differs, a replacement included, a creation is added and a
// drop removed, and an aggregate the comparison keeps or cannot decide is
// counted on neither side, so a server that agrees counts what the declaration
// does.
func TestAggregateComparison_CountsWhatItCompares(t *testing.T) {
	complete := schemaext.Knowledge{State: schemaext.Complete}
	limited := []schemaext.SubjectCoverage{{Kind: tsschema.ContinuousAggregateKind, Subject: tsschema.ContinuousAggregateRef("app", "hourly"),
		Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "the test says so"}}}
	tests := []struct {
		name             string
		desired, current schemaext.ObjectState
		want             schemaext.ObjectCount
		changes          int
		undecided        int
	}{
		{name: "an aggregate that agrees", want: schemaext.ObjectCount{Kind: tsdiff.ContinuousAggregateKind, Matched: 1},
			desired: aggregateState(schemaext.Desired, complete, nil, declaredAggregate("hourly", nil)),
			current: aggregateState(schemaext.Observed, complete, nil, heldAggregate("hourly"))},
		{name: "an aggregate replaced for its option", want: schemaext.ObjectCount{Kind: tsdiff.ContinuousAggregateKind, Matched: 1}, changes: 1,
			desired: aggregateState(schemaext.Desired, complete, nil, declaredAggregate("hourly", new(false))),
			current: aggregateState(schemaext.Observed, complete, nil, heldAggregate("hourly"))},
		{name: "a creation, a drop and a match", want: schemaext.ObjectCount{Kind: tsdiff.ContinuousAggregateKind, Matched: 1, Added: 1, Removed: 1},
			changes: 2,
			desired: aggregateState(schemaext.Desired, complete, nil, declaredAggregate("hourly", nil), declaredAggregate("daily", nil)),
			current: aggregateState(schemaext.Observed, complete, nil, heldAggregate("hourly"), heldAggregate("weekly"))},
		{name: "an aggregate the desired source did not describe", want: schemaext.ObjectCount{Kind: tsdiff.ContinuousAggregateKind},
			desired: aggregateState(schemaext.Desired, schemaext.Knowledge{State: schemaext.Uninspected, Reason: "the test says so"}, nil),
			current: aggregateState(schemaext.Observed, complete, nil, heldAggregate("hourly"))},
		{name: "a declaration the source could not describe completely", want: schemaext.ObjectCount{Kind: tsdiff.ContinuousAggregateKind},
			undecided: 1,
			desired:   aggregateState(schemaext.Desired, complete, limited, declaredAggregate("hourly", nil)),
			current:   aggregateState(schemaext.Observed, complete, nil, heldAggregate("hourly"))},
		{name: "a declaration on a server whose aggregates were not read", want: schemaext.ObjectCount{Kind: tsdiff.ContinuousAggregateKind},
			undecided: 1,
			desired:   aggregateState(schemaext.Desired, complete, nil, declaredAggregate("hourly", nil)),
			current:   aggregateState(schemaext.Observed, schemaext.Knowledge{State: schemaext.Uninspected, Reason: "the test says so"}, nil)},
		{name: "a held aggregate the read could not describe", want: schemaext.ObjectCount{Kind: tsdiff.ContinuousAggregateKind},
			desired: aggregateState(schemaext.Desired, complete, nil),
			current: aggregateState(schemaext.Observed, complete, limited, heldAggregate("hourly"))},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := aggregateRuntime(c).CompareObjects(t.Context(), schemaext.ObjectComparisonRequest{
				Target: platform.Postgres, Identifiers: identifier.ForDialect(platform.Postgres), Desired: test.desired, Current: test.current})

			c.Assert(err, qt.IsNil)
			c.Assert(result.Changes, qt.HasLen, test.changes)
			c.Assert(result.Undecided, qt.HasLen, test.undecided)
			c.Assert(result.Counts, qt.DeepEquals, []schemaext.ObjectCount{test.want})
		})
	}
}
