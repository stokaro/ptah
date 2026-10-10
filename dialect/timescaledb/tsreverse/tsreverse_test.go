package tsreverse_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/timescaledb/tsdiff"
	"ptah.run/dialect/timescaledb/tsreverse"
	"ptah.run/dialect/timescaledb/tsschema"
)

var table = objectidentity.NewBuilder(identifier.ForDialect("postgres")).TableParts("", "readings")

// TestReverseChanges_RestoresTheOtherSide pins what a down migration plans
// from each change: the operands swap representation, the forward state is
// what the up step left, and the limitations say what a restore cannot bring
// back.
func TestReverseChanges_RestoresTheOtherSide(t *testing.T) {
	aggregate := tsschema.ContinuousAggregateRef("", "hourly")
	tests := []struct {
		name         string
		record       schemaext.ChangeRecord
		wantChange   schemaext.ChangeValue
		wantForward  schemaext.Value
		wantStrategy string
	}{
		{
			name:         "a table partitioned",
			record:       schemaext.ChangeRecord{Subject: table, Value: &tsdiff.Hypertable{After: &tsschema.DesiredHypertable{Column: "time", ChunkInterval: "1 day"}}},
			wantChange:   &tsdiff.Hypertable{Before: &tsschema.ObservedHypertable{Column: "time", ChunkInterval: "1 day", Dimensions: 1}},
			wantForward:  &tsschema.ObservedHypertable{Column: "time", ChunkInterval: "1 day", Dimensions: 1},
			wantStrategy: "restore the previous partitioning",
		},
		{
			name:         "an aggregate created",
			record:       schemaext.ChangeRecord{Subject: aggregate, Value: &tsdiff.ContinuousAggregate{After: &tsschema.DesiredContinuousAggregate{Body: "SELECT 1", MaterializedOnly: new(true)}}},
			wantChange:   &tsdiff.ContinuousAggregate{Before: &tsschema.ObservedContinuousAggregate{Definition: "SELECT 1", MaterializedOnly: new(true)}},
			wantForward:  &tsschema.ObservedContinuousAggregate{Definition: "SELECT 1", MaterializedOnly: new(true)},
			wantStrategy: "drop the created continuous aggregate",
		},
		{
			name:         "an aggregate dropped",
			record:       schemaext.ChangeRecord{Subject: aggregate, Value: &tsdiff.ContinuousAggregate{Before: &tsschema.ObservedContinuousAggregate{Definition: "SELECT 1;", HypertableName: "readings"}}},
			wantChange:   &tsdiff.ContinuousAggregate{After: &tsschema.DesiredContinuousAggregate{Body: "SELECT 1;"}},
			wantStrategy: "recreate the dropped continuous aggregate from its captured definition",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			reversals, err := tsreverse.Service{}.ReverseChanges(t.Context(), schemaext.ReversalRequest{Target: "postgres", Changes: []schemaext.ChangeRecord{test.record}})

			c.Assert(err, qt.IsNil)
			c.Assert(reversals, qt.HasLen, 1)
			c.Assert(reversals[0].Change.Subject, qt.DeepEquals, test.record.Subject)
			c.Assert(reversals[0].Change.Value, qt.DeepEquals, test.wantChange)
			c.Assert(reversals[0].ForwardState, qt.HasLen, 1)
			c.Assert(reversals[0].ForwardState[0].Value, qt.DeepEquals, test.wantForward)
			c.Assert(reversals[0].Strategy, qt.Equals, test.wantStrategy)
			c.Assert(reversals[0].Limitations, qt.Not(qt.HasLen), 0)
		})
	}
}

// TestReverseChanges_FailurePath pins the refusals: another target family, a
// change of another owner, and operands that are not a change.
func TestReverseChanges_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		request schemaext.ReversalRequest
		want    error
	}{
		{name: "another target", request: schemaext.ReversalRequest{Target: "sqlite"}, want: ptaherr.ErrUnsupportedDialect},
		{name: "an empty change", request: schemaext.ReversalRequest{Target: "postgres", Changes: []schemaext.ChangeRecord{{Subject: table, Value: &tsdiff.Hypertable{}}}}, want: schemaext.ErrInvalidValue},
		{name: "an aggregate named as a table", request: schemaext.ReversalRequest{Target: "postgres", Changes: []schemaext.ChangeRecord{{Subject: table,
			Value: &tsdiff.ContinuousAggregate{After: &tsschema.DesiredContinuousAggregate{Body: "SELECT 1"}}}}}, want: schemaext.ErrInvalidValue},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			reversals, err := tsreverse.Service{}.ReverseChanges(t.Context(), test.request)

			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(reversals, qt.IsNil)
		})
	}
}
