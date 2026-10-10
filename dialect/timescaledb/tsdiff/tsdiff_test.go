package tsdiff_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/timescaledb/tsdiff"
	"ptah.run/dialect/timescaledb/tsschema"
	"ptah.run/engine"
)

var (
	ordinary    = &tsschema.ObservedHypertable{Column: "time", ChunkInterval: "7 days", Dimensions: 1}
	partitioned = &tsschema.DesiredHypertable{Column: "time", ChunkInterval: "1 day"}
	stored      = &tsschema.ObservedContinuousAggregate{Definition: "SELECT 1;", MaterializedOnly: new(false)}
	declared    = &tsschema.DesiredContinuousAggregate{Body: "SELECT 2", MaterializedOnly: new(true)}
)

// TestEffect_NamesWhatEachTransitionCosts pins the safety effect each change
// carries, which is what a plan's impact report and a lint rule read. A
// replaced aggregate is destructive: it is created WITH NO DATA, so its
// materialized history is gone until a refresh.
func TestEffect_NamesWhatEachTransitionCosts(t *testing.T) {
	tests := []struct {
		name   string
		change schemaext.ChangeValue
		want   schemaext.Impact
	}{
		{name: "a table partitioned", change: &tsdiff.Hypertable{After: partitioned}, want: schemaext.Behavioral},
		{name: "a hypertable left ordinary", change: &tsdiff.Hypertable{Before: ordinary}, want: schemaext.Destructive},
		{name: "a hypertable repartitioned", change: &tsdiff.Hypertable{Before: ordinary, After: partitioned}, want: schemaext.Behavioral},
		{name: "an aggregate created", change: &tsdiff.ContinuousAggregate{After: declared}, want: schemaext.Additive},
		{name: "an aggregate dropped", change: &tsdiff.ContinuousAggregate{Before: stored}, want: schemaext.Destructive},
		{name: "an aggregate replaced", change: &tsdiff.ContinuousAggregate{Before: stored, After: declared}, want: schemaext.Destructive},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			effect := test.change.(interface{ Effect() schemaext.Effect }).Effect()
			c.Assert(effect.Impact, qt.Equals, test.want)
			c.Assert(effect.Reason, qt.Not(qt.Equals), "")
		})
	}
}

// TestValidate_RefusesAChangeThatChangesNothing pins the operand rules: a
// change names at least one side, each side is valid, and a hypertable pair
// that agrees is not a change.
func TestValidate_RefusesAChangeThatChangesNothing(t *testing.T) {
	tests := []struct {
		name   string
		change interface{ Validate() error }
	}{
		{name: "an empty hypertable change", change: &tsdiff.Hypertable{}},
		{name: "a nil hypertable change", change: (*tsdiff.Hypertable)(nil)},
		{name: "a hypertable pair that agrees", change: &tsdiff.Hypertable{Before: ordinary, After: &tsschema.DesiredHypertable{Column: "TIME"}}},
		{name: "a hypertable declaring no column", change: &tsdiff.Hypertable{After: &tsschema.DesiredHypertable{}}},
		{name: "an empty aggregate change", change: &tsdiff.ContinuousAggregate{}},
		{name: "an aggregate declaring no body", change: &tsdiff.ContinuousAggregate{After: &tsschema.DesiredContinuousAggregate{}}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.change.Validate(), qt.ErrorIs, schemaext.ErrInvalidValue)
		})
	}
}

// TestCodecs_RoundTripBothOperands pins the change wire: both keys are always
// present, null is an absent side, and the operands keep their own models.
func TestCodecs_RoundTripBothOperands(t *testing.T) {
	tests := []struct {
		name   string
		change schemaext.ChangeValue
	}{
		{name: "a table partitioned", change: &tsdiff.Hypertable{After: partitioned}},
		{name: "a hypertable repartitioned", change: &tsdiff.Hypertable{Before: ordinary, After: partitioned}},
		{name: "an aggregate dropped", change: &tsdiff.ContinuousAggregate{Before: stored}},
		{name: "an aggregate replaced", change: &tsdiff.ContinuousAggregate{Before: stored, After: declared}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			codecs := must.Must(engine.New(engine.Provider{ID: tsschema.Owner, Codecs: append(tsschema.Codecs(), tsdiff.Codecs()...)})).Codecs()

			data, err := codecs.Marshal(t.Context(), schemaext.Change, []schemaext.Payload{test.change})
			c.Assert(err, qt.IsNil)
			decoded, err := codecs.Unmarshal(t.Context(), data)

			c.Assert(err, qt.IsNil)
			c.Assert(decoded, qt.DeepEquals, []schemaext.Payload{test.change})
		})
	}
}

// TestCodecs_RefuseAPartialChange pins that the wire names both sides
// explicitly and that each side passes its own model's rules.
func TestCodecs_RefuseAPartialChange(t *testing.T) {
	tests := []struct {
		name  string
		codec schemaext.Codec
		input string
	}{
		{name: "a missing side", codec: tsdiff.HypertableCodec(), input: `{"after":{"column":"time"}}`},
		{name: "no side at all", codec: tsdiff.HypertableCodec(), input: `{"before":null,"after":null}`},
		{name: "an extra key", codec: tsdiff.HypertableCodec(), input: `{"before":null,"after":{"column":"time"},"extra":1}`},
		{name: "an operand of the wrong representation", codec: tsdiff.HypertableCodec(), input: `{"before":{"column":"time"},"after":null}`},
		{name: "an aggregate declared without a body", codec: tsdiff.ContinuousAggregateCodec(), input: `{"before":null,"after":{"comment":"c"}}`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			value, err := test.codec.Decode(json.RawMessage(test.input))

			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(value, qt.IsNil)
		})
	}
}

// TestCloneChange_SharesNoOperand pins that a cloned change can be edited
// without reaching the record it was cloned from.
func TestCloneChange_SharesNoOperand(t *testing.T) {
	c := qt.New(t)
	original := &tsdiff.ContinuousAggregate{Before: stored.Clone().(*tsschema.ObservedContinuousAggregate), After: declared.Clone().(*tsschema.DesiredContinuousAggregate)}
	hypertable := &tsdiff.Hypertable{Before: new(*ordinary), After: new(*partitioned)}

	clone := original.CloneChange().(*tsdiff.ContinuousAggregate)
	clone.After.Body = "changed"
	*clone.Before.MaterializedOnly = true
	hypertableClone := hypertable.CloneChange().(*tsdiff.Hypertable)
	hypertableClone.After.Column = "changed"

	c.Assert(original.After.Body, qt.Equals, "SELECT 2")
	c.Assert(*original.Before.MaterializedOnly, qt.IsFalse)
	c.Assert(hypertable.After.Column, qt.Equals, "time")
}
