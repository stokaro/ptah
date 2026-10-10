package tsast_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/timescaledb/tsast"
	"ptah.run/dialect/timescaledb/tsdiff"
	"ptah.run/dialect/timescaledb/tsschema"
	"ptah.run/engine"
)

func replaced() *tsast.ContinuousAggregate {
	return &tsast.ContinuousAggregate{Schema: "metrics", Name: "hourly", Change: tsdiff.ContinuousAggregate{
		Before: &tsschema.ObservedContinuousAggregate{Definition: "SELECT 1;"},
		After:  &tsschema.DesiredContinuousAggregate{Body: "SELECT 2", MaterializedOnly: new(true)},
	}}
}

// TestOperations_NameTheirGateAndTheirSkipLine pins what a PostgreSQL-family
// renderer reads to decide whether a target takes the statement, and what the
// skip line names when it does not.
func TestOperations_NameTheirGateAndTheirSkipLine(t *testing.T) {
	tests := []struct {
		name      string
		operation interface {
			RequiredCapability() capability.Capability
			OmissionSubject() (string, string)
		}
		wantKey  capability.Capability
		wantKind string
		wantName string
	}{
		{name: "a hypertable", operation: &tsast.CreateHypertable{Table: "public.readings", Hypertable: tsschema.DesiredHypertable{Column: "time"}},
			wantKey: capability.Hypertables, wantKind: "hypertable", wantName: "public.readings"},
		{name: "an aggregate", operation: replaced(), wantKey: capability.ContinuousAggregates, wantKind: "continuous aggregate", wantName: "hourly"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			kind, name := test.operation.OmissionSubject()
			c.Assert(test.operation.RequiredCapability(), qt.Equals, test.wantKey)
			c.Assert(kind, qt.Equals, test.wantKind)
			c.Assert(name, qt.Equals, test.wantName)
		})
	}
}

// TestContinuousAggregate_SchemaChangeNamesTheTransition pins the action a
// schema-change report reads from each transition.
func TestContinuousAggregate_SchemaChangeNamesTheTransition(t *testing.T) {
	declared := &tsschema.DesiredContinuousAggregate{Body: "SELECT 1"}
	stored := &tsschema.ObservedContinuousAggregate{Definition: "SELECT 1"}
	tests := []struct {
		name   string
		schema string
		change tsdiff.ContinuousAggregate
		want   ast.ExtensionChange
	}{
		{name: "created", change: tsdiff.ContinuousAggregate{After: declared}, want: ast.ExtensionChange{Action: ast.ExtensionAdd, Name: "hourly"}},
		{name: "dropped", schema: "metrics", change: tsdiff.ContinuousAggregate{Before: stored}, want: ast.ExtensionChange{Action: ast.ExtensionDrop, Name: "metrics.hourly"}},
		{name: "replaced", change: tsdiff.ContinuousAggregate{Before: stored, After: declared}, want: ast.ExtensionChange{Action: ast.ExtensionModify, Name: "hourly"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			operation := &tsast.ContinuousAggregate{Schema: test.schema, Name: "hourly", Change: test.change}
			c.Assert(operation.SchemaChange(), qt.DeepEquals, test.want)
		})
	}
}

// TestCodecs_RoundTripEveryOperation pins the operation wire a plan artifact
// carries.
func TestCodecs_RoundTripEveryOperation(t *testing.T) {
	tests := []struct {
		name      string
		operation schemaext.Payload
	}{
		{name: "a hypertable", operation: &tsast.CreateHypertable{Table: "readings", Hypertable: tsschema.DesiredHypertable{Column: "time", ChunkInterval: "1 day", IfNotExists: true}}},
		{name: "an aggregate replaced in a schema", operation: replaced()},
		{name: "an aggregate in the default schema", operation: &tsast.ContinuousAggregate{Name: "hourly", Change: tsdiff.ContinuousAggregate{After: &tsschema.DesiredContinuousAggregate{Body: "SELECT 1"}}}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			codecs := must.Must(engine.New(engine.Provider{ID: tsschema.Owner, Codecs: tsast.Codecs()})).Codecs()

			data, err := codecs.Marshal(t.Context(), schemaext.Operation, []schemaext.Payload{test.operation})
			c.Assert(err, qt.IsNil)
			decoded, err := codecs.Unmarshal(t.Context(), data)

			c.Assert(err, qt.IsNil)
			c.Assert(decoded, qt.DeepEquals, []schemaext.Payload{test.operation})
		})
	}
}

// TestCodecs_RefuseAnIncompleteOperation pins the wire rules: every key is
// present and non-null, and the operands pass their own models.
func TestCodecs_RefuseAnIncompleteOperation(t *testing.T) {
	tests := []struct {
		name  string
		codec int
		input string
	}{
		{name: "a hypertable without its table", codec: 0, input: `{"table":"","hypertable":{"column":"time"}}`},
		{name: "a hypertable without settings", codec: 0, input: `{"table":"readings","hypertable":null}`},
		{name: "a hypertable with an extra key", codec: 0, input: `{"table":"readings","hypertable":{"column":"time"},"extra":1}`},
		{name: "an aggregate without a name", codec: 1, input: `{"schema":"","name":"","change":{"before":null,"after":{"body":"SELECT 1"}}}`},
		{name: "an aggregate without operands", codec: 1, input: `{"schema":"","name":"hourly","change":{"before":null,"after":null}}`},
		{name: "an aggregate without its schema key", codec: 1, input: `{"name":"hourly","change":{"before":null,"after":{"body":"SELECT 1"}}}`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			value, err := tsast.Codecs()[test.codec].Decode(json.RawMessage(test.input))

			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(value, qt.IsNil)
		})
	}
}

// TestCloneExtension_SharesNoOperand pins that a cloned operation can be
// edited without reaching the plan it was cloned from.
func TestCloneExtension_SharesNoOperand(t *testing.T) {
	c := qt.New(t)
	original := replaced()
	hypertable := &tsast.CreateHypertable{Table: "readings", Hypertable: tsschema.DesiredHypertable{Column: "time"}}

	clone := original.CloneExtension().(*tsast.ContinuousAggregate)
	clone.Change.After.Body = "changed"
	*clone.Change.After.MaterializedOnly = false
	hypertableClone := hypertable.CloneExtension().(*tsast.CreateHypertable)
	hypertableClone.Hypertable.Column = "changed"

	c.Assert(original.Change.After.Body, qt.Equals, "SELECT 2")
	c.Assert(*original.Change.After.MaterializedOnly, qt.IsTrue)
	c.Assert(hypertable.Hypertable.Column, qt.Equals, "time")
}
