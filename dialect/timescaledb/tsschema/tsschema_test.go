package tsschema_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/timescaledb/tsschema"
	"ptah.run/engine"
)

func registry(c *qt.C) schemaext.Registry {
	c.Helper()
	runtime, err := engine.New(engine.Provider{ID: tsschema.Owner, Codecs: tsschema.Codecs()})
	c.Assert(err, qt.IsNil)
	return runtime.Codecs()
}

// TestCodecs_RoundTripEveryModel pins that each representation survives its
// codec unchanged, the unset option included: a nil option is the server's
// default, and decoding it as false would hold the server to a value nobody
// wrote.
func TestCodecs_RoundTripEveryModel(t *testing.T) {
	tests := []struct {
		name           string
		representation schemaext.Representation
		value          schemaext.Value
	}{
		{name: "a declared hypertable", representation: schemaext.Desired,
			value: &tsschema.DesiredHypertable{Column: "time", ChunkInterval: "1 day", IfNotExists: true, Comment: "by time"}},
		{name: "the smallest declared hypertable", representation: schemaext.Desired, value: &tsschema.DesiredHypertable{Column: "time"}},
		{name: "an observed hypertable", representation: schemaext.Observed,
			value: &tsschema.ObservedHypertable{Column: "time", ColumnType: "timestamp with time zone", ChunkInterval: "7 days", Dimensions: 2}},
		{name: "a declared aggregate", representation: schemaext.Desired,
			value: &tsschema.DesiredContinuousAggregate{Body: "SELECT 1", MaterializedOnly: new(false), Comment: "c", StructName: "Hourly",
				Normalized: &tsschema.NormalizedBody{Body: "SELECT 1;"}}},
		{name: "a declared aggregate taking the default option", representation: schemaext.Desired,
			value: &tsschema.DesiredContinuousAggregate{Body: "SELECT 1"}},
		{name: "an observed aggregate", representation: schemaext.Observed,
			value: &tsschema.ObservedContinuousAggregate{Definition: "SELECT 1;", MaterializedOnly: new(true), HypertableSchema: "public", HypertableName: "readings"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			codecs := registry(c)

			data, err := codecs.Marshal(t.Context(), test.representation, []schemaext.Payload{test.value})
			c.Assert(err, qt.IsNil)
			decoded, err := codecs.Unmarshal(t.Context(), data)

			c.Assert(err, qt.IsNil)
			c.Assert(decoded, qt.DeepEquals, []schemaext.Payload{test.value})
		})
	}
}

// TestCodecs_RefuseWhatTheModelCannotHold pins the wire rules: unknown and
// null keys, a missing required field, and values the validators refuse.
func TestCodecs_RefuseWhatTheModelCannotHold(t *testing.T) {
	tests := []struct {
		name  string
		codec schemaext.Codec
		input string
	}{
		{name: "a hypertable without a column", codec: tsschema.HypertableCodecs()[0], input: `{"chunk_interval":"1 day"}`},
		{name: "a hypertable with an empty column", codec: tsschema.HypertableCodecs()[0], input: `{"column":" "}`},
		{name: "a hypertable with an unknown key", codec: tsschema.HypertableCodecs()[0], input: `{"column":"time","dimensions":1}`},
		{name: "a hypertable with a null key", codec: tsschema.HypertableCodecs()[0], input: `{"column":"time","comment":null}`},
		{name: "a hypertable spelled in another case", codec: tsschema.HypertableCodecs()[0], input: `{"Column":"time"}`},
		{name: "null", codec: tsschema.HypertableCodecs()[0], input: `null`},
		{name: "an observation without its dimension count", codec: tsschema.HypertableCodecs()[1], input: `{"column":"time"}`},
		{name: "an observation with no dimension", codec: tsschema.HypertableCodecs()[1], input: `{"column":"time","dimensions":0}`},
		{name: "an observation with a declaration's key", codec: tsschema.HypertableCodecs()[1], input: `{"column":"time","dimensions":1,"if_not_exists":true}`},
		{name: "an aggregate without a body", codec: tsschema.ContinuousAggregateCodecs()[0], input: `{"comment":"c"}`},
		{name: "an aggregate whose body is only punctuation", codec: tsschema.ContinuousAggregateCodecs()[0], input: `{"body":" ; "}`},
		{name: "an aggregate with an empty normalized body", codec: tsschema.ContinuousAggregateCodecs()[0], input: `{"body":"SELECT 1","normalized":{}}`},
		{name: "an aggregate with a normalized key it does not have", codec: tsschema.ContinuousAggregateCodecs()[0], input: `{"body":"SELECT 1","normalized":{"body":"x","extra":1}}`},
		{name: "an observation without a definition", codec: tsschema.ContinuousAggregateCodecs()[1], input: `{"hypertable_name":"readings"}`},
		{name: "an observation with a declaration's key", codec: tsschema.ContinuousAggregateCodecs()[1], input: `{"definition":"SELECT 1","body":"SELECT 1"}`},
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

// TestValues_CloneIsIndependent pins that a clone shares no pointer with its
// source, so a normalization attached to one copy cannot reach a declaration
// another caller holds.
func TestValues_CloneIsIndependent(t *testing.T) {
	c := qt.New(t)
	original := &tsschema.DesiredContinuousAggregate{Body: "SELECT 1", MaterializedOnly: new(true), Normalized: &tsschema.NormalizedBody{Body: "SELECT 1"}}
	observed := &tsschema.ObservedContinuousAggregate{Definition: "SELECT 1", MaterializedOnly: new(true)}

	clone := original.Clone().(*tsschema.DesiredContinuousAggregate)
	*clone.MaterializedOnly = false
	clone.Normalized.Body = "changed"
	observedClone := observed.Clone().(*tsschema.ObservedContinuousAggregate)
	*observedClone.MaterializedOnly = false

	c.Assert(*original.MaterializedOnly, qt.IsTrue)
	c.Assert(original.Normalized.Body, qt.Equals, "SELECT 1")
	c.Assert(*observed.MaterializedOnly, qt.IsTrue)
	c.Assert(original.Equal(clone), qt.IsFalse)
	c.Assert(original.Equal(original.Clone()), qt.IsTrue)
}

// TestSamePartitioning pins the comparison a hypertable is held to: the column
// is folded the way PostgreSQL folds an unquoted name, and an omitted interval
// takes the server's default rather than differing from it.
func TestSamePartitioning(t *testing.T) {
	observed := &tsschema.ObservedHypertable{Column: "time", ChunkInterval: "7 days", Dimensions: 1}
	tests := []struct {
		name     string
		declared tsschema.DesiredHypertable
		want     bool
	}{
		{name: "the same column and no interval", declared: tsschema.DesiredHypertable{Column: "time"}, want: true},
		{name: "the column in another case", declared: tsschema.DesiredHypertable{Column: " TIME "}, want: true},
		{name: "the interval the server reports", declared: tsschema.DesiredHypertable{Column: "time", ChunkInterval: "7 DAYS"}, want: true},
		{name: "another interval", declared: tsschema.DesiredHypertable{Column: "time", ChunkInterval: "1 day"}, want: false},
		{name: "another column", declared: tsschema.DesiredHypertable{Column: "recorded_at"}, want: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(tsschema.SamePartitioning(&test.declared, observed), qt.Equals, test.want)
		})
	}
}

// TestProjections_CarryWhatTheOtherSideCanHold pins the predictions between
// representations: a declaration predicts one dimension, and an observation
// declares its first dimension and drops the type and count, which no
// declaration states. A definition is carried as the catalog wrote it; the
// statement that replays it folds the catalog's punctuation.
func TestProjections_CarryWhatTheOtherSideCanHold(t *testing.T) {
	c := qt.New(t)

	observed := must.Must((&tsschema.DesiredHypertable{Column: "time", ChunkInterval: "1 day", IfNotExists: true}).Observed())
	declared := must.Must((&tsschema.ObservedHypertable{Column: "time", ColumnType: "timestamptz", ChunkInterval: "7 days", Dimensions: 2}).Desired())
	aggregate := must.Must((&tsschema.ObservedContinuousAggregate{Definition: " SELECT 1; ", MaterializedOnly: new(true), HypertableName: "r"}).Desired())
	predicted := must.Must((&tsschema.DesiredContinuousAggregate{Body: "SELECT 1", Normalized: &tsschema.NormalizedBody{Body: "SELECT 2"}}).Observed())

	c.Assert(observed, qt.DeepEquals, &tsschema.ObservedHypertable{Column: "time", ChunkInterval: "1 day", Dimensions: 1})
	c.Assert(declared, qt.DeepEquals, &tsschema.DesiredHypertable{Column: "time", ChunkInterval: "7 days"})
	c.Assert(aggregate.Body, qt.Equals, " SELECT 1; ")
	c.Assert(*aggregate.MaterializedOnly, qt.IsTrue)
	c.Assert(predicted.Definition, qt.Equals, "SELECT 1")
}

// TestContinuousAggregateRef pins the identity rules: the schema an author
// left out is the default, compares equal to it, and stays unwritten.
func TestContinuousAggregateRef(t *testing.T) {
	c := qt.New(t)
	defaulted := tsschema.ContinuousAggregateRef("", "Hourly")
	written := tsschema.ContinuousAggregateRef("metrics", "hourly")

	c.Assert(tsschema.QualifiedName(defaulted), qt.Equals, "Hourly")
	c.Assert(tsschema.AuthoredSchema(defaulted), qt.Equals, "")
	c.Assert(tsschema.QualifiedName(written), qt.Equals, "metrics.hourly")
	c.Assert(tsschema.ValidateContinuousAggregateRef(written), qt.IsNil)
	c.Assert(tsschema.ContinuousAggregateRef("", "hourly").Key(), qt.Equals, tsschema.ContinuousAggregateRefWith(identifier.ForDialect("postgres"), "public", "hourly").Key())
	c.Assert(tsschema.ValidateContinuousAggregateRef(objectidentity.ID{Kind: objectidentity.KindView, Name: written.Name}), qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(tsschema.FoldBody("  SELECT 1 ;\n"), qt.Equals, "SELECT 1")
}

// TestCompleteCoverage_ClaimsBothKinds pins the coverage a source records when
// it describes every hypertable and aggregate.
func TestCompleteCoverage_ClaimsBothKinds(t *testing.T) {
	c := qt.New(t)

	coverage := must.Must(tsschema.CompleteCoverage(schemaext.Observed))

	c.Assert(coverage.Representation(), qt.Equals, schemaext.Observed)
	c.Assert(coverage.Lookup(tsschema.HypertableKind, objectidentity.ID{}).State, qt.Equals, schemaext.Complete)
	c.Assert(coverage.Lookup(tsschema.ContinuousAggregateKind, objectidentity.ID{}).State, qt.Equals, schemaext.Complete)
}

func limitedCoverage(c *qt.C, state schemaext.KnowledgeState) schemaext.Coverage {
	c.Helper()
	return must.Must(schemaext.NewCoverage(schemaext.Observed, must.Must(tsschema.CompleteCoverage(schemaext.Observed)).KindRecords(),
		[]schemaext.SubjectCoverage{{Kind: tsschema.ContinuousAggregateKind, Subject: tsschema.ContinuousAggregateRef("", "hourly"),
			Knowledge: schemaext.Knowledge{State: state, Reason: "not read"}}}))
}

// TestRequireNoLimits_HappyPath pins the subject records an export that can
// only claim complete coverage carries anyway: a complete or absent subject
// restates that claim.
func TestRequireNoLimits_HappyPath(t *testing.T) {
	for _, state := range []schemaext.KnowledgeState{schemaext.Complete, schemaext.Absent} {
		t.Run(string(state), func(t *testing.T) {
			c := qt.New(t)
			c.Assert(tsschema.RequireNoLimits(limitedCoverage(c, state)), qt.IsNil)
		})
	}
}

// TestRequireNoLimits_FailurePath pins the refusal of a limit the document
// has no spelling for, naming the subject and why it was limited.
func TestRequireNoLimits_FailurePath(t *testing.T) {
	c := qt.New(t)

	err := tsschema.RequireNoLimits(limitedCoverage(c, schemaext.Unrepresentable))

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `(?s).*hourly cannot be exported without losing its coverage record: unrepresentable not read`)
}
