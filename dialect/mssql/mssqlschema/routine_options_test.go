package mssqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/mssql/mssqlschema"
)

// every states each option of the facet.
var every = mssqlschema.RoutineOptions{Principal: "auditor", SchemaBinding: true, Recompile: true, ReturnsNullOnNullInput: true}

// Options read back the same in both representations, and a value of either
// kind of facet is found by RoutineOptionsOf.
func TestRoutineOptionsOf_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name   string
		facets schemaext.Facets
		want   mssqlschema.RoutineOptions
		found  bool
	}{
		{name: "declared", facets: must.Must(mssqlschema.WithRoutineOptions(schemaext.Facets{}, every)), want: every, found: true},
		{name: "observed", facets: must.Must(mssqlschema.WithObservedRoutineOptions(schemaext.Facets{}, every)), want: every, found: true},
		{name: "one option", found: true,
			facets: must.Must(mssqlschema.WithRoutineOptions(schemaext.Facets{}, mssqlschema.RoutineOptions{SchemaBinding: true})),
			want:   mssqlschema.RoutineOptions{SchemaBinding: true}},
		{name: "none", facets: schemaext.Facets{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			options, found, err := mssqlschema.RoutineOptionsOf(test.facets)

			c.Assert(err, qt.IsNil)
			c.Assert(options, qt.Equals, test.want)
			c.Assert(found, qt.Equals, test.found)
		})
	}
}

// A principal that names no user, or holds what a statement cannot carry, is
// refused on either side, and so is a facet that states no option: a routine
// with none carries no facet.
func TestWithRoutineOptions_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name    string
		options mssqlschema.RoutineOptions
		wantErr string
	}{
		{name: "blank", options: mssqlschema.RoutineOptions{Principal: "  "}, wantErr: `.*an EXECUTE AS principal must name a database user.*`},
		{name: "a control character", options: mssqlschema.RoutineOptions{Principal: "a\x00b"}, wantErr: `.*EXECUTE AS principal.*`},
		{name: "no option", wantErr: `.*routine options must state an option.*`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			facets, err := mssqlschema.WithRoutineOptions(schemaext.Facets{}, test.options)

			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(facets.IsZero(), qt.IsTrue)
		})
	}
}

// Principal spelling preserves case until the catalog resolves equivalence.
func TestSamePrincipal(t *testing.T) {
	for _, test := range []struct {
		a, b string
		want bool
	}{
		{a: "auditor", b: "auditor", want: true},
		{a: "Auditor", b: "auditor", want: false},
		{a: "auditor  ", b: "auditor", want: true},
		{a: "auditor", b: "reviewer", want: false},
		{a: " auditor", b: "auditor", want: false},
	} {
		t.Run(test.a+"|"+test.b, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(mssqlschema.SamePrincipal(test.a, test.b), qt.Equals, test.want)
		})
	}
}

// Two sets of options are the same when every option is, the principals
// compared as SamePrincipal compares them.
func TestRoutineOptions_Same(t *testing.T) {
	for _, test := range []struct {
		name  string
		other mssqlschema.RoutineOptions
		want  bool
	}{
		{name: "every option", other: every, want: true},
		{name: "the principal in another case", other: mssqlschema.RoutineOptions{Principal: "AUDITOR", SchemaBinding: true, Recompile: true, ReturnsNullOnNullInput: true}, want: false},
		{name: "another principal", other: mssqlschema.RoutineOptions{Principal: "reviewer", SchemaBinding: true, Recompile: true, ReturnsNullOnNullInput: true}},
		{name: "no schema binding", other: mssqlschema.RoutineOptions{Principal: "auditor", Recompile: true, ReturnsNullOnNullInput: true}},
		{name: "no recompile", other: mssqlschema.RoutineOptions{Principal: "auditor", SchemaBinding: true, ReturnsNullOnNullInput: true}},
		{name: "no null input rule", other: mssqlschema.RoutineOptions{Principal: "auditor", SchemaBinding: true, Recompile: true}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(every.Same(test.other), qt.Equals, test.want)
		})
	}
}

// The WITH list entries come in the order the renderer writes them, the
// principal left to the caller.
func TestRoutineOptions_Clauses(t *testing.T) {
	c := qt.New(t)

	c.Assert(every.Clauses(), qt.DeepEquals, []string{"SCHEMABINDING", "RETURNS NULL ON NULL INPUT", "RECOMPILE"})
	c.Assert(mssqlschema.RoutineOptions{Principal: "auditor"}.Clauses(), qt.HasLen, 0)
}

// The projections carry every option across, which is what a description
// converted to stand for a database holds.
func TestRoutineOptions_Projections(t *testing.T) {
	c := qt.New(t)
	declared := new(mssqlschema.DesiredRoutineOptions(every))

	observed, err := declared.Observed()
	c.Assert(err, qt.IsNil)
	back, err := observed.Desired()
	c.Assert(err, qt.IsNil)

	c.Assert(observed, qt.DeepEquals, new(mssqlschema.ObservedRoutineOptions(every)))
	c.Assert(back, qt.DeepEquals, declared)
}

// Each codec decodes what it encodes, leaves out an option at its default,
// and refuses an object with a key or a value the encoder never writes.
func TestRoutineOptionsCodecs_RoundTrip(t *testing.T) {
	codecs := mssqlschema.RoutineOptionsCodecs()
	for _, test := range []struct {
		name    string
		codec   schemaext.Codec
		value   schemaext.Payload
		encoded string
	}{
		{name: "desired", codec: codecs[0], value: new(mssqlschema.DesiredRoutineOptions(every)),
			encoded: `{"principal":"auditor","schema_binding":true,"recompile":true,"returns_null_on_null_input":true}`},
		{name: "observed", codec: codecs[1], value: &mssqlschema.ObservedRoutineOptions{SchemaBinding: true},
			encoded: `{"schema_binding":true}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			encoded, err := test.codec.Encode(test.value)
			c.Assert(err, qt.IsNil)
			decoded, err := test.codec.Decode(encoded)
			c.Assert(err, qt.IsNil)
			_, unknownKey := test.codec.Decode([]byte(`{"principal":"auditor","schema":"dbo"}`))
			_, falseOption := test.codec.Decode([]byte(`{"principal":"auditor","recompile":false}`))
			_, noOption := test.codec.Decode([]byte(`{}`))

			c.Assert(string(encoded), qt.Equals, test.encoded)
			c.Assert(decoded, qt.DeepEquals, test.value)
			c.Assert(unknownKey, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(falseOption, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(noOption, qt.ErrorIs, schemaext.ErrInvalidValue)
		})
	}
}

// The facet attaches to a routine of either kind, and to nothing else.
func TestHoldsRoutineOptions(t *testing.T) {
	builder := objectidentity.NewBuilder(identifier.ForDialect("sqlserver"))
	for _, test := range []struct {
		name    string
		subject objectidentity.ID
		want    bool
	}{
		{name: "function", subject: builder.Function("dbo.f", ""), want: true},
		{name: "procedure", subject: builder.Procedure("dbo.p", "int"), want: true},
		{name: "table", subject: builder.Table("dbo.t"), want: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(mssqlschema.HoldsRoutineOptions(test.subject), qt.Equals, test.want)
		})
	}
}
