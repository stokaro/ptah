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

// A principal reads back the same in both representations, and a value of
// either kind of facet is found by ExecuteAsPrincipal.
func TestExecuteAsPrincipal_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name   string
		facets schemaext.Facets
		want   string
		found  bool
	}{
		{name: "declared", facets: must.Must(mssqlschema.WithExecuteAs(schemaext.Facets{}, "auditor")), want: "auditor", found: true},
		{name: "observed", facets: must.Must(mssqlschema.WithObservedExecuteAs(schemaext.Facets{}, "auditor")), want: "auditor", found: true},
		{name: "none", facets: schemaext.Facets{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			principal, found, err := mssqlschema.ExecuteAsPrincipal(test.facets)

			c.Assert(err, qt.IsNil)
			c.Assert(principal, qt.Equals, test.want)
			c.Assert(found, qt.Equals, test.found)
		})
	}
}

// A principal that names no user, or holds what a statement cannot carry, is
// refused on either side.
func TestExecuteAsPrincipal_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name      string
		principal string
		wantErr   string
	}{
		{name: "blank", principal: "  ", wantErr: `.*an EXECUTE AS principal must name a database user.*`},
		{name: "a control character", principal: "a\x00b", wantErr: `.*EXECUTE AS principal.*`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			facets, err := mssqlschema.WithExecuteAs(schemaext.Facets{}, test.principal)

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

// The projections carry the principal across, which is what a description
// converted to stand for a database holds.
func TestExecuteAs_Projections(t *testing.T) {
	c := qt.New(t)
	declared := &mssqlschema.DesiredExecuteAs{Principal: "auditor"}

	observed, err := declared.Observed()
	c.Assert(err, qt.IsNil)
	back, err := observed.Desired()
	c.Assert(err, qt.IsNil)

	c.Assert(observed, qt.DeepEquals, &mssqlschema.ObservedExecuteAs{Principal: "auditor"})
	c.Assert(back, qt.DeepEquals, declared)
}

// Each codec decodes what it encodes, and refuses an object with a key the
// encoder never writes.
func TestExecuteAsCodecs_RoundTrip(t *testing.T) {
	codecs := mssqlschema.ExecuteAsCodecs()
	for _, test := range []struct {
		name  string
		codec schemaext.Codec
		value schemaext.Payload
	}{
		{name: "desired", codec: codecs[0], value: &mssqlschema.DesiredExecuteAs{Principal: "auditor"}},
		{name: "observed", codec: codecs[1], value: &mssqlschema.ObservedExecuteAs{Principal: "auditor"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			encoded, err := test.codec.Encode(test.value)
			c.Assert(err, qt.IsNil)
			decoded, err := test.codec.Decode(encoded)
			c.Assert(err, qt.IsNil)
			_, refused := test.codec.Decode([]byte(`{"principal":"auditor","schema":"dbo"}`))

			c.Assert(string(encoded), qt.Equals, `{"principal":"auditor"}`)
			c.Assert(decoded, qt.DeepEquals, test.value)
			c.Assert(refused, qt.ErrorIs, schemaext.ErrInvalidValue)
		})
	}
}

// The facet attaches to a routine of either kind, and to nothing else.
func TestHoldsExecuteAs(t *testing.T) {
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

			c.Assert(mssqlschema.HoldsExecuteAs(test.subject), qt.Equals, test.want)
		})
	}
}
