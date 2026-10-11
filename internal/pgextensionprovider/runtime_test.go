package pgextensionprovider_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/coverage"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/engine"
	"ptah.run/feature/pgextension"
	"ptah.run/feature/pgextension/extensioncompare"
	"ptah.run/feature/pgextension/extensionconvert"
	"ptah.run/internal/pgextensionprovider"
)

// direct is a comparison as the runtime hands it to the service, which names
// the model kinds the service is asked about.
func direct(desired, current []schemaext.Object) schemaext.ObjectComparisonRequest {
	request := comparison(desired, current)
	request.Kinds = []schemaext.Kind{pgextension.ExtensionKind}
	return request
}

// TestCompareExtensions_FailurePath refuses what no comparison of extensions
// can answer, before deciding anything.
func TestCompareExtensions_FailurePath(t *testing.T) {
	outside := direct(nil, nil)
	outside.Target = "mysql"
	otherKind := direct(nil, nil)
	otherKind.Kinds = []schemaext.Kind{"example.org/other"}
	requested := direct(nil, nil)
	requested.Requests = []schemaext.ChangeRequest{{Subject: pgextension.Ref("pg_trgm")}}
	misplaced := direct(nil, nil)
	misplaced.Desired.Objects = must.Must(schemaext.NewObjects(installed("pg_trgm", trgm())))
	tests := []struct {
		name    string
		request schemaext.ObjectComparisonRequest
		is      error
		want    string
	}{
		{name: "a target outside the family", request: outside, is: ptaherr.ErrUnsupportedDialect, want: `.*PostgreSQL extension comparison on "mysql"`},
		{name: "another kind", request: otherKind, is: schemaext.ErrInvalidValue, want: `.*unsupported extension comparison kinds`},
		{name: "a change request", request: requested, is: schemaext.ErrInvalidValue, want: `.*extension comparison accepts no change requests`},
		{name: "an observation among the declarations", request: misplaced, is: schemaext.ErrInvalidValue,
			want: `.*an extension observation among the declarations: .*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := extensioncompare.Service{}.CompareObjects(t.Context(), test.request)

			c.Assert(err, qt.ErrorIs, test.is)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(result.Complete, qt.IsFalse)
		})
	}
}

// TestConvertExtensions pins both projections: a declaration predicts the
// observation that holds what it names, and an observation keeps itself.
func TestConvertExtensions(t *testing.T) {
	tests := []struct {
		name     string
		from, to schemaext.Representation
		value    schemaext.Value
		want     schemaext.Value
	}{
		{name: "a declaration", from: schemaext.Desired, to: schemaext.Observed,
			value: &pgextension.DesiredExtension{Schema: "ext", Version: "1.6", Comment: "trigrams", IfNotExists: true},
			want:  &pgextension.ObservedExtension{Schema: "ext", Version: "1.6", Comment: new("trigrams")}},
		{name: "an observation", from: schemaext.Observed, to: schemaext.Desired,
			value: &pgextension.ObservedExtension{Schema: "public", Version: "1.5", Relocatable: true, Provides: []string{"similarity"}},
			want:  &pgextension.DesiredExtension{Schema: "public", Version: "1.5", IfNotExists: true}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			converted, err := newRuntime(c).ConvertFeatures(t.Context(), schemaext.ConversionRequest{Target: "postgres", From: test.from, To: test.to,
				Values: []schemaext.Value{test.value}})

			c.Assert(err, qt.IsNil)
			c.Assert(converted, qt.DeepEquals, []schemaext.Value{test.want})
		})
	}
}

func TestConvertExtensions_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		request schemaext.ConversionRequest
		is      error
	}{
		{name: "a target outside the family", is: ptaherr.ErrUnsupportedDialect,
			request: schemaext.ConversionRequest{Target: "mysql", From: schemaext.Desired, To: schemaext.Observed}},
		{name: "one representation to itself", is: schemaext.ErrInvalidValue,
			request: schemaext.ConversionRequest{Target: "postgres", From: schemaext.Desired, To: schemaext.Desired}},
		{name: "an observation named a declaration", is: schemaext.ErrInvalidValue,
			request: schemaext.ConversionRequest{Target: "postgres", From: schemaext.Desired, To: schemaext.Observed,
				Values: []schemaext.Value{&pgextension.ObservedExtension{Schema: "public", Version: "1.5"}}}},
		{name: "an invalid declaration", is: schemaext.ErrInvalidValue,
			request: schemaext.ConversionRequest{Target: "postgres", From: schemaext.Desired, To: schemaext.Observed,
				Values: []schemaext.Value{&pgextension.DesiredExtension{Comment: "a\x00b"}}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			converted, err := extensionconvert.Service{}.ConvertFeatures(t.Context(), test.request)

			c.Assert(err, qt.ErrorIs, test.is)
			c.Assert(converted, qt.IsNil)
		})
	}
}

// TestProvider_MapsTheExtensionCoverageKind pins that a description declining
// extensions declines the owner's: a document whose header says it does not
// describe extensions plans no drop of the extensions the database holds.
func TestProvider_MapsTheExtensionCoverageKind(t *testing.T) {
	tests := []struct {
		name    string
		limited []coverage.Object
		changes int
	}{
		{name: "a record naming the kind", limited: []coverage.Object{coverage.Refused(coverage.Extension)}},
		{name: "a record naming the extension", limited: []coverage.Object{{Kind: coverage.Extension, Name: "pg_trgm"}}},
		{name: "control: a record naming another extension", limited: []coverage.Object{{Kind: coverage.Extension, Name: "postgis"}}, changes: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			objects := comparison(nil, []schemaext.Object{installed("pg_trgm", trgm())})
			request := schemaext.ComparisonRequest{Target: "postgres", Identifiers: objects.Identifiers, Capabilities: objects.Capabilities,
				Desired:       schemaext.FeatureState{Objects: objects.Desired.Objects, Coverage: objects.Desired.Coverage},
				Current:       schemaext.FeatureState{Objects: objects.Current.Objects, Coverage: objects.Current.Coverage},
				DesiredLimits: coverage.Set{}.With(test.limited...)}

			result, err := newRuntime(c).CompareFeatures(t.Context(), request)

			c.Assert(err, qt.IsNil)
			c.Assert(result.Changes, qt.HasLen, test.changes)
		})
	}
}

// TestProvider_NeedsItsTargets pins that the owner registers only beside a
// provider of every target it serves.
func TestProvider_NeedsItsTargets(t *testing.T) {
	c := qt.New(t)

	runtime, err := engine.New(engine.Provider{ID: "example.org/targets"}, pgextensionprovider.Provider())

	c.Assert(err, qt.ErrorIs, engine.ErrInvalidRegistration)
	c.Assert(runtime, qt.IsNil)
}
