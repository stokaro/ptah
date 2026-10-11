package engine_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/coverage"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
)

// TestFeatureComparison_HandsOwnersTheirContext_HappyPath pins that the
// exclusions a run configures and the columns the desired schema declares
// reach the owner as the caller wrote them.
func TestFeatureComparison_HandsOwnersTheirContext_HappyPath(t *testing.T) {
	c := qt.New(t)
	runtime, received := limitedComparison(c)
	request := heldOnOneSide(c, runtime, schemaext.Observed)
	request.Exclusions = []schemaext.Exclusion{{Kind: coverage.Extension, Name: "plpgsql"}, {Kind: coverage.Extension, Name: " PLPGSQL"}}
	orders := objectidentity.NewBuilder(identifier.ForDialect("postgres")).TableParts("", "orders")
	request.DeclaredColumns = []schemaext.DeclaredColumn{{Table: orders, Name: "embedding", Type: "vector(384)"}}

	_, err := runtime.CompareFeatures(t.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(received.Exclusions, qt.DeepEquals, request.Exclusions)
	c.Assert(received.DeclaredColumns, qt.DeepEquals, request.DeclaredColumns)
}

// TestFeatureComparison_HandsOwnersTheirContext_FailurePath refuses an
// exclusion of a kind the runtime does not read, spelled exactly, before any
// owner runs.
func TestFeatureComparison_HandsOwnersTheirContext_FailurePath(t *testing.T) {
	for _, kind := range []coverage.Kind{"changefeed", "Extension", " extension"} {
		t.Run(string(kind), func(t *testing.T) {
			c := qt.New(t)
			runtime, received := limitedComparison(c)
			request := heldOnOneSide(c, runtime, schemaext.Observed)
			request.Exclusions = []schemaext.Exclusion{{Kind: kind, Name: "plpgsql"}}

			result, err := runtime.CompareFeatures(t.Context(), request)

			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(err, qt.ErrorMatches, `.*comparison exclusion of ".*", which is not a coverage kind this runtime reads`)
			c.Assert(result.Complete, qt.IsFalse)
			c.Assert(received.Target, qt.Equals, "")
		})
	}
}

// TestFeatureComparison_HandsOwnersWhatTheDesiredValuesRequire pins that the
// requirements of the desired values reach the owner in identity order, each
// once, and that a value on the current side requires nothing.
func TestFeatureComparison_HandsOwnersWhatTheDesiredValuesRequire(t *testing.T) {
	c := qt.New(t)
	runtime, received := limitedComparison(c)
	request := heldOnOneSide(c, runtime, schemaext.Desired)
	request.Desired.Objects = must.Must(schemaext.NewObjects(
		schemaext.Object{Ref: comparedRef(conversionFirst, "first"), Value: &conversionValue{ID: conversionFirst, Number: 1, Needs: "timescaledb pg_trgm"}},
		schemaext.Object{Ref: comparedRef(conversionSecond, "second"), Value: &conversionValue{ID: conversionSecond, Number: 2, Needs: "pg_trgm"}},
	))
	request.Current.Objects = must.Must(schemaext.NewObjects(
		schemaext.Object{Ref: comparedRef(conversionFirst, "first"), Value: &conversionValue{ID: conversionFirst, Number: 1, Needs: "postgis"}},
	))

	_, err := runtime.CompareFeatures(t.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(received.Required, qt.DeepEquals, []objectidentity.ID{requiredExtension("pg_trgm"), requiredExtension("timescaledb")})
}

// TestObjectComparison_RefusesAnInvalidRequirement refuses a requirement a
// caller hands the runtime without a kind or a name, before any owner runs.
func TestObjectComparison_RefusesAnInvalidRequirement(t *testing.T) {
	for _, test := range []struct {
		name     string
		required objectidentity.ID
	}{
		{name: "no kind", required: objectidentity.ID{Name: objectidentity.Part{Source: "pg_trgm", Normalized: "pg_trgm"}}},
		{name: "no name", required: objectidentity.ID{Kind: objectidentity.KindExtension}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime, received := limitedComparison(c)
			request := comparisonRequest(c)
			request.Required = []objectidentity.ID{requiredExtension("timescaledb"), test.required}

			result, err := runtime.CompareObjects(t.Context(), request)

			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(err, qt.ErrorMatches, `.*invalid required object.*`)
			c.Assert(result.Complete, qt.IsFalse)
			c.Assert(received.Target, qt.Equals, "")
		})
	}
}
