package engine_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

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
