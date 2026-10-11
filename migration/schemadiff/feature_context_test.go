package schemadiff_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/coverage"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
)

// capturingRuntime is the bundled runtime, recording the feature comparison
// it is asked for.
type capturingRuntime struct {
	*engine.Runtime
	received *schemaext.ComparisonRequest
}

func (r capturingRuntime) CompareFeatures(ctx context.Context, request schemaext.ComparisonRequest) (schemaext.ComparisonResult, error) {
	*r.received = request
	return r.Runtime.CompareFeatures(ctx, request)
}

// TestCompare_HandsOwnersTheRunsContext pins what a feature comparison gives
// the owners beside the two states: the ignore list as exclusions, spelled as
// configured, and every column the desired schema declares on a table, an
// embedded struct's folded in and a struct that declares no table left out.
func TestCompare_HandsOwnersTheRunsContext(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Document", Name: "documents", Schema: "app"}},
		Fields: []schemamodel.Field{
			{StructName: "Document", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "Document", Name: "embedding", Type: "vector(384)"},
			{StructName: "Stamps", Name: "created_at", Type: "TIMESTAMPTZ"},
			{StructName: "Unplaced", Name: "label", Type: "citext"},
		},
		EmbeddedFields: []schemamodel.EmbeddedField{{StructName: "Document", Mode: "inline", EmbeddedTypeName: "Stamps"}},
	}
	options := config.WithIgnoredExtensions("plpgsql", " Adminpack")
	options.Dialect = "postgres"
	var received schemaext.ComparisonRequest

	_, err := schemadiff.CompareWithOptions(t.Context(), desired, &catalog.Database{}, options,
		capturingRuntime{Runtime: must.Must(builtin.New()), received: &received})

	c.Assert(err, qt.IsNil)
	documents := objectidentity.NewBuilder(identifier.ForDialect("postgres")).TableParts("app", "documents")
	c.Assert(received.Exclusions, qt.DeepEquals, []schemaext.Exclusion{{Kind: coverage.Extension, Name: "plpgsql"}, {Kind: coverage.Extension, Name: " Adminpack"}})
	c.Assert(received.DeclaredColumns, qt.DeepEquals, []schemaext.DeclaredColumn{
		{Table: documents, Name: "id", Type: "INTEGER"},
		{Table: documents, Name: "embedding", Type: "vector(384)"},
		{Table: documents, Name: "created_at", Type: "TIMESTAMPTZ"},
	})
}
