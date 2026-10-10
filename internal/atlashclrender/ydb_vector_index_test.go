package atlashclrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/atlashcl"
	"ptah.run/internal/atlashclrender"
)

// TestRenderInspectedForAtlasCLIWritesAYDBVectorIndex writes a vector index's
// settings as index attributes the HCL parser reads back, so a document
// `schema inspect` writes for a YDB table holding one applies back as the same
// index. Left out, the parsed index would declare no settings, and its plan
// would rebuild an index the renderer then refuses. The facet is written, so
// the document reports no loss of it.
func TestRenderInspectedForAtlasCLIWritesAYDBVectorIndex(t *testing.T) {
	tests := []struct {
		name   string
		vector *ydbschema.DesiredVectorIndex
		want   string
	}{
		{
			name:   "a distance",
			vector: &ydbschema.DesiredVectorIndex{Distance: "cosine", VectorType: "float", Dimension: 3, Levels: 2, Clusters: 128},
			want: `  index "docs_emb" {
    type = "GLOBAL USING vector_kmeans_tree"
    distance = "cosine"
    vector_type = "float"
    vector_dimension = 3
    levels = 2
    clusters = 128
    columns = [column.gen, column.emb]
  }`,
		},
		{
			name:   "a similarity",
			vector: &ydbschema.DesiredVectorIndex{Similarity: "inner_product", VectorType: "bit", Dimension: 64, Levels: 1, Clusters: 2},
			want: `  index "docs_emb" {
    type = "GLOBAL USING vector_kmeans_tree"
    similarity = "inner_product"
    vector_type = "bit"
    vector_dimension = 64
    levels = 1
    clusters = 2
    columns = [column.gen, column.emb]
  }`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := &schemamodel.Database{
				Tables: []schemamodel.Table{{StructName: "Docs", Name: "docs", PrimaryKey: []string{"id"}}},
				Fields: []schemamodel.Field{
					{StructName: "Docs", Name: "id", Type: "Uint64", Primary: true},
					{StructName: "Docs", Name: "gen", Type: "Uint64", Nullable: true},
					{StructName: "Docs", Name: "emb", Type: "String", Nullable: true},
				},
				Indexes: []schemamodel.Index{{StructName: "Docs", Name: "docs_emb", Fields: []string{"gen", "emb"},
					Type: "GLOBAL USING vector_kmeans_tree", Facets: must.Must(schemaext.NewFacets(test.vector))}},
			}

			result, err := atlashclrender.RenderInspectedForAtlasCLI(db, platform.YDB, "")
			c.Assert(err, qt.IsNil)
			parsed, err := atlashcl.Parse(result.Data, "schema.hcl")

			c.Assert(err, qt.IsNil)
			c.Assert(string(result.Data), qt.Contains, test.want)
			c.Assert(result.Diagnostics, qt.HasLen, 0)
			c.Assert(parsed.Indexes, qt.HasLen, 1)
			vector, _, err := schemaext.FacetAs[*ydbschema.DesiredVectorIndex](parsed.Indexes[0].Facets, ydbschema.VectorIndexKind)
			c.Assert(err, qt.IsNil)
			c.Assert(vector, qt.DeepEquals, test.vector)
		})
	}
}
