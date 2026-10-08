package goschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/goschema"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/convert/goschematogo"
)

// TestRender_VectorIndex writes a YDB vector index's settings as the
// attributes the annotation parser reads them back from, so a schema read from
// a database and written as Go builds the same index.
func TestRender_VectorIndex(t *testing.T) {
	tests := []struct {
		name   string
		vector *ast.VectorIndexSpec
		want   string
	}{
		{name: "a distance", vector: &ast.VectorIndexSpec{Distance: "cosine", VectorType: "float", Dimension: 3, Levels: 2, Clusters: 128},
			want: `distance="cosine" vector_type="float" vector_dimension="3" levels="2" clusters="128"`},
		{name: "a similarity", vector: &ast.VectorIndexSpec{Similarity: "inner_product", VectorType: "bit", Dimension: 64, Levels: 1, Clusters: 2},
			want: `similarity="inner_product" vector_type="bit" vector_dimension="64" levels="1" clusters="2"`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := &schemamodel.Database{
				Tables: []schemamodel.Table{{StructName: "Doc", Name: "docs", PrimaryKey: []string{"id"}}},
				Fields: []schemamodel.Field{
					{StructName: "Doc", FieldName: "ID", Name: "id", Type: "BIGINT", Primary: true},
					{StructName: "Doc", FieldName: "Emb", Name: "emb", Type: "String"},
				},
				Indexes: []schemamodel.Index{{
					StructName: "Doc", Name: "idx_docs_emb", TableName: "docs", Fields: []string{"emb"},
					Type: "GLOBAL USING vector_kmeans_tree", Vector: test.vector,
				}},
			}

			files, err := goschematogo.Render(c.Context(), db, goschematogo.Options{SingleFile: true})
			c.Assert(err, qt.IsNil)
			c.Assert(files, qt.HasLen, 1)
			reparsed, err := goschema.ParseSource("schema.go", string(files[0].Data))

			c.Assert(err, qt.IsNil)
			c.Assert(string(files[0].Data), qt.Contains, test.want)
			c.Assert(reparsed.Indexes, qt.HasLen, 1)
			c.Assert(reparsed.Indexes[0].Vector, qt.DeepEquals, test.vector)
			c.Assert(reparsed.Indexes[0].Type, qt.Equals, "GLOBAL USING vector_kmeans_tree")
		})
	}
}
