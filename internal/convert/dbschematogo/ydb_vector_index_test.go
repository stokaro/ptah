package dbschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/convert/dbschematogo"
)

// A vector index the YDB reader reports keeps its settings in the model, as
// the declaration that keeps them, which is what `ptah introspect` writes as
// annotations and what a rollback creates the index again from: dropped here,
// the index would come back with none, which the renderer refuses.
func TestConvertDBSchemaToGoSchema_KeepsAYDBVectorIndex(t *testing.T) {
	c := qt.New(t)
	vector := &ydbschema.ObservedVectorIndex{Similarity: "inner_product", VectorType: "int8", Dimension: 8, Levels: 2, Clusters: 16}
	read := &catalog.Database{
		Tables: []catalog.Table{{Name: "t", Columns: []catalog.Column{
			{Name: "id", DataType: "Int64", ColumnType: "Int64", IsNullable: "NO", IsPrimaryKey: true},
			{Name: "emb", DataType: "String", ColumnType: "String", IsNullable: "YES"},
		}}},
		Indexes: []catalog.Index{{Name: "i", TableName: "t", Columns: []string{"emb"},
			Method: "GLOBAL USING vector_kmeans_tree", Facets: must.Must(schemaext.NewFacets(vector))}},
	}

	model := must.Must(dbschematogo.ConvertDBSchemaToGoSchema(t.Context(), read, platform.YDB, must.Must(builtin.New())))

	c.Assert(model.Indexes, qt.HasLen, 1)
	c.Assert(model.Indexes[0].Type, qt.Equals, "GLOBAL USING vector_kmeans_tree")
	declared, _, err := schemaext.FacetAs[*ydbschema.DesiredVectorIndex](model.Indexes[0].Facets, ydbschema.VectorIndexKind)
	c.Assert(err, qt.IsNil)
	c.Assert(declared, qt.DeepEquals, vector.Desired())
	vector.Levels = 3
	c.Assert(declared.Levels, qt.Equals, uint64(2))
}
