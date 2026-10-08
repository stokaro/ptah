package goschematodb_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/convert/goschematodb"
	"ptah.run/migration/schemadiff"
)

// ydbVectorDatabase declares a YDB vector index over a vector column.
func ydbVectorDatabase() *schemamodel.Database {
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Name: "t", PrimaryKey: []string{"id"}}},
		Fields: []schemamodel.Field{
			{StructName: "T", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "T", Name: "emb", Type: "vector(3)", Nullable: true},
		},
		Indexes: []schemamodel.Index{{StructName: "T", Name: "i", Fields: []string{"emb"}, Type: "vector_kmeans_tree",
			Vector: &ast.VectorIndexSpec{Distance: "cosine", VectorType: "float", Dimension: 3, Levels: 1, Clusters: 2}}},
	}
	schemamodel.Finalize(db)
	return db
}

// A desired state compared as the --from side of `schema diff` keeps a
// vector index's settings through the hop to the DB shape: dropped here, the
// file diffed against itself would plan the index's rebuild.
func TestToDBSchema_CarriesAYDBVectorIndex(t *testing.T) {
	c := qt.New(t)
	db := ydbVectorDatabase()

	current := must.Must(goschematodb.ToDBSchema(t.Context(), db, platform.YDB, must.Must(builtin.New())))
	diff := must.Must(schemadiff.CompareWithDialect(t.Context(), db, current, platform.YDB, must.Must(builtin.New())))

	c.Assert(current.Indexes, qt.HasLen, 1)
	c.Assert(current.Indexes[0].Vector, qt.DeepEquals, db.Indexes[0].Vector)
	c.Assert(diff.IndexAdditions(), qt.HasLen, 0)
	c.Assert(diff.IndexRemovals(), qt.HasLen, 0)
}
