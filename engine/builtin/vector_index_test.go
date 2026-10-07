package builtin_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
)

// TestRender_VectorIndex_FailurePath refuses an index's vector settings on
// every target without vector_indexes, through the whole-schema render, the
// validation that renders nothing, and a single index node alike: rendered
// without them, the index would be a plain one over the vector column, which
// answers no nearest-neighbour search.
// YDB 25.1 is one of those targets, because the line keeps vector indexes
// behind a flag that is off by default.
func TestRender_VectorIndex_FailurePath(t *testing.T) {
	vector := &ast.VectorIndexSpec{Distance: "cosine", VectorType: "float", Dimension: 3, Levels: 1, Clusters: 2}
	schema := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Name: "t", PrimaryKey: []string{"id"}}},
		Fields: []schemamodel.Field{
			{StructName: "T", Name: "id", Type: "int", Primary: true},
			{StructName: "T", Name: "emb", Type: "bytea", Nullable: true},
		},
		Indexes: []schemamodel.Index{{StructName: "T", Name: "k_emb", TableName: "t", Fields: []string{"emb"},
			Type: "vector_kmeans_tree", Vector: vector}},
	}
	tests := []struct {
		dialect string
		caps    capability.Capabilities
	}{
		{dialect: platform.Postgres, caps: capability.Postgres18()},
		{dialect: platform.MySQL, caps: capability.MySQL84()},
		{dialect: platform.SQLite, caps: capability.SQLite3()},
		{dialect: platform.ClickHouse, caps: capability.ClickHouse24()},
		{dialect: platform.YDB, caps: capability.YDB251()},
	}

	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)

			statements, err := builtin.GetOrderedCreateStatementsWithCapabilities(schema, test.dialect, test.caps)
			c.Assert(err, qt.ErrorMatches, `.*index "k_emb" declares vector settings, which requires target capability vector_indexes, .*`)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(statements, qt.IsNil)

			// Validation renders no SQL, and refuses the same declaration.
			c.Assert(builtin.ValidateSchemaWithCapabilities(schema, test.dialect, test.caps), qt.ErrorMatches,
				`.*index "k_emb" declares vector settings, which requires target capability vector_indexes, .*`)

			node := &ast.IndexNode{Name: "k_emb", Table: "t", Columns: []string{"emb"}, Type: "vector_kmeans_tree", Vector: vector}
			sql, err := builtin.RenderSQLWithCapabilities(test.dialect, test.caps, node)
			c.Assert(err, qt.ErrorMatches, `.*index "k_emb" declares vector settings, which requires target capability vector_indexes, .*`)
			c.Assert(sql, qt.Equals, "")
		})
	}
}

// TestRender_VectorIndex_HappyPath is the control: the same declaration
// renders on a YDB line that has vector indexes, so the refusal above is the
// key's and not the declaration's.
func TestRender_VectorIndex_HappyPath(t *testing.T) {
	c := qt.New(t)
	node := &ast.IndexNode{Name: "k_emb", Table: "t", Columns: []string{"emb"}, Type: "vector_kmeans_tree",
		Vector: &ast.VectorIndexSpec{Distance: "cosine", VectorType: "float", Dimension: 3, Levels: 1, Clusters: 2}}

	sql, err := builtin.RenderSQLWithCapabilities(platform.YDB, capability.YDB251().With(capability.VectorIndexes, true), node)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Equals, "ALTER TABLE `t` ADD INDEX `k_emb` GLOBAL USING vector_kmeans_tree ON (`emb`) "+
		"WITH (distance=cosine, vector_type=float, vector_dimension=3, levels=1, clusters=2);\n")
}
