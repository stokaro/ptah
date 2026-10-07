package ydb_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/engine/builtin/internal/dialects/ydb"
)

// vectorSpec is a vector index's settings every line with vector indexes
// builds.
func vectorSpec() *ast.VectorIndexSpec {
	return &ast.VectorIndexSpec{Distance: "cosine", VectorType: "float", Dimension: 3, Levels: 1, Clusters: 2}
}

// vectorIndex is an index over the vector column emb with spec.
func vectorIndex(spec *ast.VectorIndexSpec, columns ...string) *ast.IndexNode {
	if len(columns) == 0 {
		columns = []string{"emb"}
	}
	return &ast.IndexNode{Name: "by_emb", Columns: columns, Type: "vector_kmeans_tree", Vector: spec}
}

// TestRender_VectorIndex_HappyPath pins how a vector index is written: its
// settings in the WITH (...) of the clause that creates it, the one place YDB
// takes them, every one named. Each rendering was applied to local-ydb
// 26.2.1.14 and 25.1.4.7 (with EnableVectorIndex) and read back through
// DescribeTable.
func TestRender_VectorIndex_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		caps capability.Capabilities
		node ast.Node
		want string
	}{
		{
			name: "a new table's vector index, its column declared as a vector",
			caps: capability.YDB262(),
			node: withIndex(vectorIndex(vectorSpec()), ast.NewColumn("emb", "vector(3)")),
			want: "CREATE TABLE `t` (\n" +
				"    `id` Int64 NOT NULL,\n" +
				"    `emb` String,\n" +
				"    PRIMARY KEY (`id`),\n" +
				"    INDEX `by_emb` GLOBAL USING vector_kmeans_tree ON (`emb`) " +
				"WITH (distance=cosine, vector_type=float, vector_dimension=3, levels=1, clusters=2)\n" +
				");\n",
		},
		{
			name: "a prefix and a cover",
			caps: capability.YDB262(),
			node: withIndex(&ast.IndexNode{Name: "by_emb", Columns: []string{"gen", "emb"}, IncludeColumns: []string{"body"},
				Type: "vector_kmeans_tree", Vector: vectorSpec()},
				ast.NewColumn("gen", "BIGINT"), ast.NewColumn("emb", "BYTEA"), ast.NewColumn("body", "TEXT")),
			want: "CREATE TABLE `t` (\n" +
				"    `id` Int64 NOT NULL,\n" +
				"    `gen` Int64,\n" +
				"    `emb` String,\n" +
				"    `body` Utf8,\n" +
				"    PRIMARY KEY (`id`),\n" +
				"    INDEX `by_emb` GLOBAL USING vector_kmeans_tree ON (`gen`, `emb`) COVER (`body`) " +
				"WITH (distance=cosine, vector_type=float, vector_dimension=3, levels=1, clusters=2)\n" +
				");\n",
		},
		{
			name: "a metric named by pgvector's operator class",
			caps: capability.YDB262(),
			node: &ast.IndexNode{Name: "by_emb", Table: "dir.t", Columns: []string{"emb"}, Type: "vector_kmeans_tree",
				Operator: "vector_ip_ops", Vector: &ast.VectorIndexSpec{VectorType: "int8", Dimension: 8, Levels: 2, Clusters: 4}},
			want: "ALTER TABLE `dir/t` ADD INDEX `by_emb` GLOBAL USING vector_kmeans_tree ON (`emb`) " +
				"WITH (similarity=inner_product, vector_type=int8, vector_dimension=8, levels=2, clusters=4);\n",
		},
		{
			name: "bit vectors where the line builds them",
			caps: capability.YDB261(),
			node: &ast.IndexNode{Name: "by_emb", Table: "t", Columns: []string{"emb"}, Type: "vector_kmeans_tree",
				Vector: &ast.VectorIndexSpec{Distance: "manhattan", VectorType: "bit", Dimension: 64, Levels: 1, Clusters: 2}},
			want: "ALTER TABLE `t` ADD INDEX `by_emb` GLOBAL USING vector_kmeans_tree ON (`emb`) " +
				"WITH (distance=manhattan, vector_type=bit, vector_dimension=64, levels=1, clusters=2);\n",
		},
		{
			name: "a vector index on 25.1 with its flag on",
			caps: capability.YDB251().With(capability.VectorIndexes, true),
			node: alter(&ast.AddIndexOperation{Index: vectorIndex(vectorSpec())}),
			want: "ALTER TABLE `t` ADD INDEX `by_emb` GLOBAL USING vector_kmeans_tree ON (`emb`) " +
				"WITH (distance=cosine, vector_type=float, vector_dimension=3, levels=1, clusters=2);\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydb.NewWithCapabilities(test.caps).Render(test.node)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// TestRender_VectorIndex_FailurePath refuses a vector index a target cannot
// build, by its key, and one YDB refuses, with the server's reason or the
// method YDB has instead.
func TestRender_VectorIndex_FailurePath(t *testing.T) {
	bits := &ast.VectorIndexSpec{Distance: "manhattan", VectorType: "bit", Dimension: 64, Levels: 1, Clusters: 2}
	tests := []struct {
		name    string
		caps    capability.Capabilities
		node    ast.Node
		wantErr string
	}{
		{
			name:    "a vector index on 25.1 with its flag off",
			caps:    capability.YDB251(),
			node:    &ast.IndexNode{Name: "by_emb", Table: "t", Columns: []string{"emb"}, Type: "vector_kmeans_tree", Vector: vectorSpec()},
			wantErr: `index "by_emb" is a vector index, which requires target capability vector_indexes, unavailable on this ydb target`,
		},
		{
			name: "bit vectors on a line that does not build them",
			caps: capability.YDB254(),
			node: &ast.IndexNode{Name: "by_emb", Table: "t", Columns: []string{"emb"}, Type: "vector_kmeans_tree", Vector: bits},
			wantErr: `index "by_emb" stores bit vectors, which requires target capability vector_bit_type, ` +
				`unavailable on this ydb target`,
		},
		{
			name:    "pgvector's hnsw",
			caps:    capability.YDB262(),
			node:    withIndex(&ast.IndexNode{Name: "by_emb", Columns: []string{"emb"}, Type: "hnsw"}, ast.NewColumn("emb", "vector(3)")),
			wantErr: `index "by_emb": index method "hnsw" is pgvector's and has no YDB counterpart: YDB's vector index is vector_kmeans_tree, .*`,
		},
		{
			name:    "pgvector's ivfflat",
			caps:    capability.YDB262(),
			node:    &ast.IndexNode{Name: "by_emb", Table: "t", Columns: []string{"emb"}, Type: "ivfflat", Operator: "vector_l2_ops"},
			wantErr: `index "by_emb": index method "ivfflat" is pgvector's and has no YDB counterpart: .*`,
		},
		{
			name: "hnsw's build parameter",
			caps: capability.YDB262(),
			node: &ast.IndexNode{Name: "by_emb", Table: "t", Columns: []string{"emb"}, Type: "vector_kmeans_tree", Vector: vectorSpec(),
				StorageParams: map[string]string{"m": "16", "ef_construction": "64"}},
			wantErr: `index "by_emb": storage parameter "ef_construction" belongs to pgvector's hnsw index; ` +
				`a vector_kmeans_tree index is shaped by its levels and clusters`,
		},
		{
			name: "ivfflat's build parameter",
			caps: capability.YDB262(),
			node: &ast.IndexNode{Name: "by_emb", Table: "t", Columns: []string{"emb"}, Type: "vector_kmeans_tree", Vector: vectorSpec(),
				StorageParams: map[string]string{"lists": "100"}},
			wantErr: `index "by_emb": storage parameter "lists" belongs to pgvector's ivfflat index; .*`,
		},
		{
			name: "a half-precision operator class",
			caps: capability.YDB262(),
			node: &ast.IndexNode{Name: "by_emb", Table: "t", Columns: []string{"emb"}, Type: "vector_kmeans_tree",
				Operator: "halfvec_cosine_ops", Vector: &ast.VectorIndexSpec{VectorType: "float", Dimension: 3, Levels: 1, Clusters: 2}},
			wantErr: `index "by_emb": operator class "halfvec_cosine_ops" has no YDB counterpart: .*`,
		},
		{
			name:    "a half-precision vector column",
			caps:    capability.YDB262(),
			node:    withIndex(vectorIndex(vectorSpec()), ast.NewColumn("emb", "halfvec(3)")),
			wantErr: `column "emb" of table "t": halfvec\(3\) has no YDB counterpart: YDB has no half-precision vector; .*`,
		},
		{
			name:    "a vector index without settings",
			caps:    capability.YDB262(),
			node:    &ast.IndexNode{Name: "by_emb", Table: "t", Columns: []string{"emb"}, Type: "vector_kmeans_tree"},
			wantErr: `index "by_emb": a vector_kmeans_tree index declares none of its settings; .*`,
		},
		{
			name: "a vector index missing its depth",
			caps: capability.YDB262(),
			node: &ast.IndexNode{Name: "by_emb", Table: "t", Columns: []string{"emb"}, Type: "vector_kmeans_tree",
				Vector: &ast.VectorIndexSpec{Distance: "cosine", VectorType: "float", Dimension: 3, Clusters: 2}},
			wantErr: `index "by_emb": a vector index's levels are between 1 and 16 \(.Invalid levels: 0 should be between 1 and 16.\)`,
		},
		{
			name: "a unique vector index",
			caps: capability.YDB262(),
			node: withIndex(&ast.IndexNode{Name: "by_emb", Columns: []string{"emb"}, Type: "vector_kmeans_tree", Unique: true,
				Vector: vectorSpec()}, ast.NewColumn("emb", "BYTEA")),
			wantErr: `index "by_emb": a vector index is not unique \(.VECTOR_KMEANS_TREE index can only be GLOBAL \[SYNC\].\)`,
		},
		{
			name: "a vector index's partitioning",
			caps: capability.YDB262(),
			node: &ast.IndexNode{Name: "by_emb", Table: "t", Columns: []string{"emb"}, Type: "vector_kmeans_tree", Vector: vectorSpec(),
				Partitioning: &ast.IndexPartitioningSpec{MinPartitions: 3}},
			wantErr: `index "by_emb": a vector index keeps the partitioning YDB gives it ` +
				`\(.ALTER INDEX \.\.\. SET. answers .Only index with one impl table is supported.\)`,
		},
		{
			name: "vector settings on a global index",
			caps: capability.YDB262(),
			node: &ast.IndexNode{Name: "by_emb", Table: "t", Columns: []string{"emb"}, Vector: vectorSpec()},
			wantErr: `index "by_emb": it declares vector settings and is a sync index; ` +
				`declare type "vector_kmeans_tree" for a vector index`,
		},
		{
			name: "a text vector column",
			caps: capability.YDB262(),
			node: withIndex(vectorIndex(vectorSpec()), ast.NewColumn("emb", "TEXT")),
			wantErr: `index "by_emb" on table "t": its vector column "emb" is Utf8, and a YDB vector index reads a String column ` +
				"\\(`Embedding column 'emb' expected type 'String' but got Utf8`\\)",
		},
		{
			name: "a vector column of another dimension",
			caps: capability.YDB262(),
			node: withIndex(vectorIndex(vectorSpec()), ast.NewColumn("emb", "vector(4)")),
			wantErr: `index "by_emb" on table "t": its vector column "emb" is declared with dimension 4 and the index ` +
				`with vector_dimension 3; .*`,
		},
		{
			name:    "a prefix YDB cannot order",
			caps:    capability.YDB262(),
			node:    withIndex(vectorIndex(vectorSpec(), "score", "emb"), ast.NewColumn("score", "REAL"), ast.NewColumn("emb", "BYTEA")),
			wantErr: `index "by_emb" on table "t": column "score" is Float, which YDB refuses as an index key`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydb.NewWithCapabilities(test.caps).Render(test.node)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(got, qt.Equals, "")
		})
	}
}
