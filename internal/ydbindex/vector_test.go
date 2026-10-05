package ydbindex_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/internal/ydbindex"
	"ptah.run/internal/ydbpartition"
)

// complete is a vector index declaration YDB builds on every line that has
// vector indexes.
func complete() *ast.VectorIndexSpec {
	return &ast.VectorIndexSpec{Distance: "cosine", VectorType: "float", Dimension: 3, Levels: 2, Clusters: 128}
}

func TestParseVectorDeclaration_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		values map[string]string
		want   *ast.VectorIndexSpec
	}{
		{name: "no vector attribute", values: map[string]string{"name": "i", "type": "async"}, want: nil},
		{
			name: "every setting, in the case YDB reads",
			values: map[string]string{
				"distance": "COSINE", "vector_type": " Float ", "vector_dimension": "3", "levels": "2", "clusters": "128",
				"name": "i",
			},
			want: complete(),
		},
		{
			name:   "a similarity, and a setting left for resolution to refuse",
			values: map[string]string{"similarity": "inner_product", "vector_type": "bit"},
			want:   &ast.VectorIndexSpec{Similarity: "inner_product", VectorType: "bit"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydbindex.ParseVectorDeclaration(test.values)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// TestParseVectorDeclaration_FailurePath refuses a value YDB would refuse
// where it was written, naming the attribute; YDB's answer to each is quoted
// on the value lists.
func TestParseVectorDeclaration_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		values   map[string]string
		wantAttr string
		wantErr  string
	}{
		{name: "inner product is a similarity", values: map[string]string{"distance": "inner_product"},
			wantAttr: "distance", wantErr: `invalid distance "inner_product": write one of cosine, euclidean, manhattan`},
		{name: "euclidean is a distance", values: map[string]string{"similarity": "euclidean"},
			wantAttr: "similarity", wantErr: `invalid similarity "euclidean": write one of inner_product, cosine`},
		{name: "double is no element type", values: map[string]string{"vector_type": "double"},
			wantAttr: "vector_type", wantErr: `invalid vector_type "double": write one of float, uint8, int8, bit`},
		{name: "a dimension of zero", values: map[string]string{"vector_dimension": "0"},
			wantAttr: "vector_dimension", wantErr: `invalid vector_dimension "0": write a whole number of at least 1; .*`},
		{name: "levels that are no number", values: map[string]string{"levels": "two"},
			wantAttr: "levels", wantErr: `invalid levels "two": .*`},
		{name: "negative clusters", values: map[string]string{"clusters": "-2"},
			wantAttr: "clusters", wantErr: `invalid clusters "-2": .*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydbindex.ParseVectorDeclaration(test.values)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			var declaration *ydbpartition.DeclarationError
			c.Assert(err, qt.ErrorAs, &declaration)
			c.Assert(declaration.Attribute, qt.Equals, test.wantAttr)
			c.Assert(got, qt.IsNil)
		})
	}
}

// TestResolveVector_HappyPath pins how a declaration reads as the settings it
// builds, a pgvector operator class included, and the limits YDB 25.3 and
// later accept at their edges.
func TestResolveVector_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		spec     *ast.VectorIndexSpec
		operator string
		want     ast.VectorIndexSpec
	}{
		{name: "a complete declaration", spec: complete(), want: *complete()},
		{name: "a metric in capitals is kept in lower case",
			spec: &ast.VectorIndexSpec{Similarity: "COSINE", VectorType: "INT8", Dimension: 8, Levels: 1, Clusters: 2},
			want: ast.VectorIndexSpec{Similarity: "cosine", VectorType: "int8", Dimension: 8, Levels: 1, Clusters: 2}},
		{name: "pgvector's cosine class is the cosine distance",
			spec: &ast.VectorIndexSpec{VectorType: "float", Dimension: 3, Levels: 2, Clusters: 128}, operator: "vector_cosine_ops",
			want: *complete()},
		{name: "pgvector's L2 class is the Euclidean distance",
			spec: &ast.VectorIndexSpec{VectorType: "float", Dimension: 3, Levels: 2, Clusters: 128}, operator: "vector_l2_ops",
			want: ast.VectorIndexSpec{Distance: "euclidean", VectorType: "float", Dimension: 3, Levels: 2, Clusters: 128}},
		{name: "pgvector's L1 class is the Manhattan distance",
			spec: &ast.VectorIndexSpec{VectorType: "float", Dimension: 3, Levels: 2, Clusters: 128}, operator: "VECTOR_L1_OPS",
			want: ast.VectorIndexSpec{Distance: "manhattan", VectorType: "float", Dimension: 3, Levels: 2, Clusters: 128}},
		{name: "pgvector's inner product class is the inner product similarity",
			spec: &ast.VectorIndexSpec{VectorType: "float", Dimension: 3, Levels: 2, Clusters: 128}, operator: "vector_ip_ops",
			want: ast.VectorIndexSpec{Similarity: "inner_product", VectorType: "float", Dimension: 3, Levels: 2, Clusters: 128}},
		{name: "a class that agrees with the settings", spec: complete(), operator: "vector_cosine_ops", want: *complete()},
		{name: "the largest dimension and depth",
			spec: &ast.VectorIndexSpec{Distance: "cosine", VectorType: "float", Dimension: 16384, Levels: 15, Clusters: 4},
			want: ast.VectorIndexSpec{Distance: "cosine", VectorType: "float", Dimension: 16384, Levels: 15, Clusters: 4}},
		{name: "clusters to the power of levels at the limit",
			spec: &ast.VectorIndexSpec{Distance: "cosine", VectorType: "float", Dimension: 3, Levels: 3, Clusters: 1024},
			want: ast.VectorIndexSpec{Distance: "cosine", VectorType: "float", Dimension: 3, Levels: 3, Clusters: 1024}},
		{name: "the widest level",
			spec: &ast.VectorIndexSpec{Distance: "cosine", VectorType: "float", Dimension: 3, Levels: 2, Clusters: 2048},
			want: ast.VectorIndexSpec{Distance: "cosine", VectorType: "float", Dimension: 3, Levels: 2, Clusters: 2048}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydbindex.ResolveVector(test.spec, test.operator)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// TestResolveVector_FailurePath refuses what YDB 25.3 and later refuse, each
// with the server's answer.
func TestResolveVector_FailurePath(t *testing.T) {
	with := func(change func(*ast.VectorIndexSpec)) *ast.VectorIndexSpec {
		spec := complete()
		change(spec)
		return spec
	}
	tests := []struct {
		name     string
		spec     *ast.VectorIndexSpec
		operator string
		wantErr  string
	}{
		{name: "no settings", spec: nil,
			wantErr: `a vector_kmeans_tree index declares none of its settings; declare its distance or similarity, .*`},
		{name: "both metrics", spec: with(func(s *ast.VectorIndexSpec) { s.Similarity = "cosine" }),
			wantErr: `a vector index names one metric, distance or similarity, and this one names both \(.only one of distance or similarity should be set, not both.\)`},
		{name: "no metric", spec: with(func(s *ast.VectorIndexSpec) { s.Distance = "" }),
			wantErr: `a vector index names its metric with distance or similarity \(.either distance or similarity should be set.\)`},
		{name: "a distance YDB does not take", spec: with(func(s *ast.VectorIndexSpec) { s.Distance = "hamming" }),
			wantErr: `distance "hamming" is not one YDB takes \(.Invalid distance.\)`},
		{name: "a similarity YDB does not take", spec: with(func(s *ast.VectorIndexSpec) { s.Distance, s.Similarity = "", "euclidean" }),
			wantErr: `similarity "euclidean" is not one YDB takes \(.Invalid similarity.\)`},
		{name: "no element type", spec: with(func(s *ast.VectorIndexSpec) { s.VectorType = "" }),
			wantErr: `a vector index names its element type with vector_type \(.vector_type should be set.\)`},
		{name: "an element type YDB does not take", spec: with(func(s *ast.VectorIndexSpec) { s.VectorType = "half" }),
			wantErr: `vector_type "half" is not one YDB takes \(.Invalid vector_type.\)`},
		{name: "no dimension", spec: with(func(s *ast.VectorIndexSpec) { s.Dimension = 0 }),
			wantErr: `a vector index's vector_dimension is between 1 and 16384 \(.Invalid vector_dimension: 0 should be between 1 and 16384.\)`},
		{name: "a dimension over the limit", spec: with(func(s *ast.VectorIndexSpec) { s.Dimension = 16385 }),
			wantErr: `a vector index's vector_dimension is between 1 and 16384 \(.Invalid vector_dimension: 16385 .*`},
		{name: "no levels", spec: with(func(s *ast.VectorIndexSpec) { s.Levels = 0 }),
			wantErr: `a vector index's levels are between 1 and 16 \(.Invalid levels: 0 should be between 1 and 16.\)`},
		{name: "too many levels", spec: with(func(s *ast.VectorIndexSpec) { s.Levels = 17 }),
			wantErr: `a vector index's levels are between 1 and 16 \(.Invalid levels: 17 .*`},
		{name: "one cluster", spec: with(func(s *ast.VectorIndexSpec) { s.Clusters = 1 }),
			wantErr: `a vector index's clusters are between 2 and 2048 \(.Invalid clusters: 1 should be between 2 and 2048.\)`},
		{name: "too many clusters", spec: with(func(s *ast.VectorIndexSpec) { s.Clusters = 2049 }),
			wantErr: `a vector index's clusters are between 2 and 2048 \(.Invalid clusters: 2049 .*`},
		{name: "a tree past the limit", spec: with(func(s *ast.VectorIndexSpec) { s.Levels, s.Clusters = 3, 1025 }),
			wantErr: `a vector index's clusters to the power of its levels is at most 1073741824 \(.Invalid clusters\^levels: 1025\^3 should be less than 1073741824.\)`},
		{name: "a deep tree past the limit", spec: with(func(s *ast.VectorIndexSpec) { s.Levels, s.Clusters = 16, 2048 }),
			wantErr: `a vector index's clusters to the power of its levels is at most 1073741824 .*`},
		{name: "a half-precision class", spec: with(func(s *ast.VectorIndexSpec) { s.Distance = "" }), operator: "halfvec_cosine_ops",
			wantErr: `operator class "halfvec_cosine_ops" has no YDB counterpart: a YDB vector index names its metric .*`},
		{name: "a class the settings contradict", spec: complete(), operator: "vector_ip_ops",
			wantErr: `operator class "vector_ip_ops" names similarity=inner_product, and the index's settings name distance=cosine`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydbindex.ResolveVector(test.spec, test.operator)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(got, qt.Equals, ast.VectorIndexSpec{})
		})
	}
}

func TestVectorClause(t *testing.T) {
	tests := []struct {
		name string
		spec ast.VectorIndexSpec
		want string
	}{
		{name: "a distance", spec: *complete(),
			want: "WITH (distance=cosine, vector_type=float, vector_dimension=3, levels=2, clusters=128)"},
		{name: "a similarity", spec: ast.VectorIndexSpec{Similarity: "inner_product", VectorType: "bit", Dimension: 64, Levels: 1, Clusters: 2},
			want: "WITH (similarity=inner_product, vector_type=bit, vector_dimension=64, levels=1, clusters=2)"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbindex.VectorClause(test.spec), qt.Equals, test.want)
		})
	}
}

// TestVectorEqual pins what the comparison calls one vector index: the
// declaration as it resolves, against the settings the server reports.
func TestVectorEqual(t *testing.T) {
	described := complete()
	tests := []struct {
		name      string
		declared  *ast.VectorIndexSpec
		operator  string
		described *ast.VectorIndexSpec
		want      bool
	}{
		{name: "neither side is a vector index", want: true},
		{name: "the same settings", declared: complete(), described: described, want: true},
		{name: "a metric named by its pgvector class",
			declared: &ast.VectorIndexSpec{VectorType: "float", Dimension: 3, Levels: 2, Clusters: 128}, operator: "vector_cosine_ops",
			described: described, want: true},
		{name: "a metric in capitals",
			declared:  &ast.VectorIndexSpec{Distance: "Cosine", VectorType: "FLOAT", Dimension: 3, Levels: 2, Clusters: 128},
			described: described, want: true},
		{name: "a different width",
			declared:  &ast.VectorIndexSpec{Distance: "cosine", VectorType: "float", Dimension: 3, Levels: 2, Clusters: 64},
			described: described, want: false},
		{name: "a different metric of the same name",
			declared:  &ast.VectorIndexSpec{Similarity: "cosine", VectorType: "float", Dimension: 3, Levels: 2, Clusters: 128},
			described: described, want: false},
		{name: "a declaration that does not resolve", declared: &ast.VectorIndexSpec{Distance: "cosine"},
			described: described, want: false},
		{name: "settings on one side only", declared: complete(), want: false},
		{name: "an index the server built with settings Ptah was not asked for", described: described, want: false},
		{name: "a class with no settings and no index", operator: "vector_cosine_ops", want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbindex.VectorEqual(test.declared, test.operator, test.described), qt.Equals, test.want)
		})
	}
}

func TestStorageParameterRefusal(t *testing.T) {
	tests := []struct {
		name string
		want string
	}{
		{name: "m", want: `storage parameter "m" belongs to pgvector's hnsw index; a vector_kmeans_tree index is shaped by its levels and clusters`},
		{name: "ef_construction", want: `storage parameter "ef_construction" belongs to pgvector's hnsw index; .*`},
		{name: "LISTS", want: `storage parameter "LISTS" belongs to pgvector's ivfflat index; .*`},
		{name: "fillfactor", want: `storage parameter "fillfactor" has no YDB counterpart: a vector_kmeans_tree index takes its settings as .*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbindex.StorageParameterRefusal(test.name), qt.Matches, test.want)
		})
	}
}

// TestShapeRefusal_Vector pins how a vector index's columns are held to what
// YDB builds: a key of the table is a column it builds over, its last column
// holds String vectors of the index's dimension, and a prefix column is an
// ordered key like any other index's.
func TestShapeRefusal_Vector(t *testing.T) {
	columns := map[string]ydbindex.Column{
		"id":    {Type: "Uint64"},
		"gen":   {Type: "Uint64"},
		"emb":   {Type: "String", Dimension: 3},
		"raw":   {Type: "String"},
		"text":  {Type: "Utf8"},
		"score": {Type: "Float"},
	}
	column := func(name string) (ydbindex.Column, bool) {
		declared, ok := columns[name]
		return declared, ok
	}
	tests := []struct {
		name  string
		shape ydbindex.Shape
		key   []string
		want  string
	}{
		{name: "over the vector column", shape: ydbindex.Shape{Kind: ydbindex.Vector, Columns: []string{"emb"}, Dimension: 3},
			key: []string{"id"}, want: ""},
		{name: "a prefix and a cover", key: []string{"id"},
			shape: ydbindex.Shape{Kind: ydbindex.Vector, Columns: []string{"gen", "emb"}, Cover: []string{"text"}, Dimension: 3}, want: ""},
		{name: "a String column declared without a dimension", key: []string{"id"},
			shape: ydbindex.Shape{Kind: ydbindex.Vector, Columns: []string{"raw"}, Dimension: 1536}, want: ""},
		{name: "a vector column that is the table's key", key: []string{"raw"},
			shape: ydbindex.Shape{Kind: ydbindex.Vector, Columns: []string{"raw"}, Dimension: 3}, want: ""},
		{name: "a global index over the key", key: []string{"raw"},
			shape: ydbindex.Shape{Kind: ydbindex.Sync, Columns: []string{"raw"}},
			want:  "its columns are the table's key, which YDB refuses (`index keys shouldn't be table keys`)"},
		{name: "a text vector column", key: []string{"id"},
			shape: ydbindex.Shape{Kind: ydbindex.Vector, Columns: []string{"text"}, Dimension: 3},
			want: `its vector column "text" is Utf8, and a YDB vector index reads a String column ` +
				"(`Embedding column 'text' expected type 'String' but got Utf8`)"},
		{name: "a key column last", key: []string{"id"},
			shape: ydbindex.Shape{Kind: ydbindex.Vector, Columns: []string{"emb", "id"}, Dimension: 3},
			want: `its vector column "id" is Uint64, and a YDB vector index reads a String column ` +
				"(`Embedding column 'id' expected type 'String' but got Uint64`)"},
		{name: "a dimension the column does not have", key: []string{"id"},
			shape: ydbindex.Shape{Kind: ydbindex.Vector, Columns: []string{"emb"}, Dimension: 4},
			want: `its vector column "emb" is declared with dimension 3 and the index with vector_dimension 4; ` +
				"YDB checks neither, and leaves a vector of the wrong length out of the index"},
		{name: "a prefix YDB cannot order", key: []string{"id"},
			shape: ydbindex.Shape{Kind: ydbindex.Vector, Columns: []string{"score", "emb"}, Dimension: 3},
			want:  `column "score" is Float, which YDB refuses as an index key`},
		{name: "a covered key column", key: []string{"id"},
			shape: ydbindex.Shape{Kind: ydbindex.Vector, Columns: []string{"emb"}, Cover: []string{"id"}, Dimension: 3},
			want:  `it covers key column "id", which every YDB index carries already`},
		{name: "a column the table does not declare", key: []string{"id"},
			shape: ydbindex.Shape{Kind: ydbindex.Vector, Columns: []string{"missing"}, Dimension: 3},
			want:  `it names column "missing", which the table does not declare`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbindex.ShapeRefusal(test.shape, test.key, column), qt.Equals, test.want)
		})
	}
}
