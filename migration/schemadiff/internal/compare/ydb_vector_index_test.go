package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/difftypes"
)

// heldVector is a vector index as the YDB reader reports it: the clause in
// Method and the settings the server built it with.
func heldVector(spec ast.VectorIndexSpec, columns ...string) catalog.Index {
	held := ydbIndex("GLOBAL USING vector_kmeans_tree", false, columns, nil)
	held.Vector = &spec
	return held
}

// declaredVector is a vector index as a declaration writes it.
func declaredVector(spec *ast.VectorIndexSpec, operator string, columns ...string) schemamodel.Index {
	declared := ydbDeclaredIndex("vector_kmeans_tree", false, columns, nil)
	declared.Vector = spec
	declared.Operator = operator
	return declared
}

// builtVector is the settings of the index the tests below hold.
func builtVector() ast.VectorIndexSpec {
	return ast.VectorIndexSpec{Distance: "cosine", VectorType: "float", Dimension: 3, Levels: 2, Clusters: 128}
}

// TestIndexes_YDBKeepsAnUnchangedVectorIndex reads a declared vector index and
// the one the server built with the same settings as one index, whichever way
// the declaration names its metric and in whatever case.
func TestIndexes_YDBKeepsAnUnchangedVectorIndex(t *testing.T) {
	tests := []struct {
		name    string
		desired schemamodel.Index
	}{
		{name: "the same settings", desired: declaredVector(new(builtVector()), "", "emb")},
		{name: "the metric named by pgvector's class",
			desired: declaredVector(&ast.VectorIndexSpec{VectorType: "float", Dimension: 3, Levels: 2, Clusters: 128},
				"vector_cosine_ops", "emb")},
		{name: "settings in capitals",
			desired: declaredVector(&ast.VectorIndexSpec{Distance: "COSINE", VectorType: "Float", Dimension: 3, Levels: 2, Clusters: 128},
				"", "emb")},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := compareYDBIndexes([]schemamodel.Index{test.desired}, []catalog.Index{heldVector(builtVector(), "emb")},
				capability.YDB262())
			c.Assert(diff.IndexAdditions(), qt.HasLen, 0)
			c.Assert(diff.IndexRemovals(), qt.HasLen, 0)
			c.Assert(diff.IndexPartitioningChanged, qt.HasLen, 0)
		})
	}
}

// TestIndexes_YDBRebuildsAChangedVectorIndex rebuilds a vector index whose
// settings, columns or kind changed, because YDB changes none of them in
// place: a DROP INDEX and an ADD INDEX of the same name.
func TestIndexes_YDBRebuildsAChangedVectorIndex(t *testing.T) {
	with := func(change func(*ast.VectorIndexSpec)) *ast.VectorIndexSpec {
		spec := builtVector()
		change(&spec)
		return &spec
	}
	tests := []struct {
		name     string
		desired  schemamodel.Index
		database catalog.Index
	}{
		{name: "another distance", desired: declaredVector(with(func(s *ast.VectorIndexSpec) { s.Distance = "euclidean" }), "", "emb"),
			database: heldVector(builtVector(), "emb")},
		{name: "a similarity of the same name",
			desired:  declaredVector(with(func(s *ast.VectorIndexSpec) { s.Distance, s.Similarity = "", "cosine" }), "", "emb"),
			database: heldVector(builtVector(), "emb")},
		{name: "another element type", desired: declaredVector(with(func(s *ast.VectorIndexSpec) { s.VectorType = "int8" }), "", "emb"),
			database: heldVector(builtVector(), "emb")},
		{name: "another dimension", desired: declaredVector(with(func(s *ast.VectorIndexSpec) { s.Dimension = 4 }), "", "emb"),
			database: heldVector(builtVector(), "emb")},
		{name: "another depth", desired: declaredVector(with(func(s *ast.VectorIndexSpec) { s.Levels = 3 }), "", "emb"),
			database: heldVector(builtVector(), "emb")},
		{name: "another width", desired: declaredVector(with(func(s *ast.VectorIndexSpec) { s.Clusters = 64 }), "", "emb"),
			database: heldVector(builtVector(), "emb")},
		{name: "a pgvector class naming another metric", desired: declaredVector(
			&ast.VectorIndexSpec{VectorType: "float", Dimension: 3, Levels: 2, Clusters: 128}, "vector_l2_ops", "emb"),
			database: heldVector(builtVector(), "emb")},
		{name: "a prefix added", desired: declaredVector(new(builtVector()), "", "gen", "emb"),
			database: heldVector(builtVector(), "emb")},
		{name: "25.1's index without levels and clusters", desired: declaredVector(new(builtVector()), "", "emb"),
			database: heldVector(ast.VectorIndexSpec{Distance: "cosine", VectorType: "float", Dimension: 3}, "emb")},
		{name: "a global index made a vector index", desired: declaredVector(new(builtVector()), "", "emb"),
			database: ydbIndex("GLOBAL SYNC", false, []string{"emb"}, nil)},
		{name: "a vector index made a global one", desired: ydbDeclaredIndex("", false, []string{"emb"}, nil),
			database: heldVector(builtVector(), "emb")},
		{name: "a vector index declared without its settings", desired: declaredVector(nil, "", "emb"),
			database: heldVector(builtVector(), "emb")},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := compareYDBIndexes([]schemamodel.Index{test.desired}, []catalog.Index{test.database}, capability.YDB262())
			c.Assert(diff.IndexAdditions(), qt.DeepEquals, []difftypes.IndexRef{{Name: "i", TableName: "t"}})
			c.Assert(diff.IndexRemovals(), qt.DeepEquals, []difftypes.IndexRef{{Name: "i", TableName: "t"}})
		})
	}
}

// TestIndexes_YDBRenamesAVectorIndex pairs a vector index whose name alone
// changed into a rename, which keeps the built index (measured on 25.1.4.7 and
// 26.2.1.14: RENAME INDEX keeps its settings), and leaves one whose settings
// changed too as a drop and a create.
func TestIndexes_YDBRenamesAVectorIndex(t *testing.T) {
	c := qt.New(t)
	renamed := namedIndex(declaredVector(new(builtVector()), "", "emb"), "by_emb")
	retuned := namedIndex(declaredVector(&ast.VectorIndexSpec{Distance: "cosine", VectorType: "float", Dimension: 3,
		Levels: 2, Clusters: 64}, "", "body_emb"), "by_body")

	diff := compareYDBIndexes(
		[]schemamodel.Index{renamed, retuned},
		[]catalog.Index{
			namedCatalogIndex(heldVector(builtVector(), "emb"), "old_emb"),
			namedCatalogIndex(heldVector(builtVector(), "body_emb"), "old_body"),
		},
		capability.YDB262(),
	)

	c.Assert(diff.IndexesRenamed, qt.DeepEquals, []difftypes.IndexRename{{TableName: "t", From: "old_emb", To: "by_emb"}})
	c.Assert(diff.IndexAdditions(), qt.DeepEquals, []difftypes.IndexRef{{Name: "by_body", TableName: "t"}})
	c.Assert(diff.IndexRemovals(), qt.DeepEquals, []difftypes.IndexRef{{Name: "old_body", TableName: "t"}})
}
