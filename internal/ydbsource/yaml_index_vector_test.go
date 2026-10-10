package ydbsource_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/yamlschema"
	"ptah.run/dialect/ydb/ydbschema"
)

// TestParse_YDBVectorIndex_HappyPath reads a YDB vector index in YAML: its
// method under type, and its settings under the keys the annotation reads,
// with the same spellings, into the YDB owner's facet.
func TestParse_YDBVectorIndex_HappyPath(t *testing.T) {
	c := qt.New(t)

	db, err := yamlschema.Parse(ydbYAMLOwners, []byte(`
tables:
  docs:
    columns:
      id:
        type: bigint
        primary: true
      gen:
        type: bigint
      emb:
        type: vector(3)
    indexes:
      idx_docs_emb:
        fields: [gen, emb]
        type: vector_kmeans_tree
        distance: COSINE
        vector_type: float
        vector_dimension: 3
        levels: 2
        clusters: 128
`))

	c.Assert(err, qt.IsNil)
	c.Assert(db.Indexes, qt.HasLen, 1)
	vector, found, err := schemaext.FacetAs[*ydbschema.DesiredVectorIndex](db.Indexes[0].Facets, ydbschema.VectorIndexKind)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(vector, qt.DeepEquals, &ydbschema.DesiredVectorIndex{Distance: "cosine", VectorType: "float", Dimension: 3, Levels: 2, Clusters: 128})
	db.Indexes[0].Facets = schemaext.Facets{}
	c.Assert(db.Indexes, qt.DeepEquals, []schemamodel.Index{
		{StructName: "docs", TableName: "docs", Name: "idx_docs_emb", Fields: []string{"gen", "emb"}, Type: "vector_kmeans_tree"},
	})
}

// TestParse_YDBVectorIndex_FailurePath refuses a vector setting YDB would
// refuse, an empty one included.
func TestParse_YDBVectorIndex_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		index   string
		wantErr string
	}{
		{name: "euclidean as a similarity", index: "similarity: euclidean",
			wantErr: `.*index "i": invalid similarity "euclidean": write one of inner_product, cosine`},
		{name: "an empty element type", index: `vector_type: ""`,
			wantErr: `.*index "i": invalid vector_type "": write one of float, uint8, int8, bit`},
		{name: "clusters of zero", index: "clusters: 0",
			wantErr: `.*index "i": invalid clusters "0": write a whole number of at least 1; .*`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := yamlschema.Parse(ydbYAMLOwners, []byte(`
tables:
  docs:
    columns:
      id:
        type: bigint
        primary: true
      emb:
        type: vector(3)
    indexes:
      i:
        fields: [emb]
        type: vector_kmeans_tree
        `+test.index+`
`))
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(db, qt.IsNil)
		})
	}
}
