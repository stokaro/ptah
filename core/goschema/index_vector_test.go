package goschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/goschema"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbschema"
)

// declaredVector is the YDB owner's vector index facet an index carries, or
// nil where it carries none.
func declaredVector(c *qt.C, index schemamodel.Index) *ydbschema.DesiredVectorIndex {
	c.Helper()
	value, _, err := schemaext.FacetAs[*ydbschema.DesiredVectorIndex](index.Facets, ydbschema.VectorIndexKind)
	c.Assert(err, qt.IsNil)
	return value
}

// vectorIndexSource is an entity whose vector column carries an index with
// attributes.
func vectorIndexSource(attributes string) string {
	return `package entities

//ptah:schema:table name="docs"
type Doc struct {
	//ptah:schema:field name="id" type="BIGINT" primary="true"
	ID int64
	//ptah:schema:field name="emb" type="vector(3)"
	//ptah:schema:index name="idx_docs_emb" fields="emb" ` + attributes + `
	Emb []byte
}
`
}

// TestParseSource_VectorIndex_HappyPath reads the settings of a YDB vector
// index, spelled as YDB's WITH (...) clause spells them, into the YDB owner's
// facet, with the names YDB reads in any case kept in lower case. A vector
// index stating no setting still carries the facet, so the stage that builds
// it refuses it, and a pgvector operator class states the metric it names.
func TestParseSource_VectorIndex_HappyPath(t *testing.T) {
	tests := []struct {
		name       string
		attributes string
		want       *ydbschema.DesiredVectorIndex
	}{
		{name: "none", attributes: `type="async"`, want: nil},
		{name: "a vector index stating none", attributes: `type="vector_kmeans_tree"`, want: &ydbschema.DesiredVectorIndex{}},
		{
			name:       "a metric through an operator class",
			attributes: `type="vector_kmeans_tree" ops="vector_l2_ops" vector_type="float" vector_dimension="3" levels="1" clusters="2"`,
			want:       &ydbschema.DesiredVectorIndex{Distance: "euclidean", VectorType: "float", Dimension: 3, Levels: 1, Clusters: 2},
		},
		{
			name: "every setting",
			attributes: `type="vector_kmeans_tree" distance="Cosine" vector_type="FLOAT" vector_dimension="3" ` +
				`levels="2" clusters="128"`,
			want: &ydbschema.DesiredVectorIndex{Distance: "cosine", VectorType: "float", Dimension: 3, Levels: 2, Clusters: 128},
		},
		{
			name:       "a similarity",
			attributes: `type="vector_kmeans_tree" similarity="inner_product" vector_type="int8" vector_dimension="8" levels="1" clusters="2"`,
			want:       &ydbschema.DesiredVectorIndex{Similarity: "inner_product", VectorType: "int8", Dimension: 8, Levels: 1, Clusters: 2},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := goschema.ParseSource(noOwners, "docs.go", vectorIndexSource(test.attributes))
			c.Assert(err, qt.IsNil)
			c.Assert(db.Indexes, qt.HasLen, 1)
			c.Assert(declaredVector(c, db.Indexes[0]), qt.DeepEquals, test.want)
		})
	}
}

// TestParseSource_VectorIndex_FailurePath refuses a value YDB would refuse
// where it was written.
func TestParseSource_VectorIndex_FailurePath(t *testing.T) {
	tests := []struct {
		name       string
		attributes string
		wantErr    string
	}{
		{name: "inner product as a distance", attributes: `distance="inner_product"`,
			wantErr: `.*invalid distance "inner_product": write one of cosine, euclidean, manhattan on //ptah:schema:index at Doc`},
		{name: "an element type YDB does not have", attributes: `vector_type="half"`,
			wantErr: `.*invalid vector_type "half": write one of float, uint8, int8, bit on //ptah:schema:index at Doc`},
		{name: "a dimension of zero", attributes: `vector_dimension="0"`,
			wantErr: `.*invalid vector_dimension "0": write a whole number of at least 1; .*`},
		{name: "levels that are no number", attributes: `levels="deep"`,
			wantErr: `.*invalid levels "deep": .*`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := goschema.ParseSource(noOwners, "docs.go", vectorIndexSource(test.attributes))
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidAttributeValue)
			c.Assert(db, qt.DeepEquals, schemamodel.Database{})
		})
	}
}

// TestParseSource_VectorIndexOperatorClass_FailurePath refuses an operator
// class no YDB vector index can be built from, where it was written: one with
// no YDB counterpart, and one naming another metric than the settings.
func TestParseSource_VectorIndexOperatorClass_FailurePath(t *testing.T) {
	tests := []struct {
		name       string
		attributes string
		wantErr    string
	}{
		{name: "a half-precision class", attributes: `type="vector_kmeans_tree" ops="halfvec_cosine_ops" vector_type="float"`,
			wantErr: `index "idx_docs_emb" at Doc: operator class "halfvec_cosine_ops" has no YDB counterpart: .*`},
		{name: "a class naming another metric", attributes: `type="vector_kmeans_tree" ops="vector_ip_ops" distance="cosine"`,
			wantErr: `index "idx_docs_emb" at Doc: operator class "vector_ip_ops" names similarity=inner_product, and the index's settings name distance=cosine`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := goschema.ParseSource(noOwners, "docs.go", vectorIndexSource(test.attributes))
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(db, qt.DeepEquals, schemamodel.Database{})
		})
	}
}
