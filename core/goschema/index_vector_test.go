package goschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/goschema"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
)

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
// index, spelled as YDB's WITH (...) clause spells them, into the index's
// spec, with the names YDB reads in any case kept in lower case.
func TestParseSource_VectorIndex_HappyPath(t *testing.T) {
	tests := []struct {
		name       string
		attributes string
		want       *ast.VectorIndexSpec
	}{
		{name: "none", attributes: `type="async"`, want: nil},
		{
			name: "every setting",
			attributes: `type="vector_kmeans_tree" distance="Cosine" vector_type="FLOAT" vector_dimension="3" ` +
				`levels="2" clusters="128"`,
			want: &ast.VectorIndexSpec{Distance: "cosine", VectorType: "float", Dimension: 3, Levels: 2, Clusters: 128},
		},
		{
			name:       "a similarity",
			attributes: `type="vector_kmeans_tree" similarity="inner_product" vector_type="int8" vector_dimension="8" levels="1" clusters="2"`,
			want:       &ast.VectorIndexSpec{Similarity: "inner_product", VectorType: "int8", Dimension: 8, Levels: 1, Clusters: 2},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := goschema.ParseSource("docs.go", vectorIndexSource(test.attributes))
			c.Assert(err, qt.IsNil)
			c.Assert(db.Indexes, qt.HasLen, 1)
			c.Assert(db.Indexes[0].Vector, qt.DeepEquals, test.want)
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
			db, err := goschema.ParseSource("docs.go", vectorIndexSource(test.attributes))
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidAttributeValue)
			c.Assert(db, qt.DeepEquals, schemamodel.Database{})
		})
	}
}
