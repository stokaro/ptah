package clickhouse_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
)

// clickHouseIndex builds a minimal secondary index carrying one declared type.
// The table and the column are there so the render fails, when it fails, on the
// type rather than on a missing target.
func clickHouseIndex(indexType string) *ast.IndexNode {
	return &ast.IndexNode{
		Name:    "idx_docs_tags",
		Table:   "docs",
		Columns: []string{"tags"},
		Type:    indexType,
	}
}

// TestRenderSQL_RefusesAForeignIndexAccessMethod_FailurePath measures the defect
// in stokaro/ptah#3060.
//
// An index's type reaches this target through the field the PostgreSQL and
// MySQL families fill, so `USING GIN` in a SQL source arrives indistinguishable
// from a data-skipping type authored for ClickHouse. Rendering it produced
// syntactically valid ClickHouse the server refuses.
//
// Measured on ClickHouse 25.8.33.6: `ALTER TABLE docs ADD INDEX idx tags TYPE
// GIN GRANULARITY 8192` is refused with `Code: 80 ... Unknown Index type 'gin'.
// Available index types: text, vector_similarity, hypothesis, bloom_filter,
// tokenbf_v1, ngrambf_v1, set, minmax`, and the index is not added. Every name
// below was measured refused the same way.
func TestRenderSQL_RefusesAForeignIndexAccessMethod_FailurePath(t *testing.T) {
	tests := []struct {
		name      string
		indexType string
	}{
		{name: "the PostgreSQL default", indexType: "BTREE"},
		{name: "a different case", indexType: "btree"},
		{name: "the method that made this visible", indexType: "GIN"},
		{name: "GiST", indexType: "GIST"},
		{name: "SP-GiST", indexType: "SPGIST"},
		{name: "BRIN", indexType: "BRIN"},
		{name: "the PostgreSQL bloom extension", indexType: "bloom"},
		{name: "a method both families have", indexType: "HASH"},
		{name: "a MySQL index kind", indexType: "FULLTEXT"},
		{name: "the other MySQL index kind", indexType: "SPATIAL"},
		{name: "the MySQL spatial method", indexType: "RTREE"},
		// A ClickHouse type takes arguments, so a foreign method written with
		// them is the shape this target expects and the base name still has to
		// be read out of it.
		{name: "parameterized", indexType: "gin(2)"},
		{name: "surrounded by space", indexType: "  GIN  "},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			sql, err := renderer.RenderSQL(platform.ClickHouse, clickHouseIndex(test.indexType))

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, `(?s).*names a PostgreSQL or MySQL access method.*`)
			c.Assert(sql, qt.Equals, "")
		})
	}
}

// TestRenderSQL_KeepsAClickHouseIndexType_HappyPath is the control that the
// refusal reads the name rather than refusing every declared type.
//
// Each type below was accepted by ClickHouse 25.8.33.6, and `minmax` is what an
// index with no declared type renders as, so a refusal here would break the
// default too.
func TestRenderSQL_KeepsAClickHouseIndexType_HappyPath(t *testing.T) {
	tests := []struct {
		name      string
		indexType string
		want      string
	}{
		{name: "the default", indexType: "", want: "TYPE minmax"},
		{name: "minmax", indexType: "minmax", want: "TYPE minmax"},
		{name: "set", indexType: "set(100)", want: "TYPE set(100)"},
		{name: "bloom filter", indexType: "bloom_filter", want: "TYPE bloom_filter"},
		{name: "token bloom filter", indexType: "tokenbf_v1", want: "TYPE tokenbf_v1"},
		{name: "ngram bloom filter", indexType: "ngrambf_v1", want: "TYPE ngrambf_v1"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			sql, err := renderer.RenderSQL(platform.ClickHouse, clickHouseIndex(test.indexType))

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, test.want)
		})
	}
}

// TestRenderSQL_KeepsAnIndexTypeThisRepositoryDoesNotKnow is what makes the
// pinned list safe.
//
// ClickHouse's index-type set moves between releases -- an inverted index was
// renamed and a vector index added -- so the refusal names the closed
// PostgreSQL and MySQL sets rather than allow-listing ClickHouse's. A type this
// repository has never heard of therefore renders, which is the property that
// keeps a future release working without a change here.
func TestRenderSQL_KeepsAnIndexTypeThisRepositoryDoesNotKnow(t *testing.T) {
	c := qt.New(t)

	sql, err := renderer.RenderSQL(platform.ClickHouse, clickHouseIndex("quantum_index"))

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "TYPE quantum_index")
}
