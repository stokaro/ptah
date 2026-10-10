package modelast_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/modelast"
)

// fullyDeclaredIndex sets every field a schemamodel.Index carries that either
// converter can express, so a field the copying forgets shows up as a
// difference rather than as a zero both sides agree on.
func fullyDeclaredIndex() schemamodel.Index {
	nullsDistinct := true

	return schemamodel.Index{
		StructName:     "Doc",
		TableName:      "docs",
		Name:           "idx_docs_title",
		Fields:         []string{"title"},
		Unique:         true,
		Comment:        "lookup",
		Type:           "btree",
		Parser:         "ngram",
		Condition:      "archived = false",
		Operator:       "text_pattern_ops",
		IncludeColumns: []string{"body"},
		StorageParams:  map[string]string{"fillfactor": "70"},
		NullsDistinct:  &nullsDistinct,
		Facets: must.Must(schemaext.NewFacets(&chschema.DesiredIndex{
			Granularity: chschema.GranularitySetting{State: chschema.Explicit, Value: 4},
		})),
		Concurrently: true,
		Partitioning: &ast.IndexPartitioningSpec{ByLoad: new(true), MinPartitions: 3},
	}
}

// TestFromIndex_CarriesThePartitioning holds the hop from the schema model to
// the renderer to a YDB index's partitioning: dropped here, the index is built
// with YDB's defaults and the comparison then plans the settings again on every
// run. The node carries a copy, so a renderer cannot change the declaration it
// came from.
func TestFromIndex_CarriesThePartitioning(t *testing.T) {
	c := qt.New(t)
	index := schemamodel.Index{
		StructName: "Doc", TableName: "docs", Name: "idx_docs_title", Fields: []string{"title"},
		Partitioning: &ast.IndexPartitioningSpec{ByLoad: new(true), MinPartitions: 3},
	}

	node := modelast.FromIndex(index)
	c.Assert(node.Partitioning, qt.DeepEquals, index.Partitioning)
	*node.Partitioning.ByLoad = false
	c.Assert(*index.Partitioning.ByLoad, qt.IsTrue)
}

// TestFromIndex_CarriesTheVectorSettings holds the hop from the schema model
// to the renderer to a YDB vector index's settings, the owner's facet:
// dropped here, the renderer refuses the index as one that declares none.
func TestFromIndex_CarriesTheVectorSettings(t *testing.T) {
	c := qt.New(t)
	vector := &ydbschema.DesiredVectorIndex{Distance: "cosine", VectorType: "float", Dimension: 3, Levels: 1, Clusters: 2}
	index := schemamodel.Index{
		StructName: "Doc", TableName: "docs", Name: "idx_docs_emb", Fields: []string{"emb"}, Type: "vector_kmeans_tree",
		Facets: must.Must(schemaext.NewFacets(vector)),
	}

	node := modelast.FromIndex(index)
	carried, _, err := schemaext.FacetAs[*ydbschema.DesiredVectorIndex](node.Facets, ydbschema.VectorIndexKind)
	c.Assert(err, qt.IsNil)
	c.Assert(carried, qt.DeepEquals, vector)
}

// TestIndexConverters_CarryTheSameDeclaration is the guard the fix for
// stokaro/ptah#3042 needs to keep working.
//
// FromIndex and FromIndexWithTableMapping differ in how they resolve the table
// name and in nothing else. They were two hand-written copies of the same field
// list, and a list written twice stops agreeing the moment one of them grows:
// Concurrently was in neither, so a schema asking for a concurrent build
// rendered a locking one.
//
// The declaration below names the table itself, so both converters take the
// same branch and any difference in the result is a field one of them dropped.
func TestIndexConverters_CarryTheSameDeclaration(t *testing.T) {
	c := qt.New(t)

	index := fullyDeclaredIndex()

	direct := modelast.FromIndex(index)
	mapped := modelast.FromIndexWithTableMapping(index, map[string]string{"Doc": "ignored"})

	c.Assert(mapped, qt.DeepEquals, direct)
}

// TestFromIndex_CarriesTheConcurrentBuild is the reproduction from
// stokaro/ptah#3042 at the hop that lost it.
//
// The keyword survives the parser and the schema model and was dropped here, so
// nothing reached the renderer with it set. internal/txrequire reads the same
// field to route the statement out of a transaction block, which is what a
// concurrent build cannot run inside, so the loss took that decision with it.
func TestFromIndex_CarriesTheConcurrentBuild(t *testing.T) {
	tests := []struct {
		name         string
		concurrently bool
	}{
		{name: "declared", concurrently: true},
		{name: "not declared", concurrently: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			index := schemamodel.Index{
				StructName:   "Doc",
				TableName:    "docs",
				Name:         "idx_docs_title",
				Fields:       []string{"title"},
				Concurrently: test.concurrently,
			}

			c.Assert(modelast.FromIndex(index).Concurrently, qt.Equals, test.concurrently)
			c.Assert(modelast.FromIndexWithTableMapping(index, nil).Concurrently, qt.Equals, test.concurrently)
		})
	}
}
