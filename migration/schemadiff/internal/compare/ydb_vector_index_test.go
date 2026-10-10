package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/migration/schemadiff/difftypes"
)

// builtVector is the settings of the index the tests below hold.
func builtVector() ydbschema.VectorSettings {
	return ydbschema.VectorSettings{Distance: "cosine", VectorType: "float", Dimension: 3, Levels: 2, Clusters: 128}
}

// heldVector is a vector index as the YDB reader reports it: the clause in
// Method and the settings the server built it with as the owner's facet.
func heldVector(settings ydbschema.VectorSettings, columns ...string) catalog.Index {
	held := ydbIndex("GLOBAL USING vector_kmeans_tree", false, columns, nil)
	held.Facets = must.Must(schemaext.NewFacets(new(ydbschema.ObservedVectorIndex(settings))))
	return held
}

// declaredVector is a vector index as a declaration writes it, its settings
// the owner's facet.
func declaredVector(settings ydbschema.VectorSettings, columns ...string) schemamodel.Index {
	declared := ydbDeclaredIndex("vector_kmeans_tree", false, columns, nil)
	declared.Facets = must.Must(schemaext.NewFacets(new(ydbschema.DesiredVectorIndex(settings))))
	return declared
}

// TestIndexes_YDBKeepsAVectorIndexWhoseSettingsChange leaves a vector index
// whose settings alone differ to its owner: this comparison cannot read the
// settings, and the YDB owner's comparison plans the rebuild.
func TestIndexes_YDBKeepsAVectorIndexWhoseSettingsChange(t *testing.T) {
	c := qt.New(t)
	retuned := builtVector()
	retuned.Clusters = 64

	diff := compareYDBIndexes([]schemamodel.Index{declaredVector(retuned, "emb")}, []catalog.Index{heldVector(builtVector(), "emb")},
		capability.YDB262())

	c.Assert(diff.IndexAdditions(), qt.HasLen, 0)
	c.Assert(diff.IndexRemovals(), qt.HasLen, 0)
	c.Assert(diff.IndexPartitioningChanged, qt.HasLen, 0)
}

// TestIndexes_YDBRebuildsAChangedVectorIndex rebuilds a vector index whose
// columns or kind changed, because YDB changes none of them in place: a DROP
// INDEX and an ADD INDEX of the same name, which builds the index with the
// declared settings.
func TestIndexes_YDBRebuildsAChangedVectorIndex(t *testing.T) {
	tests := []struct {
		name     string
		desired  schemamodel.Index
		database catalog.Index
	}{
		{name: "a prefix added", desired: declaredVector(builtVector(), "gen", "emb"),
			database: heldVector(builtVector(), "emb")},
		{name: "a global index made a vector index", desired: declaredVector(builtVector(), "emb"),
			database: ydbIndex("GLOBAL SYNC", false, []string{"emb"}, nil)},
		{name: "a vector index made a global one", desired: ydbDeclaredIndex("", false, []string{"emb"}, nil),
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

// TestIndexes_YDBRebuildsARenamedVectorIndex leaves a vector index whose name
// changed as a drop and a create, which builds it with the declared settings:
// a rename would keep the settings the index was built with, and the owner
// compares settings only for an index both sides hold under one name. A
// plain index whose name alone changed is the control, and is renamed.
func TestIndexes_YDBRebuildsARenamedVectorIndex(t *testing.T) {
	c := qt.New(t)
	renamed := namedIndex(declaredVector(builtVector(), "emb"), "by_emb")
	plain := namedIndex(ydbDeclaredIndex("", false, []string{"body"}, nil), "by_body")

	diff := compareYDBIndexes(
		[]schemamodel.Index{renamed, plain},
		[]catalog.Index{
			namedCatalogIndex(heldVector(builtVector(), "emb"), "old_emb"),
			namedCatalogIndex(ydbIndex("GLOBAL SYNC", false, []string{"body"}, nil), "old_body"),
		},
		capability.YDB262(),
	)

	c.Assert(diff.IndexesRenamed, qt.DeepEquals, []difftypes.IndexRename{{TableName: "t", From: "old_body", To: "by_body"}})
	c.Assert(diff.IndexAdditions(), qt.DeepEquals, []difftypes.IndexRef{{Name: "by_emb", TableName: "t"}})
	c.Assert(diff.IndexRemovals(), qt.DeepEquals, []difftypes.IndexRef{{Name: "old_emb", TableName: "t"}})
}
