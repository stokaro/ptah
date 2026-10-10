package ydbplan_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbplan"
	"ptah.run/dialect/ydb/ydbschema"
)

func heldSettings() ydbschema.VectorSettings {
	return ydbschema.VectorSettings{Distance: "cosine", VectorType: "float", Dimension: 3, Levels: 1, Clusters: 2}
}

// vectorPlanRequest asks for the change of index by_emb of table docs, which
// both sides hold, to clusters, the index declared as indexType, with common
// steps the host plans beside it.
func vectorPlanRequest(indexType string, clusters uint64, common ...featureplan.CommonStep) featureplan.Request {
	semantics := identifier.ForDialect("ydb")
	builder := objectidentity.NewBuilder(semantics)
	desired := heldSettings()
	desired.Clusters = clusters
	declaration := schemacapture.TableDeclaration{
		Table: schemamodel.Table{StructName: "D", Name: "docs"},
		Fields: []schemamodel.Field{
			{StructName: "D", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "D", Name: "emb", Type: "vector(3)", Nullable: true},
		},
		Indexes: []schemamodel.Index{{StructName: "D", Name: "by_emb", TableName: "docs", Fields: []string{"emb"}, Type: indexType}},
	}
	observation := schemacapture.TableObservation{
		Table:   catalog.Table{Name: "docs", Columns: []catalog.Column{{Name: "id", DataType: "Int64"}, {Name: "emb", DataType: "String"}}},
		Indexes: []catalog.Index{{Name: "by_emb", TableName: "docs", Columns: []string{"emb"}, Method: "GLOBAL USING vector_kmeans_tree"}},
	}
	return featureplan.Request{
		Target: "ydb", Identifiers: semantics, Capabilities: capability.YDB262(),
		Tables: []featureplan.Table{{Subject: builder.Table("docs"), Desired: declaration, Current: observation}},
		Changes: []schemaext.ChangeRecord{{Subject: builder.IndexParts("", "docs", "by_emb"), Value: &ydbdiff.VectorIndex{
			Before: new(ydbschema.ObservedVectorIndex(heldSettings())), After: new(ydbschema.DesiredVectorIndex(desired)),
		}}},
		CommonSteps: common,
	}
}

// TestVectorIndexService_PlansARebuild_HappyPath drops a vector index whose
// settings change and adds it again with the declared settings, the drop
// first.
func TestVectorIndexService_PlansARebuild_HappyPath(t *testing.T) {
	c := qt.New(t)

	result, err := ydbplan.VectorIndexService{}.PlanFeatures(c.Context(), vectorPlanRequest("vector_kmeans_tree", 4))

	c.Assert(err, qt.IsNil)
	c.Assert(result.Diagnostics, qt.HasLen, 0)
	c.Assert(result.Contributions, qt.HasLen, 1)
	steps := result.Contributions[0].Steps
	c.Assert(steps, qt.HasLen, 2)
	c.Assert(steps[0].Payload.Payload, qt.DeepEquals, &ydbast.DropVectorIndex{Name: "by_emb"})
	want := heldSettings()
	want.Clusters = 4
	c.Assert(steps[1].Payload.Payload, qt.DeepEquals, &ydbast.AddVectorIndex{Name: "by_emb", Columns: []string{"emb"}, Settings: want})
	c.Assert(result.Contributions[0].Dependencies, qt.DeepEquals, []plangraph.Dependency{{Before: steps[0].ID, After: steps[1].ID}})
}

// TestVectorIndexService_LeavesACommonReplacementAlone plans nothing of its
// own for an index the common plan drops and adds again, which builds it with
// the declared settings.
func TestVectorIndexService_LeavesACommonReplacementAlone(t *testing.T) {
	c := qt.New(t)
	index := objectidentity.NewBuilder(identifier.ForDialect("ydb")).IndexParts("", "docs", "by_emb")
	common := []featureplan.CommonStep{
		{ID: plangraph.StepID{Owner: "ptah.run/ydb", Name: "common/before/000000"}, Effects: []plangraph.Effect{{Subject: index, Action: plangraph.Drop}}},
		{ID: plangraph.StepID{Owner: "ptah.run/ydb", Name: "common/after/000000"}, Effects: []plangraph.Effect{{Subject: index, Action: plangraph.Create}}},
	}

	result, err := ydbplan.VectorIndexService{}.PlanFeatures(c.Context(), vectorPlanRequest("vector_kmeans_tree", 4, common...))

	c.Assert(err, qt.IsNil)
	c.Assert(result.Contributions, qt.HasLen, 0)
	c.Assert(result.Changes, qt.HasLen, 1)
	c.Assert(result.Changes[0].Strategy, qt.Equals, "the common index replacement builds the index again with the desired settings")
}

// TestVectorIndexService_PlansARebuild_FailurePath refuses, before any
// statement, a rebuild YDB would refuse once the old index is gone: vector
// settings on an index of another kind, and settings out of YDB's limits.
func TestVectorIndexService_PlansARebuild_FailurePath(t *testing.T) {
	tests := []struct {
		name      string
		indexType string
		clusters  uint64
		wantErr   string
	}{
		{name: "an index of another kind", indexType: "", clusters: 4,
			wantErr: `index "by_emb": it declares vector settings and is a sync index; declare type "vector_kmeans_tree" for a vector index`},
		{name: "one cluster", indexType: "vector_kmeans_tree", clusters: 1,
			wantErr: `index "by_emb": a vector index's clusters are between 2 and 2048 .*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := ydbplan.VectorIndexService{}.PlanFeatures(c.Context(), vectorPlanRequest(test.indexType, test.clusters))

			c.Assert(err, qt.IsNil)
			c.Assert(result.Contributions, qt.HasLen, 0)
			c.Assert(result.Diagnostics, qt.HasLen, 1)
			c.Assert(result.Diagnostics[0].Problem.Message, qt.Matches, test.wantErr)
		})
	}
}
