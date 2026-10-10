package generator_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/engine/builtin"
	"ptah.run/migration/generator"
	"ptah.run/migration/schemadiff"
)

// vectorDocs declares table docs with a vector column, and index by_emb over
// it as indexType with settings, the YDB owner's facet.
func vectorDocs(indexType string, settings *ydbschema.DesiredVectorIndex) *schemamodel.Database {
	index := schemamodel.Index{StructName: "Doc", Name: "by_emb", TableName: "docs", Fields: []string{"emb"}, Type: indexType}
	if settings != nil {
		index.Facets = must.Must(schemaext.NewFacets(settings))
	}
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Doc", Name: "docs"}},
		Fields: []schemamodel.Field{
			{StructName: "Doc", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "Doc", Name: "emb", Type: "vector(3)", Nullable: true},
		},
		Indexes:         []schemamodel.Index{index},
		FeatureCoverage: must.Must(ydbschema.VectorIndexCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil)),
	}
	schemamodel.Finalize(desired)
	return desired
}

// heldDocs is table docs as the YDB reader gives it, holding index by_emb of
// method with settings, or a synchronous index where settings is nil.
func heldDocs(settings *ydbschema.ObservedVectorIndex) *catalog.Database {
	index := catalog.Index{Name: "by_emb", TableName: "docs", Columns: []string{"emb"}, Method: "GLOBAL SYNC"}
	if settings != nil {
		index.Method = "GLOBAL USING vector_kmeans_tree"
		index.Facets = must.Must(must.Must(schemaext.NewFacets(settings)).WithTargetScope(ydbschema.VectorIndexKind, platform.YDB))
	}
	return &catalog.Database{
		Tables: []catalog.Table{{Name: "docs", Columns: []catalog.Column{
			{Name: "id", DataType: "Int64", IsPrimaryKey: true}, {Name: "emb", DataType: "String", IsNullable: "YES"},
		}}},
		Indexes:         []catalog.Index{index},
		Constraints:     []catalog.Constraint{{Name: "docs_pkey", TableName: "docs", Type: "PRIMARY KEY", ColumnName: "id", ColumnNames: []string{"id"}}},
		FeatureCoverage: must.Must(ydbschema.VectorIndexCoverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, nil)),
	}
}

// planVectorDocs plans desired against current on YDB 26.2, and renders both
// directions.
func planVectorDocs(c *qt.C, desired *schemamodel.Database, current *catalog.Database) (forward, reverse string) {
	c.Helper()
	runtime := must.Must(builtin.New())
	diff, err := schemadiff.CompareWithDatabaseInfo(c.Context(), desired, current, catalog.ServerInfo{Dialect: platform.YDB}, nil, runtime)
	c.Assert(err, qt.IsNil)
	plan, err := generator.PlanBidirectionalSchemaDiff(c.Context(),
		generator.BidirectionalSchemaPlanOptions{Runtime: runtime, Diff: diff, DesiredSchema: desired, CurrentSchema: current,
			Dialect: platform.YDB, Capabilities: capability.YDB262()})
	c.Assert(err, qt.IsNil)
	forward, err = builtin.RenderSQLWithCapabilities(platform.YDB, capability.YDB262(), plan.Forward.Nodes...)
	c.Assert(err, qt.IsNil)
	reverse, err = builtin.RenderSQLWithCapabilities(platform.YDB, capability.YDB262(), plan.Reverse.Nodes...)
	c.Assert(err, qt.IsNil)
	return forward, reverse
}

// TestPlanBidirectionalSchemaDiff_VectorIndexSettingsRebuild pins how a change
// of a vector index's settings is planned: YDB changes no setting of a built
// vector index, so the index is dropped and added again with the declared
// settings, and the rollback builds it again with the settings it held. A
// change of the index's kind is the common plan's rebuild, which carries the
// settings, so the owner adds no second one, and the plain index comes back
// on rollback.
func TestPlanBidirectionalSchemaDiff_VectorIndexSettingsRebuild(t *testing.T) {
	held := &ydbschema.ObservedVectorIndex{Distance: "cosine", VectorType: "float", Dimension: 3, Levels: 1, Clusters: 2}
	tests := []struct {
		name    string
		desired *schemamodel.Database
		current *catalog.Database
		forward string
		reverse string
	}{
		{
			name:    "another width",
			desired: vectorDocs("vector_kmeans_tree", &ydbschema.DesiredVectorIndex{Distance: "cosine", VectorType: "float", Dimension: 3, Levels: 1, Clusters: 4}),
			current: heldDocs(held),
			forward: "ALTER TABLE `docs` DROP INDEX `by_emb`;\n" +
				"ALTER TABLE `docs` ADD INDEX `by_emb` GLOBAL USING vector_kmeans_tree ON (`emb`) " +
				"WITH (distance=cosine, vector_type=float, vector_dimension=3, levels=1, clusters=4);\n",
			reverse: "-- Rollback of \"index docs.by_emb\": \"drop the index and add it again with its captured settings\".\n" +
				"-- Recovery limit: \"Building a vector index again reads every row of its table; searches through the index fail until it is built.\"\n" +
				"ALTER TABLE `docs` DROP INDEX `by_emb`;\n" +
				"ALTER TABLE `docs` ADD INDEX `by_emb` GLOBAL USING vector_kmeans_tree ON (`emb`) " +
				"WITH (distance=cosine, vector_type=float, vector_dimension=3, levels=1, clusters=2);\n",
		},
		{
			name:    "a plain index becoming a vector index",
			desired: vectorDocs("vector_kmeans_tree", held.Desired()),
			current: heldDocs(nil),
			forward: "ALTER TABLE `docs` DROP INDEX `by_emb`;\n" +
				"ALTER TABLE `docs` ADD INDEX `by_emb` GLOBAL USING vector_kmeans_tree ON (`emb`) " +
				"WITH (distance=cosine, vector_type=float, vector_dimension=3, levels=1, clusters=2);\n",
			reverse: "-- Rollback of \"index docs.by_emb\": \"drop the index and add it again with its captured settings\".\n" +
				"-- Recovery limit: \"Building a vector index again reads every row of its table; searches through the index fail until it is built.\"\n" +
				"ALTER TABLE `docs` DROP INDEX `by_emb`;\n" +
				"ALTER TABLE `docs` ADD INDEX `by_emb` GLOBAL SYNC ON (`emb`);\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			forward, reverse := planVectorDocs(c, test.desired, test.current)

			c.Assert(forward, qt.Equals, test.forward)
			c.Assert(reverse, qt.Equals, test.reverse)
		})
	}
}

// TestPlanBidirectionalSchemaDiff_VectorIndexSettingsUnchanged is the control:
// a declaration naming the settings the index holds plans nothing, and so
// does one naming its metric through a pgvector operator class.
func TestPlanBidirectionalSchemaDiff_VectorIndexSettingsUnchanged(t *testing.T) {
	c := qt.New(t)
	held := &ydbschema.ObservedVectorIndex{Distance: "cosine", VectorType: "float", Dimension: 3, Levels: 1, Clusters: 2}

	forward, reverse := planVectorDocs(c, vectorDocs("vector_kmeans_tree", held.Desired()), heldDocs(held))

	c.Assert(forward, qt.Equals, "")
	c.Assert(reverse, qt.Equals, "")
}

// TestCompareWithDatabaseInfo_VectorIndexSettings_FailurePath refuses, before
// any statement is planned, a settings change YDB would refuse once the old
// index is gone: settings that do not resolve, vector settings on an index of
// another kind, and a dimension the vector column does not have. The
// declaration is validated before it is compared, as a render of it is.
func TestCompareWithDatabaseInfo_VectorIndexSettings_FailurePath(t *testing.T) {
	held := &ydbschema.ObservedVectorIndex{Distance: "cosine", VectorType: "float", Dimension: 3, Levels: 1, Clusters: 2}
	tests := []struct {
		name    string
		desired *schemamodel.Database
		current *catalog.Database
		wantErr string
	}{
		{
			name:    "no levels",
			desired: vectorDocs("vector_kmeans_tree", &ydbschema.DesiredVectorIndex{Distance: "cosine", VectorType: "float", Dimension: 3, Clusters: 4}),
			current: heldDocs(held),
			wantErr: `index "by_emb": a vector index's levels are between 1 and 16 .*`,
		},
		{
			name:    "settings on a synchronous index",
			desired: vectorDocs("", held.Desired()),
			current: heldDocs(nil),
			wantErr: `index "by_emb": it declares vector settings and is a sync index; declare type "vector_kmeans_tree" for a vector index`,
		},
		{
			name:    "a dimension the column does not have",
			desired: vectorDocs("vector_kmeans_tree", &ydbschema.DesiredVectorIndex{Distance: "cosine", VectorType: "float", Dimension: 4, Levels: 1, Clusters: 2}),
			current: heldDocs(held),
			wantErr: `index "by_emb" on table "docs": its vector column "emb" is declared with dimension 3 and the index with vector_dimension 4; .*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff, err := schemadiff.CompareWithDatabaseInfo(c.Context(), test.desired, test.current, catalog.ServerInfo{Dialect: platform.YDB}, nil, must.Must(builtin.New()))

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(diff, qt.IsNil)
		})
	}
}
