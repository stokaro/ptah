package atlashclrender_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/atlashcl"
	"ptah.run/internal/atlashclrender"
	"ptah.run/internal/convert/goschematodb"
	"ptah.run/internal/ydbindex"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// declaredIndexFacets is spec as an index's YDB owner facet, and no facet
// for nil.
func declaredIndexFacets(c *qt.C, spec *ydbschema.IndexPartitioning) schemaext.Facets {
	c.Helper()
	facets, err := ydbindex.WithPartitioning(schemaext.Facets{}, spec)
	c.Assert(err, qt.IsNil)
	return facets
}

// heldIndexPartitioning is the settings an index's YDB owner facet declares,
// or nil where it states none.
func heldIndexPartitioning(c *qt.C, facets schemaext.Facets) *ydbschema.IndexPartitioning {
	c.Helper()
	value, found, err := schemaext.FacetAs[*ydbschema.DesiredIndexPartitioning](facets, ydbschema.IndexPartitioningKind)
	c.Assert(err, qt.IsNil)
	if !found {
		return nil
	}
	return &value.IndexPartitioning
}

// TestYDBIndexPartitioning_HCLRoundTrip writes an index's partitioning as
// attributes of its block, reports no loss, and reads them back as the same
// facet, so a comparison of the document with the schema it came from plans
// nothing.
func TestYDBIndexPartitioning_HCLRoundTrip(t *testing.T) {
	for _, test := range []struct {
		name string
		spec *ydbschema.IndexPartitioning
	}{
		{"configured", &ydbschema.IndexPartitioning{BySize: new(true), PartitionSizeMB: 512, ByLoad: new(true), MinPartitions: 4, MaxPartitions: 16, ReadReplicas: "ANY_AZ:2"}},
		{"explicitly disabled", &ydbschema.IndexPartitioning{BySize: new(false), ByLoad: new(false), MinPartitions: 1, ReadReplicas: "ANY_AZ:0"}},
		{"omitted", nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			original := &schemamodel.Database{
				FeatureCoverage: must.Must(ydbcoordination.Coverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil)),
				Tables:          []schemamodel.Table{{StructName: "Item", Name: "items", PrimaryKey: []string{"id"}}},
				Fields:          []schemamodel.Field{{StructName: "Item", Name: "id", Type: "Int64", Primary: true}, {StructName: "Item", Name: "kind", Type: "Utf8", Nullable: true}},
				Indexes: []schemamodel.Index{{StructName: "Item", TableName: "items", Name: "by_kind", Type: "GLOBAL SYNC", Fields: []string{"kind"},
					Facets: declaredIndexFacets(c, test.spec)}},
			}
			rendered, err := atlashclrender.RenderInspectedForAtlasCLI(original, "ydb", "")
			c.Assert(err, qt.IsNil)
			// The block writes the settings, so nothing reports them lost.
			c.Assert(rendered.Diagnostics, qt.HasLen, 0)
			parsed, err := atlashcl.Parse(rendered.Data, "schema.hcl")
			c.Assert(err, qt.IsNil)
			c.Assert(parsed.Indexes, qt.HasLen, 1)
			c.Assert(heldIndexPartitioning(c, parsed.Indexes[0].Facets), qt.DeepEquals, test.spec)
			current := must.Must(goschematodb.ToDBSchema(t.Context(), original, "ydb", must.Must(builtin.New())))
			caps := capability.YDB262()
			diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), parsed, current, catalog.ServerInfo{Dialect: "ydb", Capabilities: caps}, nil, must.Must(builtin.New()))
			c.Assert(err, qt.IsNil)
			statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(
				context.Background(), must.Must(builtin.New()),
				diff, "ydb", planner.Options{Capabilities: caps},
			)
			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.HasLen, 0)
		})
	}
}
