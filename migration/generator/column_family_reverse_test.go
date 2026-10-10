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

// A rollback moves each column back to the family it left and gives back each
// setting the forward change wrote, in place. A family the forward change
// added and a storage pool it named where the table had none are not stated
// by the earlier state, so they stay, the family emptied of its columns: YQL
// drops no family and removes no pool, and refusing them would refuse the
// whole rollback. The current side reads as the YDB reader gives it, each
// setting at the value the table holds. The forward diff is left as it was.
func TestPlanBidirectionalSchemaDiff_ColumnFamiliesRollBack(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(builtin.New())
	known := schemaext.Knowledge{State: schemaext.Complete}
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Item", Name: "items", Facets: must.Must(schemaext.NewFacets(&ydbschema.DesiredColumnFamilies{
			Families: []ydbschema.ColumnFamily{
				{Name: "cold", Data: "hdd", Compression: "lz4", Columns: []string{"body"}},
				{Name: "default", Data: "ssd", Compression: "lz4"},
				{Name: "warm"},
			},
		}))}},
		Fields: []schemamodel.Field{
			{StructName: "Item", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "Item", Name: "body", Type: "TEXT", Nullable: true},
		},
		FeatureCoverage: must.Must(ydbschema.ColumnFamiliesCoverage(schemaext.Desired, known, nil)),
	}
	schemamodel.Finalize(desired)
	held := []ydbschema.ColumnFamily{
		{Name: "default", Compression: "off"},
		{Name: "warm", Compression: "off", Columns: []string{"body"}},
	}
	current := &catalog.Database{
		Tables: []catalog.Table{{Name: "items", Columns: []catalog.Column{
			{Name: "id", DataType: "Int64", IsPrimaryKey: true}, {Name: "body", DataType: "Utf8", IsNullable: "YES"},
		}, Facets: must.Must(must.Must(schemaext.NewFacets(&ydbschema.ObservedColumnFamilies{Families: held})).
			WithTargetScope(ydbschema.ColumnFamiliesKind, platform.YDB))}},
		Constraints:     []catalog.Constraint{{Name: "items_pkey", TableName: "items", Type: "PRIMARY KEY", ColumnName: "id", ColumnNames: []string{"id"}}},
		FeatureCoverage: must.Must(ydbschema.ColumnFamiliesCoverage(schemaext.Observed, known, nil)),
	}
	diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), desired, current, catalog.ServerInfo{Dialect: platform.YDB}, nil, runtime)
	c.Assert(err, qt.IsNil)

	plan, err := generator.PlanBidirectionalSchemaDiff(t.Context(),
		generator.BidirectionalSchemaPlanOptions{Runtime: runtime, Diff: diff,
			DesiredSchema: desired,
			CurrentSchema: current,
			Dialect:       platform.YDB,
			Capabilities:  capability.YDB262(),
		})
	c.Assert(err, qt.IsNil)
	forward, err := builtin.RenderSQLWithCapabilities(platform.YDB, capability.YDB262(), plan.Forward.Nodes...)
	c.Assert(err, qt.IsNil)
	reverse, err := builtin.RenderSQLWithCapabilities(platform.YDB, capability.YDB262(), plan.Reverse.Nodes...)

	c.Assert(err, qt.IsNil)
	c.Assert(forward, qt.Equals, "ALTER TABLE `items` ADD FAMILY `cold` (DATA = 'hdd', COMPRESSION = 'lz4'), "+
		"ALTER FAMILY `default` SET DATA 'ssd', ALTER FAMILY `default` SET COMPRESSION 'lz4', "+
		"ALTER COLUMN `body` SET FAMILY `cold`;\n")
	c.Assert(reverse, qt.Equals, "-- Rollback of \"table items\": \"move each column back to its family and restore each family setting in place\".\n"+
		"-- Recovery limit: \"YQL drops no column family and resets no family setting, so the rollback keeps family cold "+
		"and the DATA of family default, which the forward change added.\"\n"+
		"ALTER TABLE `items` ALTER FAMILY `default` SET COMPRESSION 'off', ALTER COLUMN `body` SET FAMILY `warm`;\n")
	read, _, err := schemaext.FacetAs[*ydbschema.ObservedColumnFamilies](current.Tables[0].Facets, ydbschema.ColumnFamiliesKind)
	c.Assert(err, qt.IsNil)
	c.Assert(read.Families, qt.DeepEquals, held, qt.Commentf("the reversal must not write through to the current state"))
}
