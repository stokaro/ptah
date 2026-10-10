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
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/engine/builtin"
	"ptah.run/migration/generator"
	"ptah.run/migration/schemadiff"
)

// A rollback gives a table back the settings it held, in place, writing the
// statement from both sides as the forward change does. The current side
// reads as the YDB reader gives it, the settings that differ from YDB's
// documented defaults. The forward diff is left as it was.
func TestPlanBidirectionalSchemaDiff_TablePartitioningRollsBack(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(builtin.New())
	known := schemaext.Knowledge{State: schemaext.Complete}
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Item", Name: "items", Facets: must.Must(schemaext.NewFacets(&ydbschema.DesiredTablePartitioning{
			TablePartitioning: ydbschema.TablePartitioning{ByLoad: new(true), MinPartitions: 4, KeyBloomFilter: new(true)},
		}))}},
		Fields:          []schemamodel.Field{{StructName: "Item", Name: "id", Type: "BIGINT", Primary: true}},
		FeatureCoverage: must.Must(ydbschema.TablePartitioningCoverage(schemaext.Desired, known, nil)),
	}
	schemamodel.Finalize(desired)
	current := &catalog.Database{
		Tables: []catalog.Table{{Name: "items", Columns: []catalog.Column{{Name: "id", DataType: "Int64", IsPrimaryKey: true}},
			Facets: must.Must(must.Must(schemaext.NewFacets(&ydbschema.ObservedTablePartitioning{
				TablePartitioning: ydbschema.TablePartitioning{MinPartitions: 6},
			})).WithTargetScope(ydbschema.TablePartitioningKind, platform.YDB))}},
		Constraints:     []catalog.Constraint{{Name: "items_pkey", TableName: "items", Type: "PRIMARY KEY", ColumnName: "id", ColumnNames: []string{"id"}}},
		FeatureCoverage: must.Must(ydbschema.TablePartitioningCoverage(schemaext.Observed, known, nil)),
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
	c.Assert(forward, qt.Equals, "ALTER TABLE `items` SET (AUTO_PARTITIONING_BY_SIZE = ENABLED, "+
		"AUTO_PARTITIONING_PARTITION_SIZE_MB = 2048, AUTO_PARTITIONING_BY_LOAD = ENABLED, "+
		"AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 4, KEY_BLOOM_FILTER = ENABLED);\n")
	c.Assert(reverse, qt.Equals, "-- Rollback of \"table items\": \"restore every setting the table held in place\".\n"+
		"ALTER TABLE `items` SET (AUTO_PARTITIONING_BY_SIZE = ENABLED, "+
		"AUTO_PARTITIONING_PARTITION_SIZE_MB = 2048, AUTO_PARTITIONING_BY_LOAD = DISABLED, "+
		"AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 6, KEY_BLOOM_FILTER = DISABLED);\n")
	c.Assert(diff.TablesModified, qt.HasLen, 1)
	change := diff.TablesModified[0].FeatureChanges[0].Value.(*ydbdiff.TablePartitioning)
	c.Assert(change.After.MinPartitions, qt.Equals, uint64(4), qt.Commentf("the reversal must not write through to the forward diff"))
	c.Assert(change.Before.MinPartitions, qt.Equals, uint64(6))
}
