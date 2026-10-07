package generator_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/generator"
	"ptah.run/migration/schemadiff/difftypes"
)

// A rollback gives a table back the settings it held, in place, writing the
// statement from both sides as the forward change does. The forward diff is
// left as it was.
func TestPlanBidirectionalSchemaDiff_TablePartitioningRollsBack(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{{
		TableName: "items",
		YDBPartitioningChange: &difftypes.YDBTablePartitioningChange{
			Desired: &ast.YDBTablePartitioningSpec{ByLoad: new(true), MinPartitions: 4, KeyBloomFilter: new(true)},
			Current: &ast.YDBTablePartitioningSpec{MinPartitions: 6},
		},
	}}}

	plan, err := generator.PlanBidirectionalSchemaDiff(generator.BidirectionalSchemaPlanOptions{
		Diff:          diff,
		DesiredSchema: &schemamodel.Database{},
		CurrentSchema: &catalog.Database{},
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
	c.Assert(reverse, qt.Equals, "ALTER TABLE `items` SET (AUTO_PARTITIONING_BY_SIZE = ENABLED, "+
		"AUTO_PARTITIONING_PARTITION_SIZE_MB = 2048, AUTO_PARTITIONING_BY_LOAD = DISABLED, "+
		"AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 6, KEY_BLOOM_FILTER = DISABLED);\n")
	c.Assert(diff.TablesModified[0].YDBPartitioningChange.Desired.MinPartitions, qt.Equals, uint64(4),
		qt.Commentf("the reversal must not write through to the forward diff"))
}
