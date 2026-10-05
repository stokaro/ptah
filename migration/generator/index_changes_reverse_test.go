package generator_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/renderer"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/generator"
	"ptah.run/migration/schemadiff/difftypes"
)

// A rollback renames an index back and gives it back the partitioning it held,
// in place, under the name it has once renamed back: the forward change names
// the index as the declaration does. The rollback names every setting, since a
// setting a declaration leaves out keeps what the index holds, and a held
// value at YDB's default is one a reader's report leaves out. The forward diff
// is left as it was.
func TestPlanBidirectionalSchemaDiff_IndexChangesInPlaceRollBack(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{
		IndexesRenamed: []difftypes.IndexRename{{TableName: "items", From: "items_a", To: "items_by_a"}},
		IndexPartitioningChanged: []difftypes.IndexPartitioningChange{
			{TableName: "items", Name: "items_by_a",
				Partitioning: &ast.IndexPartitioningSpec{MinPartitions: 4}, Previous: &ast.IndexPartitioningSpec{MinPartitions: 3}},
			{TableName: "items", Name: "items_b",
				Partitioning: &ast.IndexPartitioningSpec{ByLoad: new(true)}},
		},
	}

	plan, err := generator.PlanBidirectionalSchemaDiff(generator.BidirectionalSchemaPlanOptions{
		Diff:          diff,
		DesiredSchema: &schemamodel.Database{},
		CurrentSchema: &catalog.Database{},
		Dialect:       platform.YDB,
		Capabilities:  capability.YDB262(),
	})
	c.Assert(err, qt.IsNil)
	sql, err := renderer.RenderSQLWithCapabilities(platform.YDB, capability.YDB262(), plan.Reverse.Nodes...)

	c.Assert(err, qt.IsNil)
	c.Assert(plan.Reverse.Diff.IndexesRenamed, qt.DeepEquals, []difftypes.IndexRename{
		{TableName: "items", From: "items_by_a", To: "items_a"},
	})
	every := func(byLoad bool, minimum uint64) *ast.IndexPartitioningSpec {
		return &ast.IndexPartitioningSpec{BySize: new(true), PartitionSizeMB: 2048, ByLoad: new(byLoad),
			MinPartitions: minimum, ReadReplicas: "PER_AZ:0"}
	}
	c.Assert(plan.Reverse.Diff.IndexPartitioningChanged, qt.DeepEquals, []difftypes.IndexPartitioningChange{
		{TableName: "items", Name: "items_a", Partitioning: every(false, 3), Previous: every(false, 4)},
		{TableName: "items", Name: "items_b", Partitioning: every(false, 1), Previous: every(true, 1)},
	})
	c.Assert(sql, qt.Equals, "ALTER TABLE `items` RENAME INDEX `items_by_a` TO `items_a`;\n"+
		"ALTER TABLE `items` ALTER INDEX `items_a` SET (AUTO_PARTITIONING_BY_SIZE = ENABLED, "+
		"AUTO_PARTITIONING_PARTITION_SIZE_MB = 2048, AUTO_PARTITIONING_BY_LOAD = DISABLED, "+
		"AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 3);\n"+
		"ALTER TABLE `items` ALTER INDEX `items_b` SET (AUTO_PARTITIONING_BY_SIZE = ENABLED, "+
		"AUTO_PARTITIONING_PARTITION_SIZE_MB = 2048, AUTO_PARTITIONING_BY_LOAD = DISABLED, "+
		"AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1);\n")
	c.Assert(diff.IndexesRenamed[0].From, qt.Equals, "items_a",
		qt.Commentf("the reversal must not write through to the forward diff"))
	c.Assert(diff.IndexPartitioningChanged[0].Partitioning.MinPartitions, qt.Equals, uint64(4))
}
