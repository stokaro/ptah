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
// the index as the declaration does. The forward diff is left as it was.
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
	c.Assert(plan.Reverse.Diff.IndexPartitioningChanged, qt.DeepEquals, []difftypes.IndexPartitioningChange{
		{TableName: "items", Name: "items_a",
			Partitioning: &ast.IndexPartitioningSpec{MinPartitions: 3}, Previous: &ast.IndexPartitioningSpec{MinPartitions: 4}},
		{TableName: "items", Name: "items_b", Previous: &ast.IndexPartitioningSpec{ByLoad: new(true)}},
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

// A rollback writes each index comment back to the one the database held, and
// a renamed index's comment back under its old name, removing it from the new
// one, after the index is renamed back. The forward diff is left as it was.
func TestPlanBidirectionalSchemaDiff_IndexCommentsRollBack(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{
		IndexesRenamed: []difftypes.IndexRename{{TableName: "items", From: "items_a", To: "items_by_a"}},
		IndexCommentsChanged: []difftypes.IndexCommentChange{
			{TableName: "items", Name: "items_by_a", From: "items_a", Current: "Old", Desired: "New"},
			{TableName: "items", Name: "items_b", Current: "Kept", Desired: ""},
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
	c.Assert(plan.Reverse.Diff.IndexCommentsChanged, qt.DeepEquals, []difftypes.IndexCommentChange{
		{TableName: "items", Name: "items_a", From: "items_by_a", Current: "New", Desired: "Old"},
		{TableName: "items", Name: "items_b", Current: "", Desired: "Kept"},
	})
	c.Assert(sql, qt.Equals, "ALTER TABLE `items` RENAME INDEX `items_by_a` TO `items_a`;\n"+
		"COMMENT ON INDEX `items_a` ON `items` IS 'Old';\n"+
		"COMMENT ON INDEX `items_by_a` ON `items` IS NULL;\n"+
		"COMMENT ON INDEX `items_b` ON `items` IS 'Kept';\n")
	c.Assert(diff.IndexCommentsChanged[0].Name, qt.Equals, "items_by_a",
		qt.Commentf("the reversal must not write through to the forward diff"))
}
