package generator_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/engine/builtin"
	"ptah.run/migration/generator"
	"ptah.run/migration/schemadiff/difftypes"
)

// indexSettingsChange is the YDB owner's change of the settings of index name
// of table items, from current, nil for YDB's defaults, to desired.
func indexSettingsChange(name string, desired ydbschema.IndexPartitioning, current *ydbschema.IndexPartitioning) schemaext.ChangeRecord {
	change := &ydbdiff.IndexPartitioning{After: &ydbschema.DesiredIndexPartitioning{IndexPartitioning: desired}}
	if current != nil {
		change.Before = &ydbschema.ObservedIndexPartitioning{IndexPartitioning: *current}
	}
	subject := objectidentity.NewBuilder(identifier.ForDialect(platform.YDB)).IndexParts("", "items", name)
	return schemaext.ChangeRecord{Subject: subject, Value: change}
}

// A rollback renames an index back and gives it back the partitioning it held,
// in place, under the name it has once renamed back: the forward change names
// the index as the declaration does. The rollback names every setting, since a
// setting a declaration leaves out keeps what the index holds, and a held
// value at YDB's default is one a reader's report leaves out. The forward diff
// is left as it was.
func TestPlanBidirectionalSchemaDiff_IndexChangesInPlaceRollBack(t *testing.T) {
	c := qt.New(t)
	held := must.Must(schemaext.NewFacets(&ydbschema.ObservedIndexPartitioning{IndexPartitioning: ydbschema.IndexPartitioning{MinPartitions: 3}}))
	diff := &difftypes.SchemaDiff{
		TablesModified: []difftypes.TableDiff{{
			TableName: "items",
			Desired: schemacapture.TableDeclaration{Table: schemamodel.Table{StructName: "Item", Name: "items", PrimaryKey: []string{"id"}},
				Fields: []schemamodel.Field{{StructName: "Item", Name: "id", Type: "Int64", Primary: true}}},
			Current: schemacapture.TableObservation{Table: catalog.Table{Name: "items"},
				FeatureCoverage: must.Must(ydbschema.IndexPartitioningCoverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, nil)),
				Indexes: []catalog.Index{
					{TableName: "items", Name: "items_a", Columns: []string{"a"}, Method: "GLOBAL SYNC", Facets: held},
					{TableName: "items", Name: "items_b", Columns: []string{"b"}, Method: "GLOBAL SYNC"},
				}},
			FeatureChanges: []schemaext.ChangeRecord{
				indexSettingsChange("items_by_a", ydbschema.IndexPartitioning{MinPartitions: 4}, &ydbschema.IndexPartitioning{MinPartitions: 3}),
				indexSettingsChange("items_b", ydbschema.IndexPartitioning{ByLoad: new(true)}, nil),
			},
		}},
		IndexesRenamed: []difftypes.IndexRename{{TableName: "items", From: "items_a", To: "items_by_a"}},
	}

	plan, err := generator.PlanBidirectionalSchemaDiff(t.Context(),
		generator.BidirectionalSchemaPlanOptions{Runtime: must.Must(builtin.New()), Diff: diff,
			DesiredSchema: &schemamodel.Database{},
			CurrentSchema: &catalog.Database{},
			Dialect:       platform.YDB,
			Capabilities:  capability.YDB262(),
		})
	c.Assert(err, qt.IsNil)
	sql, err := builtin.RenderSQLWithCapabilities(platform.YDB, capability.YDB262(), plan.Reverse.Nodes...)

	c.Assert(err, qt.IsNil)
	c.Assert(plan.Reverse.Diff.IndexesRenamed, qt.DeepEquals, []difftypes.IndexRename{
		{TableName: "items", From: "items_by_a", To: "items_a"},
	})
	subjects := make([]string, 0, 2)
	for _, change := range plan.Reverse.Diff.TablesModified[0].FeatureChanges {
		subjects = append(subjects, change.Subject.Name.Source)
	}
	c.Assert(subjects, qt.DeepEquals, []string{"items_a", "items_b"})
	c.Assert(sql, qt.Contains, "ALTER TABLE `items` RENAME INDEX `items_by_a` TO `items_a`;\n")
	c.Assert(sql, qt.Contains, "ALTER TABLE `items` ALTER INDEX `items_a` SET (AUTO_PARTITIONING_BY_SIZE = ENABLED, "+
		"AUTO_PARTITIONING_PARTITION_SIZE_MB = 2048, AUTO_PARTITIONING_BY_LOAD = DISABLED, "+
		"AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 3);\n")
	c.Assert(sql, qt.Contains, "ALTER TABLE `items` ALTER INDEX `items_b` SET (AUTO_PARTITIONING_BY_SIZE = ENABLED, "+
		"AUTO_PARTITIONING_PARTITION_SIZE_MB = 2048, AUTO_PARTITIONING_BY_LOAD = DISABLED, "+
		"AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1);\n")
	c.Assert(diff.IndexesRenamed[0].From, qt.Equals, "items_a",
		qt.Commentf("the reversal must not write through to the forward diff"))
	c.Assert(diff.TablesModified[0].FeatureChanges[0].Subject.Name.Source, qt.Equals, "items_by_a",
		qt.Commentf("the reversal must not write through to the forward diff"))
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

	plan, err := generator.PlanBidirectionalSchemaDiff(t.Context(),
		generator.BidirectionalSchemaPlanOptions{Runtime: must.Must(builtin.New()), Diff: diff,
			DesiredSchema: &schemamodel.Database{},
			CurrentSchema: &catalog.Database{},
			Dialect:       platform.YDB,
			Capabilities:  capability.YDB262(),
		})
	c.Assert(err, qt.IsNil)
	sql, err := builtin.RenderSQLWithCapabilities(platform.YDB, capability.YDB262(), plan.Reverse.Nodes...)

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
