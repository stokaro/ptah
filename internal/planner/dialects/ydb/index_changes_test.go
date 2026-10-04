package ydb_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/planner/dialects/ydb"
	"ptah.run/migration/schemadiff/difftypes"
)

// TestGenerateMigrationAST_IndexChangesInPlace_HappyPath pins where a rename
// and a change of partitioning sit in a plan: after the index drops, so a name
// a dropped index frees is free, and before the column changes and the index
// additions; each rename in a statement of its own (`RENAME INDEX TO can not
// be used together with another table action`), and the partitioning set
// under the name the rename gave. A plan of this shape applied on 26.2.1.14
// and 25.1.4.7 and read back as declared.
func TestGenerateMigrationAST_IndexChangesInPlace_HappyPath(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{
		TablesModified: []difftypes.TableDiff{{
			TableName:    "items",
			Desired:      itemsDeclaration(field("note", "TEXT", true)),
			ColumnsAdded: difftypes.ColumnChanges{field("note", "TEXT", true)},
		}},
		IndexesRemoved: []difftypes.IndexRef{{Name: "items_gone", TableName: "items"}},
		IndexesRenamed: []difftypes.IndexRename{
			{TableName: "items", From: "items_a", To: "items_by_a"},
			{TableName: "items", From: "items_b", To: "items_by_b"},
		},
		IndexPartitioningChanged: []difftypes.IndexPartitioningChange{{
			TableName: "items", Name: "items_by_a",
			Partitioning: &ast.IndexPartitioningSpec{MinPartitions: 4},
			Previous:     &ast.IndexPartitioningSpec{MinPartitions: 3},
		}},
		IndexesAdded: difftypes.IndexChanges{{TableName: "items", Index: schemamodel.Index{
			Name: "items_note", Fields: []string{"note"}, Partitioning: &ast.IndexPartitioningSpec{ByLoad: new(true)},
		}}},
	}

	got := render(c, capability.YDB251(), diff)

	c.Assert(got, qt.Equals, "ALTER TABLE `items` DROP INDEX `items_gone`;\n"+
		"ALTER TABLE `items` RENAME INDEX `items_a` TO `items_by_a`;\n"+
		"ALTER TABLE `items` RENAME INDEX `items_b` TO `items_by_b`;\n"+
		"ALTER TABLE `items` ALTER INDEX `items_by_a` SET (AUTO_PARTITIONING_BY_SIZE = ENABLED, "+
		"AUTO_PARTITIONING_PARTITION_SIZE_MB = 2048, AUTO_PARTITIONING_BY_LOAD = DISABLED, "+
		"AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 4);\n"+
		"ALTER TABLE `items` ADD COLUMN `note` Utf8;\n"+
		"ALTER TABLE `items` ADD INDEX `items_note` GLOBAL SYNC ON (`note`);\n"+
		"ALTER TABLE `items` ALTER INDEX `items_note` SET (AUTO_PARTITIONING_BY_LOAD = ENABLED);\n")
}

// TestGenerateMigrationAST_IndexChangesInPlace_FailurePath refuses, before any
// node, a rename or a change of partitioning the target cannot make, by its
// key, and a change of partitioning YDB refuses on every line, by the server's
// reason.
func TestGenerateMigrationAST_IndexChangesInPlace_FailurePath(t *testing.T) {
	rename := &difftypes.SchemaDiff{IndexesRenamed: []difftypes.IndexRename{{TableName: "items", From: "a", To: "b"}}}
	change := func(desired, previous *ast.IndexPartitioningSpec) *difftypes.SchemaDiff {
		return &difftypes.SchemaDiff{
			IndexesRemoved: []difftypes.IndexRef{{Name: "gone", TableName: "items"}},
			IndexPartitioningChanged: []difftypes.IndexPartitioningChange{
				{TableName: "items", Name: "a", Partitioning: desired, Previous: previous},
			},
		}
	}
	tests := []struct {
		name        string
		caps        capability.Capabilities
		diff        *difftypes.SchemaDiff
		wantFeature string
		wantErr     string
	}{
		{name: "a rename without the key", caps: capability.YDB262().With(capability.IndexRename, false), diff: rename,
			wantFeature: string(capability.IndexRename),
			wantErr:     `renaming index "a" of table "items" to "b", which requires target capability index_rename, unavailable on this ydb target`},
		{name: "partitioning without the key", caps: capability.YDB262().With(capability.IndexPartitioning, false),
			diff: change(&ast.IndexPartitioningSpec{MinPartitions: 2}, nil), wantFeature: string(capability.IndexPartitioning),
			wantErr: `changing the partitioning of index "a" of table "items", which requires target capability index_partitioning, .*`},
		{name: "a declaration YDB refuses", caps: capability.YDB262(),
			diff: change(&ast.IndexPartitioningSpec{BySize: new(false), PartitionSizeMB: 64}, nil), wantFeature: `index "a" of table "items"`,
			wantErr: `index "a" of table "items": auto_partitioning_partition_size_mb is set while .*`},
		{name: "settings the index holds that YDB would refuse", caps: capability.YDB262(),
			diff: change(nil, &ast.IndexPartitioningSpec{ReadReplicas: "SOME_AZ:1"}), wantFeature: `index "a" of table "items"`,
			wantErr: `index "a" of table "items": the settings it holds: read replicas "SOME_AZ:1" are not one YDB takes: .*`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			nodes, err := ydb.NewWithCapabilities(test.caps).GenerateMigrationAST(test.diff)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			var capabilityErr *ptaherr.CapabilityError
			c.Assert(err, qt.ErrorAs, &capabilityErr)
			c.Assert(capabilityErr.Feature, qt.Equals, test.wantFeature)
			c.Assert(nodes, qt.IsNil)
		})
	}
}

// TestGenerateMigrationAST_PlansARenameAlone holds the catch-all that refuses a
// diff carrying a family no one planned: a diff that only renames an index, or
// only changes its partitioning, is planned, not refused as one.
func TestGenerateMigrationAST_PlansARenameAlone(t *testing.T) {
	tests := []struct {
		name string
		diff *difftypes.SchemaDiff
	}{
		{name: "a rename", diff: &difftypes.SchemaDiff{IndexesRenamed: []difftypes.IndexRename{{TableName: "items", From: "a", To: "b"}}}},
		{name: "a change of partitioning", diff: &difftypes.SchemaDiff{IndexPartitioningChanged: []difftypes.IndexPartitioningChange{
			{TableName: "items", Name: "a", Partitioning: &ast.IndexPartitioningSpec{MinPartitions: 2}},
		}}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			nodes, err := ydb.NewWithCapabilities(capability.YDB262()).GenerateMigrationAST(test.diff)
			c.Assert(err, qt.IsNil)
			c.Assert(nodes, qt.HasLen, 1)
		})
	}
}

// TestGenerateMigrationAST_TableRebuild_CarriesIndexChangesInPlace leaves a
// rename and a change of partitioning on a table the plan rebuilds to the new
// table: its CREATE TABLE names the index as declared and an ALTER INDEX after
// it gives the declared settings, so nothing is renamed or set on the old
// table, which the rebuild drops.
func TestGenerateMigrationAST_TableRebuild_CarriesIndexChangesInPlace(t *testing.T) {
	c := qt.New(t)
	declaration := appItems(field("label", "TEXT", true), field("n", "BIGINT", true))
	declaration.Indexes[0].Partitioning = &ast.IndexPartitioningSpec{MinPartitions: 4}
	diff := modified(difftypes.TableDiff{
		TableName: "app.items", Desired: declaration,
		ColumnsModified: []difftypes.ColumnDiff{{ColumnName: "n", Changes: map[string]string{"type": "Int32 -> Int64"}}},
	})
	diff.IndexesRenamed = []difftypes.IndexRename{{TableName: "app.items", From: "items_old_label", To: "items_label"}}
	diff.IndexPartitioningChanged = []difftypes.IndexPartitioningChange{{TableName: "app.items", Name: "items_label",
		Partitioning: &ast.IndexPartitioningSpec{MinPartitions: 4}, Previous: &ast.IndexPartitioningSpec{MinPartitions: 2}}}

	got := renderRebuild(c, capability.YDB262(), diff)

	c.Assert(got, qt.Not(qt.Contains), "RENAME INDEX")
	c.Assert(got, qt.Not(qt.Contains), "ALTER TABLE `app/items` ALTER INDEX")
	c.Assert(got, qt.Contains, "    INDEX `items_label` GLOBAL SYNC ON (`label`)\n"+heldDefaults+
		"ALTER TABLE `app/__ptah_rebuild_items` ALTER INDEX `items_label` SET (")
}
