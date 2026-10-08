package ydb_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/difftypes"
)

func columnRetention() *ast.YDBColumnTableSpec {
	return &ast.YDBColumnTableSpec{HashColumns: []string{"id"}, TTL: &ast.YDBTieredTTLSpec{Column: "ts", Tiers: []ast.YDBTTLTierSpec{{Interval: "P1D", ExternalSource: "/local/ext/bucket"}, {Interval: "P7D"}}}}
}

// Replacing a source must detach an unchanged policy too. The source cannot
// be dropped while a column table's eviction tier references it.
func TestGenerateMigrationAST_ColumnTTLSourceReplacement(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{
		CurrentDatabasePath:        "/local",
		DeclaredTables:             []schemamodel.Table{{Name: "events", YDBColumnTable: columnRetention()}},
		ExternalDataSourcesChanged: []difftypes.ExternalDataSourceChange{{Current: plannedBucket, Declared: movedBucket()}},
	}
	got := render(c, externalPlanCaps(false).With(capability.TieredTTL, true), diff)
	c.Assert(strings.Split(got, ";\n"), qt.DeepEquals, []string{
		"ALTER TABLE `events` RESET (TTL)",
		"DROP EXTERNAL DATA SOURCE `ext/bucket`",
		"CREATE EXTERNAL DATA SOURCE `ext/bucket` WITH (\n    SOURCE_TYPE = 'ObjectStorage',\n    LOCATION = 'https://s3.example.test/other/',\n    AUTH_METHOD = 'NONE'\n)",
		"ALTER TABLE `events` SET (TTL = Interval(\"PT86400S\") TO EXTERNAL DATA SOURCE `/local/ext/bucket`, Interval(\"PT604800S\") DELETE ON `ts`)", "",
	})
}

// Both TTL forms use the same server setting. Resetting the old row policy
// after installing the eviction policy would silently remove the new one.
func TestGenerateMigrationAST_ColumnTTLFromRowPolicy(t *testing.T) {
	c := qt.New(t)
	declaration := itemsDeclaration(field("ts", "TIMESTAMP", true))
	declaration.Table.YDBColumnTable = columnRetention()
	declaration.Table.PrimaryKey = []string{"ts", "id"}
	diff := modified(t, difftypes.TableDiff{
		TableName: "items", Desired: declaration,
		YDBColumnTableChange:    &difftypes.YDBColumnTableChange{Desired: columnRetention(), Current: &ast.YDBColumnTableSpec{HashColumns: []string{"id"}}},
		RowDeletionPolicyChange: &difftypes.RowDeletionPolicyChange{Current: &ast.RowDeletionPolicySpec{Column: "ts", Interval: "P7D"}},
	})
	got := render(c, capability.YDB262().With(capability.TieredTTL, true), diff)
	c.Assert(got, qt.Equals, "ALTER TABLE `items` RESET (TTL);\nALTER TABLE `items` SET (TTL = Interval(\"PT86400S\") TO EXTERNAL DATA SOURCE `/local/ext/bucket`, Interval(\"PT604800S\") DELETE ON `ts`);\n")
}
