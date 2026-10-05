package planner_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/coverage"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// A desired state that cannot spell a TTL -- an HCL document, whose loader
// records that -- keeps the TTL a table holds. A plan that rebuilds the table
// writes that TTL on the new one, rather than dropping it with the old table,
// and plans no TTL change of its own.
func TestGenerateSchemaDiff_YDBRebuildKeepsATTLTheDesiredStateDoesNotDescribe(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Event", Name: "events"}},
		Fields: []schemamodel.Field{
			{StructName: "Event", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "Event", Name: "n", Type: "BIGINT", Nullable: true},
			{StructName: "Event", Name: "ts", Type: "Timestamp", Nullable: true},
		},
		NotDescribed: coverage.Set{}.With(coverage.Object{
			Kind: coverage.TTL, Reason: coverage.Unsupported, Provenance: coverage.DerivedFromFact,
		}),
	}
	current := &catalog.Database{
		Tables: []catalog.Table{{Name: "events", Type: "TABLE",
			RowDeletionPolicy: &ast.RowDeletionPolicySpec{Column: "ts", Interval: "P30D"},
			Columns: []catalog.Column{
				{Name: "id", DataType: "Int64", ColumnType: "Int64", IsNullable: "NO", IsPrimaryKey: true, OrdinalPosition: 1},
				{Name: "n", DataType: "Int32", ColumnType: "Int32", IsNullable: "YES", OrdinalPosition: 2},
				{Name: "ts", DataType: "Timestamp", ColumnType: "Timestamp", IsNullable: "YES", OrdinalPosition: 3},
			}}},
		Constraints: []catalog.Constraint{{Name: "events_pkey", TableName: "events", Type: "PRIMARY KEY",
			ColumnName: "id", ColumnNames: []string{"id"}}},
	}
	diff := schemadiff.CompareWithDialect(desired, current, platform.YDB)

	plan, err := planner.GenerateSchemaDiffSQLWithOptions(diff, platform.YDB, planner.Options{
		Capabilities: capability.YDB262(), AllowTableRebuild: true,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(plan, qt.Contains, ") WITH (TTL = Interval(\"P30D\") ON `ts`, AUTO_PARTITIONING_BY_SIZE = ENABLED, ")
	c.Assert(plan, qt.Not(qt.Contains), "RESET (TTL)")
}
