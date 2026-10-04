package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff"
	"ptah.run/migration/schemadiff/difftypes"
)

// ydbTTLDeclaration declares a table whose row deletion policy is policy.
func ydbTTLDeclaration(policy *ast.RowDeletionPolicySpec) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Event", Name: "events", RowDeletionPolicy: policy}},
		Fields: []schemamodel.Field{
			{StructName: "Event", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "Event", Name: "ts", Type: "TIMESTAMP", Nullable: true},
			{StructName: "Event", Name: "expires", Type: "BIGINT UNSIGNED", Nullable: true},
		},
	}
}

// ydbTTLCatalog is the table as the YDB reader reports it, with the TTL read
// back as policy.
func ydbTTLCatalog(policy *ast.RowDeletionPolicySpec) *catalog.Database {
	return &catalog.Database{
		Tables: []catalog.Table{{Name: "events", Type: "TABLE", RowDeletionPolicy: policy, Columns: []catalog.Column{
			{Name: "id", DataType: "Int64", ColumnType: "Int64", IsNullable: "NO", IsPrimaryKey: true, OrdinalPosition: 1},
			{Name: "ts", DataType: "Timestamp64", ColumnType: "Timestamp64", IsNullable: "YES", OrdinalPosition: 2},
			{Name: "expires", DataType: "Uint64", ColumnType: "Uint64", IsNullable: "YES", OrdinalPosition: 3},
		}}},
		Constraints: []catalog.Constraint{{Name: "events_pkey", TableName: "events", Type: "PRIMARY KEY",
			ColumnName: "id", ColumnNames: []string{"id"}}},
	}
}

// TestCompare_YDBTTL_HappyPath reads a declared policy and the TTL YDB reads
// back as one policy when they delete the same rows on the same schedule,
// whatever the spelling of the interval or the case of the unit, and plans
// nothing, against the database and against the same document alike.
func TestCompare_YDBTTL_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		declared *ast.RowDeletionPolicySpec
		read     *ast.RowDeletionPolicySpec
	}{
		{
			name:     "an interval in hours read back in days",
			declared: &ast.RowDeletionPolicySpec{Column: "ts", Interval: "PT720H"},
			read:     &ast.RowDeletionPolicySpec{Column: "ts", Interval: "P30D"},
		},
		{
			name:     "a week read back in days",
			declared: &ast.RowDeletionPolicySpec{Column: "ts", Interval: "P1W"},
			read:     &ast.RowDeletionPolicySpec{Column: "ts", Interval: "P7D"},
		},
		{
			name:     "a unit in another case",
			declared: &ast.RowDeletionPolicySpec{Column: "expires", Interval: "PT90M", Unit: "seconds"},
			read:     &ast.RowDeletionPolicySpec{Column: "expires", Interval: "PT1H30M", Unit: "SECONDS"},
		},
		{name: "no TTL on either side"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			against := schemadiff.CompareWithDialect(ydbTTLDeclaration(test.declared), ydbTTLCatalog(test.read), platform.YDB)
			c.Assert(against.TablesModified, qt.HasLen, 0)
			itself := schemadiff.CompareSchemas(ydbTTLDeclaration(test.declared), ydbTTLDeclaration(test.declared), platform.YDB)
			c.Assert(itself.TablesModified, qt.HasLen, 0)
		})
	}
}

// TestCompare_YDBTTL_Changes reports a table whose policy differs in any part
// as modified, with both sides, so the planner knows what to set or reset.
func TestCompare_YDBTTL_Changes(t *testing.T) {
	tests := []struct {
		name     string
		declared *ast.RowDeletionPolicySpec
		read     *ast.RowDeletionPolicySpec
	}{
		{
			name:     "a policy added",
			declared: &ast.RowDeletionPolicySpec{Column: "ts", Interval: "P30D"},
		},
		{
			name: "a policy removed",
			read: &ast.RowDeletionPolicySpec{Column: "ts", Interval: "P30D"},
		},
		{
			name:     "another interval",
			declared: &ast.RowDeletionPolicySpec{Column: "ts", Interval: "P31D"},
			read:     &ast.RowDeletionPolicySpec{Column: "ts", Interval: "P30D"},
		},
		{
			name:     "another column",
			declared: &ast.RowDeletionPolicySpec{Column: "expires", Interval: "P30D", Unit: "SECONDS"},
			read:     &ast.RowDeletionPolicySpec{Column: "ts", Interval: "P30D"},
		},
		{
			name:     "another unit",
			declared: &ast.RowDeletionPolicySpec{Column: "expires", Interval: "P30D", Unit: "MILLISECONDS"},
			read:     &ast.RowDeletionPolicySpec{Column: "expires", Interval: "P30D", Unit: "SECONDS"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := schemadiff.CompareWithDialect(ydbTTLDeclaration(test.declared), ydbTTLCatalog(test.read), platform.YDB)
			c.Assert(diff.TablesModified, qt.HasLen, 1)
			c.Assert(diff.TablesModified[0].RowDeletionPolicyChange, qt.DeepEquals, &difftypes.RowDeletionPolicyChange{
				Desired: test.declared, Current: test.read,
			})
		})
	}
}
