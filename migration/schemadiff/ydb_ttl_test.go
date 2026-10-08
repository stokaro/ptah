package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/coverage"
	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/engine/builtin"
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
		FeatureCoverage: must.Must(ydbschema.ChangefeedCoverage(schemaext.Observed, nil)),
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
			against := must.Must(schemadiff.CompareWithDialect(t.Context(), ydbTTLDeclaration(test.declared), ydbTTLCatalog(test.read), platform.YDB, must.Must(builtin.New())))
			c.Assert(against.TablesModified, qt.HasLen, 0)
			itself := must.Must(schemadiff.CompareSchemas(t.Context(), ydbTTLDeclaration(test.declared), ydbTTLDeclaration(test.declared), platform.YDB, must.Must(builtin.New())))
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
			diff := must.Must(schemadiff.CompareWithDialect(t.Context(), ydbTTLDeclaration(test.declared), ydbTTLCatalog(test.read), platform.YDB, must.Must(builtin.New())))
			c.Assert(diff.TablesModified, qt.HasLen, 1)
			c.Assert(diff.TablesModified[0].RowDeletionPolicyChange, qt.DeepEquals, &difftypes.RowDeletionPolicyChange{
				Desired: test.declared, Current: test.read,
			})
		})
	}
}

// A desired state that does not describe TTLs -- an HCL document says so in
// its header, because HCL has no spelling for one -- is silent about a table's
// TTL rather than asking for its removal, so nothing is planned for it.
func TestCompare_YDBTTL_UndescribedIsNotRemoved(t *testing.T) {
	tests := []struct {
		name         string
		notDescribed coverage.Set
	}{
		{name: "every TTL undescribed", notDescribed: coverage.Set{}.WithKind(coverage.TTL)},
		{name: "this table's TTL undescribed", notDescribed: coverage.Set{}.WithObject(coverage.TTL, "events")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			declared := ydbTTLDeclaration(nil)
			declared.NotDescribed = test.notDescribed

			diff := must.Must(schemadiff.CompareWithDialect(t.Context(), declared,
				ydbTTLCatalog(&ast.RowDeletionPolicySpec{Column: "ts", Interval: "P30D"}), platform.YDB, must.Must(builtin.New())))

			c.Assert(diff.TablesModified, qt.HasLen, 0)
		})
	}
}

// The gate withholds only the removal of an undescribed policy: a policy the
// description declares is planned whatever it says about the others, and a
// record about another table, or none, leaves the removal planned.
func TestCompare_YDBTTL_UndescribedGatesOnlyItsRemoval(t *testing.T) {
	read := &ast.RowDeletionPolicySpec{Column: "ts", Interval: "P30D"}
	tests := []struct {
		name         string
		notDescribed coverage.Set
		declared     *ast.RowDeletionPolicySpec
	}{
		{name: "another table's TTL undescribed", notDescribed: coverage.Set{}.WithObject(coverage.TTL, "other")},
		{
			name:         "a declared policy where TTLs are undescribed",
			notDescribed: coverage.Set{}.WithKind(coverage.TTL),
			declared:     &ast.RowDeletionPolicySpec{Column: "ts", Interval: "P1D"},
		},
		{name: "every TTL described"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			declared := ydbTTLDeclaration(test.declared)
			declared.NotDescribed = test.notDescribed

			diff := must.Must(schemadiff.CompareWithDialect(t.Context(), declared, ydbTTLCatalog(read), platform.YDB, must.Must(builtin.New())))

			c.Assert(diff.TablesModified, qt.HasLen, 1)
			c.Assert(diff.TablesModified[0].RowDeletionPolicyChange, qt.DeepEquals,
				&difftypes.RowDeletionPolicyChange{Desired: test.declared, Current: read})
		})
	}
}
