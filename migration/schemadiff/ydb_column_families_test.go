package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/ast"
	"ptah.run/core/coverage"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff"
	"ptah.run/migration/schemadiff/difftypes"
)

// ydbFamilyDeclaration declares a table with columns id, body and blob in the
// given column families.
func ydbFamilyDeclaration(families ...ast.YDBColumnFamilySpec) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Doc", Name: "docs", YDBColumnFamilies: families}},
		Fields: []schemamodel.Field{
			{StructName: "Doc", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "Doc", Name: "body", Type: "TEXT", Nullable: true},
			{StructName: "Doc", Name: "blob", Type: "BYTEA", Nullable: true},
		},
	}
}

// ydbFamilyCatalog is the table as the YDB reader reports it, with its column
// families read back as families.
func ydbFamilyCatalog(families ...ast.YDBColumnFamilySpec) *catalog.Database {
	return &catalog.Database{
		Tables: []catalog.Table{{Name: "docs", Type: "TABLE", YDBColumnFamilies: families, Columns: []catalog.Column{
			{Name: "id", DataType: "Int64", ColumnType: "Int64", IsNullable: "NO", IsPrimaryKey: true, OrdinalPosition: 1},
			{Name: "body", DataType: "Utf8", ColumnType: "Utf8", IsNullable: "YES", OrdinalPosition: 2},
			{Name: "blob", DataType: "String", ColumnType: "String", IsNullable: "YES", OrdinalPosition: 3},
		}}},
		Constraints: []catalog.Constraint{{Name: "docs_pkey", TableName: "docs", Type: "PRIMARY KEY",
			ColumnName: "id", ColumnNames: []string{"id"}}},
	}
}

// TestCompare_YDBColumnFamilies_HappyPath plans nothing for a table that
// holds what the declaration states, against the database and against the
// same document alike: the order and the case of a setting do not matter, and
// neither does a setting, a family or a keep_in_memory the table holds and
// the declaration does not state, which a cluster's table profile can give
// every new table. The rows read the table as the YDB reader does, each
// setting at the value the table holds.
func TestCompare_YDBColumnFamilies_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		declared []ast.YDBColumnFamilySpec
		read     []ast.YDBColumnFamilySpec
	}{
		{
			name: "families declared in another order, a setting in capitals",
			declared: []ast.YDBColumnFamilySpec{
				{Name: "warm", Columns: []string{"blob"}},
				{Name: "cold", Data: "hdd", Compression: "LZ4", Columns: []string{"body"}},
			},
			read: []ast.YDBColumnFamilySpec{
				{Name: "cold", Data: "hdd", Compression: "lz4", Columns: []string{"body"}},
				{Name: "default", Compression: "off"},
				{Name: "warm", Compression: "off", Columns: []string{"blob"}},
			},
		},
		{
			name:     "settings and families the declaration does not state",
			declared: []ast.YDBColumnFamilySpec{{Name: "cold", Columns: []string{"body"}}},
			read: []ast.YDBColumnFamilySpec{
				{Name: "cold", Data: "hdd", Compression: "lz4", Columns: []string{"body"}},
				{Name: "default", Compression: "lz4", CacheMode: "in_memory", KeepInMemory: true},
				{Name: "extra", Compression: "lz4"},
			},
		},
		{
			name:     "the default family stating the compression the table holds",
			declared: []ast.YDBColumnFamilySpec{{Name: "default", Compression: "off"}},
			read:     []ast.YDBColumnFamilySpec{{Name: "default", Compression: "off"}},
		},
		{name: "no family on either side"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			against := schemadiff.CompareWithDialect(ydbFamilyDeclaration(test.declared...), ydbFamilyCatalog(test.read...), platform.YDB)
			c.Assert(against.TablesModified, qt.HasLen, 0)
			itself := schemadiff.CompareSchemas(ydbFamilyDeclaration(test.declared...), ydbFamilyDeclaration(test.declared...), platform.YDB)
			c.Assert(itself.TablesModified, qt.HasLen, 0)
		})
	}
}

// TestCompare_YDBColumnFamilies_Change reports a table that does not hold
// what the declaration states. The desired side is what the table holds once
// the declaration is applied: every family it holds stays, and each setting
// the declaration leaves out keeps the value the table holds, so a rebuild
// and a rollback see the whole table.
func TestCompare_YDBColumnFamilies_Change(t *testing.T) {
	tests := []struct {
		name     string
		declared []ast.YDBColumnFamilySpec
		read     []ast.YDBColumnFamilySpec
		want     []ast.YDBColumnFamilySpec
	}{
		{
			name:     "a column in another family",
			declared: []ast.YDBColumnFamilySpec{{Name: "cold", Columns: []string{"body", "blob"}}},
			read:     []ast.YDBColumnFamilySpec{{Name: "cold", Data: "hdd", Compression: "off", Columns: []string{"body"}}},
			want:     []ast.YDBColumnFamilySpec{{Name: "cold", Data: "hdd", Compression: "off", Columns: []string{"blob", "body"}}},
		},
		{
			name:     "a setting",
			declared: []ast.YDBColumnFamilySpec{{Name: "cold", Compression: "lz4", Columns: []string{"body"}}},
			read:     []ast.YDBColumnFamilySpec{{Name: "cold", Compression: "off", Columns: []string{"body"}}},
			want:     []ast.YDBColumnFamilySpec{{Name: "cold", Compression: "lz4", Columns: []string{"body"}}},
		},
		{
			name:     "the default family's compression over a profile's",
			declared: []ast.YDBColumnFamilySpec{{Name: "default", Compression: "off"}},
			read:     []ast.YDBColumnFamilySpec{{Name: "default", Compression: "lz4", KeepInMemory: true}},
			want:     []ast.YDBColumnFamilySpec{{Name: "default", Compression: "off", KeepInMemory: true}},
		},
		{
			name:     "a column out of a family the declaration leaves out",
			declared: nil,
			read:     []ast.YDBColumnFamilySpec{{Name: "cold", Compression: "lz4", Columns: []string{"body"}}},
			want:     []ast.YDBColumnFamilySpec{{Name: "cold", Compression: "lz4"}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := schemadiff.CompareWithDialect(ydbFamilyDeclaration(test.declared...), ydbFamilyCatalog(test.read...), platform.YDB)
			c.Assert(diff.TablesModified, qt.HasLen, 1)
			c.Assert(diff.TablesModified[0].YDBColumnFamiliesChange, qt.DeepEquals,
				&difftypes.YDBColumnFamiliesChange{Desired: test.want, Current: test.read})
			c.Assert(diff.TablesModified[0].Desired.Table.YDBColumnFamilies, qt.DeepEquals, test.want)
		})
	}
}

// TestCompare_YDBColumnFamilies_Coverage reads each side's silence through
// coverage. A document that cannot spell a family -- HCL or DBML -- takes the
// database's families, without the columns it does not declare, and plans
// nothing for them. A read that could not describe a table's families
// withholds the declared ones, and says so, rather than planning against
// families it did not see.
func TestCompare_YDBColumnFamilies_Coverage(t *testing.T) {
	c := qt.New(t)
	held := []ast.YDBColumnFamilySpec{{Name: "cold", Data: "hdd", Columns: []string{"body", "gone"}}}
	silent := ydbFamilyDeclaration()
	silent.NotDescribed = coverage.Set{}.With(coverage.Object{Kind: coverage.ColumnFamily,
		Reason: coverage.Unsupported, Provenance: coverage.DerivedFromFact})
	read := ydbFamilyCatalog(held...)
	read.Tables[0].Columns = append(read.Tables[0].Columns, catalog.Column{Name: "gone", DataType: "Utf8",
		ColumnType: "Utf8", IsNullable: "YES", OrdinalPosition: 4})

	diff := schemadiff.CompareWithDialect(silent, read, platform.YDB)
	c.Assert(diff.TablesModified, qt.HasLen, 1)
	c.Assert(diff.TablesModified[0].YDBColumnFamiliesChange, qt.IsNil)
	c.Assert(diff.TablesModified[0].Desired.Table.YDBColumnFamilies, qt.DeepEquals,
		[]ast.YDBColumnFamilySpec{{Name: "cold", Data: "hdd", Columns: []string{"body"}}})

	unread := ydbFamilyCatalog()
	unread.NotDescribed = coverage.Set{}.With(coverage.Object{Kind: coverage.ColumnFamily, Name: "docs",
		Reason: coverage.Unsupported, Provenance: coverage.Observed})
	opts := config.DefaultCompareOptions()
	opts.Dialect = platform.YDB
	withheld, undecided := schemadiff.CompareReportingUndecidedAdditions(
		ydbFamilyDeclaration(ast.YDBColumnFamilySpec{Name: "cold"}), unread, opts)
	c.Assert(withheld.HasChanges(), qt.IsFalse)
	c.Assert(undecided, qt.DeepEquals, []coverage.Object{{Kind: coverage.ColumnFamily, Name: "docs",
		Reason: coverage.Unsupported, Provenance: coverage.Observed}})
}
