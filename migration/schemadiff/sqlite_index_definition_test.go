package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
)

// SQLite changes nothing about an index in place, and its reader reports an
// index's uniqueness and each key's column, direction and expression. The
// comparison reads them all through SQLite's index rules, so a changed
// definition replaces the index.

// sqliteIndexedTable declares t (id, a, b) with the index t_a built as
// declared.
func sqliteIndexedTable(index schemamodel.Index) *schemamodel.Database {
	index.StructName, index.Name, index.TableName = "T", "t_a", "t"
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Name: "t", PrimaryKey: []string{"id"}}},
		Fields: []schemamodel.Field{
			{StructName: "T", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "T", Name: "a", Type: "TEXT", Nullable: true},
			{StructName: "T", Name: "b", Type: "TEXT", Nullable: true},
		},
		Indexes: []schemamodel.Index{index},
	}
}

// liveSQLiteIndexedTable is the same table as the SQLite reader reports it,
// with t_a over parts.
func liveSQLiteIndexedTable(unique bool, parts ...catalog.IndexPart) *catalog.Database {
	var columns []string
	for _, part := range parts {
		if part.Name != "" {
			columns = append(columns, part.Name)
		}
	}
	return &catalog.Database{
		Tables: []catalog.Table{{Name: "t", Columns: []catalog.Column{
			{Name: "id", DataType: "INTEGER", IsNullable: "NO", IsPrimaryKey: true},
			{Name: "a", DataType: "TEXT", IsNullable: "YES"},
			{Name: "b", DataType: "TEXT", IsNullable: "YES"},
		}}},
		Indexes: []catalog.Index{{Name: "t_a", TableName: "t", Columns: columns, Parts: parts, IsUnique: unique}},
	}
}

// TestCompare_SQLiteIndexDefinition_HappyPath replaces an index whose
// definition changed.
func TestCompare_SQLiteIndexDefinition_HappyPath(t *testing.T) {
	column := catalog.IndexPart{Name: "a"}
	tests := []struct {
		name    string
		desired schemamodel.Index
		live    catalog.IndexPart
	}{
		{name: "made unique", desired: schemamodel.Index{Fields: []string{"a"}, Unique: true}, live: column},
		{name: "made descending", desired: schemamodel.Index{Parts: []schemamodel.IndexPart{{Name: "a", Desc: true}}}, live: column},
		{name: "another column", desired: schemamodel.Index{Fields: []string{"b"}}, live: column},
		{name: "a second column", desired: schemamodel.Index{Fields: []string{"a", "b"}}, live: column},
		{name: "an expression for a column", desired: schemamodel.Index{Parts: []schemamodel.IndexPart{{Expr: "lower(a)"}}}, live: column},
		{name: "another expression", desired: schemamodel.Index{Parts: []schemamodel.IndexPart{{Expr: "upper(a)"}}},
			live: catalog.IndexPart{Expr: "lower(a)"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := compareForDialect(platform.SQLite, sqliteIndexedTable(test.desired), liveSQLiteIndexedTable(false, test.live))

			c.Assert(diff.IndexesAdded, qt.HasLen, 1)
			c.Assert(diff.IndexesRemoved, qt.HasLen, 1)
		})
	}
}

// TestCompare_SQLiteIndexDefinition_Synced compares an index with the one the
// server built from it, an expression spelled with other spacing and case
// included.
func TestCompare_SQLiteIndexDefinition_Synced(t *testing.T) {
	tests := []struct {
		name    string
		desired schemamodel.Index
		live    *catalog.Database
	}{
		{
			name:    "a unique descending index",
			desired: schemamodel.Index{Parts: []schemamodel.IndexPart{{Name: "a", Desc: true}}, Unique: true},
			live:    liveSQLiteIndexedTable(true, catalog.IndexPart{Name: "a", Desc: true}),
		},
		{
			name:    "an expression",
			desired: schemamodel.Index{Parts: []schemamodel.IndexPart{{Expr: "lower(a)"}}},
			live:    liveSQLiteIndexedTable(false, catalog.IndexPart{Expr: "LOWER( a )"}),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := compareForDialect(platform.SQLite, sqliteIndexedTable(test.desired), test.live)

			c.Assert(diff.HasChanges(), qt.IsFalse)
		})
	}
}
