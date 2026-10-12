package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
)

// MySQL and MariaDB change no part of an index's key in place, and the reader
// takes each key part's column or expression, its prefix length and its
// direction from information_schema.STATISTICS. A changed part rebuilds the
// index.

// mysqlKeyedTable declares t (id, a, b) with the index t_a over parts.
func mysqlKeyedTable(parts ...schemamodel.IndexPart) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Name: "t", PrimaryKey: []string{"id"}}},
		Fields: []schemamodel.Field{
			{StructName: "T", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "T", Name: "a", Type: "TEXT", Nullable: true},
			{StructName: "T", Name: "b", Type: "TEXT", Nullable: true},
		},
		Indexes: []schemamodel.Index{{StructName: "T", Name: "t_a", TableName: "t", Parts: parts}},
	}
}

// liveMySQLKeyedTable is the same table as the MySQL reader reports it, with
// t_a over parts.
func liveMySQLKeyedTable(parts ...catalog.IndexPart) *catalog.Database {
	var columns []string
	for _, part := range parts {
		if part.Name != "" {
			columns = append(columns, part.Name)
		}
	}
	return &catalog.Database{
		Tables: []catalog.Table{{Name: "t", Columns: []catalog.Column{
			{Name: "id", DataType: "bigint", IsNullable: "NO", IsPrimaryKey: true},
			{Name: "a", DataType: "text", IsNullable: "YES"},
			{Name: "b", DataType: "text", IsNullable: "YES"},
		}}},
		Indexes: []catalog.Index{{Name: "t_a", TableName: "t", Columns: columns, Parts: parts, Method: "BTREE"}},
	}
}

// TestCompare_MySQLIndexKeyParts_HappyPath rebuilds an index one of whose key
// parts changed.
func TestCompare_MySQLIndexKeyParts_HappyPath(t *testing.T) {
	column := catalog.IndexPart{Name: "a"}
	tests := []struct {
		name    string
		desired schemamodel.IndexPart
		live    catalog.IndexPart
	}{
		{name: "made descending", desired: schemamodel.IndexPart{Name: "a", Desc: true}, live: column},
		{name: "a prefix", desired: schemamodel.IndexPart{Name: "a", Prefix: "10"}, live: column},
		{name: "another prefix", desired: schemamodel.IndexPart{Name: "a", Prefix: "20"}, live: catalog.IndexPart{Name: "a", Prefix: "10"}},
		{name: "an expression for a column", desired: schemamodel.IndexPart{Expr: "lower(a)"}, live: column},
		{name: "another expression", desired: schemamodel.IndexPart{Expr: "upper(a)"}, live: catalog.IndexPart{Expr: "lower(`a`)"}},
	}
	for _, dialect := range []string{platform.MySQL, platform.MariaDB} {
		for _, test := range tests {
			t.Run(dialect+" "+test.name, func(t *testing.T) {
				c := qt.New(t)

				diff := compareForDialect(dialect, mysqlKeyedTable(test.desired), liveMySQLKeyedTable(test.live))

				c.Assert(diff.IndexesAdded, qt.HasLen, 1)
				c.Assert(diff.IndexesRemoved, qt.HasLen, 1)
			})
		}
	}
}

// TestCompare_MySQLIndexKeyParts_Synced compares an index with the one the
// server built from it: MySQL keeps a functional key part's expression in its
// own spelling, lower(`a`) for a declared lower(a).
func TestCompare_MySQLIndexKeyParts_Synced(t *testing.T) {
	tests := []struct {
		name    string
		desired schemamodel.IndexPart
		live    catalog.IndexPart
	}{
		{name: "a descending prefix", desired: schemamodel.IndexPart{Name: "a", Prefix: "10", Desc: true},
			live: catalog.IndexPart{Name: "a", Prefix: "10", Desc: true}},
		{name: "an expression", desired: schemamodel.IndexPart{Expr: "lower(a)"}, live: catalog.IndexPart{Expr: "lower(`a`)"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := compareForDialect(platform.MySQL, mysqlKeyedTable(test.desired), liveMySQLKeyedTable(test.live))

			c.Assert(diff.HasChanges(), qt.IsFalse)
		})
	}
}

// TestCompare_PrimaryKeyDeclaredOnColumnAndAsConstraint pairs a key declared
// both on its column and as a PRIMARY KEY constraint with the key the table
// holds, as a read of the table reports it: a PRIMARY row and the column's
// mark. Without the pairing the declared constraint met no database key, and
// the plan added a second primary key.
func TestCompare_PrimaryKeyDeclaredOnColumnAndAsConstraint(t *testing.T) {
	desired := &schemamodel.Database{
		Tables:      []schemamodel.Table{{StructName: "T", Name: "t"}},
		Fields:      []schemamodel.Field{{StructName: "T", Name: "id", Type: "BIGINT", Primary: true}},
		Constraints: []schemamodel.Constraint{{StructName: "T", Table: "t", Name: "PRIMARY", Type: "PRIMARY KEY", Columns: []string{"id"}}},
	}
	current := &catalog.Database{
		Tables:      []catalog.Table{{Name: "t", Columns: []catalog.Column{{Name: "id", DataType: "bigint", IsNullable: "NO", IsPrimaryKey: true}}}},
		Constraints: []catalog.Constraint{{Name: "PRIMARY", TableName: "t", Type: "PRIMARY KEY", ColumnName: "id", ColumnNames: []string{"id"}}},
		Indexes:     []catalog.Index{{Name: "PRIMARY", TableName: "t", Columns: []string{"id"}, IsUnique: true, IsPrimary: true, Method: "BTREE"}},
	}
	for _, dialect := range []string{platform.MySQL, platform.MariaDB} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			diff := compareForDialect(dialect, desired, current)

			c.Assert(diff.ConstraintsAdded, qt.HasLen, 0)
			c.Assert(diff.ConstraintsRemoved, qt.HasLen, 0)
		})
	}
}
