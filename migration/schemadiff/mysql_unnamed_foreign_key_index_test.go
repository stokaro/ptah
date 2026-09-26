package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff"
)

// liveChildWithKey is a catalog of `c` holding the foreign key keyName over
// p_id, and the indexes given.
func liveChildWithKey(keyName string, indexes ...catalog.Index) *catalog.Database {
	return &catalog.Database{
		Tables: []catalog.Table{
			{Name: "p", Columns: []catalog.Column{{Name: "id", DataType: "int", IsNullable: "NO", IsPrimaryKey: true}}},
			{Name: "c", Columns: []catalog.Column{
				{Name: "id", DataType: "int", IsNullable: "NO", IsPrimaryKey: true},
				{Name: "p_id", DataType: "int", IsNullable: "YES"},
				{Name: "o", DataType: "int", IsNullable: "YES"},
			}},
		},
		Indexes: indexes,
		Constraints: []catalog.Constraint{{
			Name: keyName, TableName: "c", Type: "FOREIGN KEY",
			ColumnName: "p_id", ColumnNames: []string{"p_id"},
			ForeignTable: new("p"), ForeignColumn: new("id"), ForeignColumns: []string{"id"},
			DeleteRule: new("NO ACTION"), UpdateRule: new("NO ACTION"),
		}},
	}
}

// desiredChildWithKey declares the same table, the key keyName and the
// indexes given, as a schema file read for the dialect holds them.
func desiredChildWithKey(keyName string, indexes ...schemamodel.Index) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "P", Name: "p"}, {StructName: "C", Name: "c"}},
		Fields: []schemamodel.Field{
			{StructName: "P", Name: "id", Type: "int", Primary: true},
			{StructName: "C", Name: "id", Type: "int", Primary: true},
			{StructName: "C", Name: "p_id", Type: "int", Nullable: true},
			{StructName: "C", Name: "o", Type: "int", Nullable: true},
		},
		Indexes: indexes,
		Constraints: []schemamodel.Constraint{{
			StructName: "C", Name: keyName, Type: "FOREIGN KEY", Table: "c",
			Columns: []string{"p_id"}, ForeignTable: "p", ForeignColumn: "id",
		}},
	}
}

// indexOn is an index of `c` as a catalog reports it.
func indexOn(name string, columns ...string) catalog.Index {
	return catalog.Index{Name: name, TableName: "c", Columns: columns}
}

// removedIndexes is the index removals a comparison planned for the dialect.
func removedIndexes(dialect string, desired *schemamodel.Database, current *catalog.Database) []string {
	opts := config.DefaultCompareOptions()
	opts.Dialect = dialect
	return removedIndexNames(schemadiff.CompareWithOptions(desired, current, opts))
}

// TestCompare_TheIndexMySQLBuildsForAnUnnamedKeyIsTheKeys covers the
// comparison half of stokaro/ptah#3725.
//
// MySQL names an unnamed key `<table>_ibfk_<n>` and the index it builds for it
// after the key's first column, `_2` and on when that is taken. Measured on
// MySQL 8.4.11: `FOREIGN KEY (p_id) REFERENCES p(id)` holds `c_ibfk_1` over an
// index `p_id`, and beside an index already called `p_id` over another column
// the key's index is `p_id_2`. Read as an index nobody declared, it is planned
// for removal, which the server refuses while the key needs it.
func TestCompare_TheIndexMySQLBuildsForAnUnnamedKeyIsTheKeys(t *testing.T) {
	tests := []struct {
		name     string
		current  *catalog.Database
		declared []schemamodel.Index
	}{
		{
			name:    "named after the key's column",
			current: liveChildWithKey("c_ibfk_1", indexOn("p_id", "p_id")),
		},
		{
			name:     "numbered after an index that holds the column's name",
			current:  liveChildWithKey("c_ibfk_1", indexOn("p_id", "o"), indexOn("p_id_2", "p_id")),
			declared: []schemamodel.Index{{StructName: "C", Name: "p_id", Fields: []string{"o"}, TableName: "c"}},
		},
		{
			name:    "a key the server numbered past one",
			current: liveChildWithKey("c_ibfk_6", indexOn("p_id", "p_id")),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			key := test.current.Constraints[0].Name

			removed := removedIndexes(platform.MySQL, desiredChildWithKey(key, test.declared...), test.current)

			c.Assert(removed, qt.HasLen, 0)
		})
	}
}

// TestCompare_AColumnNamedIndexBesideANamedKeyIsTheAuthors is the control the
// constraint's name is there for. A key written with a name gets an index
// under that name, so an index named after the column beside it is one the
// author made, and dropping it from the declaration plans its removal. MariaDB
// was not measured, so it keeps the name rule alone.
func TestCompare_AColumnNamedIndexBesideANamedKeyIsTheAuthors(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		key     string
	}{
		{name: "a key of the author's name on MySQL", dialect: platform.MySQL, key: "fk_c_p"},
		{name: "an ibfk name with a leading zero on MySQL", dialect: platform.MySQL, key: "c_ibfk_01"},
		{name: "an unnamed key's name on MariaDB", dialect: platform.MariaDB, key: "c_ibfk_1"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			removed := removedIndexes(test.dialect, desiredChildWithKey(test.key), liveChildWithKey(test.key, indexOn("p_id", "p_id")))

			c.Assert(removed, qt.DeepEquals, []string{"p_id"})
		})
	}
}
