package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff"
	"ptah.run/migration/schemadiff/difftypes"
)

// liveChildWithKeys is a catalog of `c` holding one foreign key per entry of
// keys, from the column it names to p(id), and an index named after each column.
func liveChildWithKeys(keys map[string]string, onDelete string) *catalog.Database {
	database := &catalog.Database{
		Tables: []catalog.Table{
			{Name: "p", Columns: []catalog.Column{{Name: "id", DataType: "int", IsNullable: "NO", IsPrimaryKey: true}}},
			{Name: "c", Columns: []catalog.Column{
				{Name: "id", DataType: "int", IsNullable: "NO", IsPrimaryKey: true},
				{Name: "a", DataType: "int", IsNullable: "YES"},
				{Name: "b", DataType: "int", IsNullable: "YES"},
			}},
		},
	}
	for name, column := range keys {
		database.Indexes = append(database.Indexes, indexOn(column, column))
		database.Constraints = append(database.Constraints, catalog.Constraint{
			Name: name, TableName: "c", Type: "FOREIGN KEY",
			ColumnName: column, ColumnNames: []string{column},
			ForeignTable: new("p"), ForeignColumn: new("id"), ForeignColumns: []string{"id"},
			DeleteRule: new(onDelete), UpdateRule: new("NO ACTION"),
		})
	}
	return database
}

// desiredChildWithKeys declares the same table and one foreign key per entry
// of keys, as a schema file read for the dialect holds them.
func desiredChildWithKeys(keys map[string]string, onDelete string) *schemamodel.Database {
	database := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "P", Name: "p"}, {StructName: "C", Name: "c"}},
		Fields: []schemamodel.Field{
			{StructName: "P", Name: "id", Type: "int", Primary: true},
			{StructName: "C", Name: "id", Type: "int", Primary: true},
			{StructName: "C", Name: "a", Type: "int", Nullable: true},
			{StructName: "C", Name: "b", Type: "int", Nullable: true},
		},
	}
	for name, column := range keys {
		database.Constraints = append(database.Constraints, schemamodel.Constraint{
			StructName: "C", Name: name, Type: "FOREIGN KEY", Table: "c",
			Columns: []string{column}, ForeignTable: "p", ForeignColumn: "id", OnDelete: onDelete,
		})
	}
	return database
}

// compareForDialect is the comparison a plan for the dialect starts from.
func compareForDialect(dialect string, desired *schemamodel.Database, current *catalog.Database) *difftypes.SchemaDiff {
	opts := config.DefaultCompareOptions()
	opts.Dialect = dialect
	return schemadiff.CompareWithOptions(desired, current, opts)
}

// TestCompare_MariaDBNumberedKeyIsTheUnnamedKey covers MariaDB 12.1 and later,
// which name an unnamed key `<n>` where the reader derives `c_ibfk_<n>`.
// Measured on 12.3.3, a file with unnamed keys builds keys `1`, `2` over
// indexes named after their columns, and the file must compare equal to it
// (stokaro/ptah#3743). The second row crosses the numbers: the two schemes
// number a table's keys differently, so the key is found by its definition.
func TestCompare_MariaDBNumberedKeyIsTheUnnamedKey(t *testing.T) {
	tests := []struct {
		name    string
		desired map[string]string
		live    map[string]string
	}{
		{name: "one key", desired: map[string]string{"c_ibfk_1": "a"}, live: map[string]string{"1": "a"}},
		{
			name:    "two keys whose numbers cross",
			desired: map[string]string{"c_ibfk_1": "a", "c_ibfk_2": "b"},
			live:    map[string]string{"1": "b", "2": "a"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := compareForDialect(platform.MariaDB,
				desiredChildWithKeys(test.desired, "NO ACTION"), liveChildWithKeys(test.live, "NO ACTION"))

			c.Assert(diff.ConstraintsAdded, qt.HasLen, 0)
			c.Assert(diff.ConstraintsRemoved, qt.HasLen, 0)
			c.Assert(removedIndexNames(diff), qt.HasLen, 0)
		})
	}
}

// TestCompare_MariaDBNumberedKeyStaysItsOwn keeps the pairing to what it is
// for. A key whose definition changed is replaced, as any changed key is; a
// name of the author's is not one the reader derived; and MySQL never writes
// `<n>`, so there a key of that name is the author's.
func TestCompare_MariaDBNumberedKeyStaysItsOwn(t *testing.T) {
	tests := []struct {
		name        string
		dialect     string
		desiredName string
		desiredOn   string
	}{
		{name: "a changed definition", dialect: platform.MariaDB, desiredName: "c_ibfk_1", desiredOn: "CASCADE"},
		{name: "a name of the author's", dialect: platform.MariaDB, desiredName: "fk_c_a", desiredOn: "NO ACTION"},
		{name: "MySQL", dialect: platform.MySQL, desiredName: "c_ibfk_1", desiredOn: "NO ACTION"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := compareForDialect(test.dialect,
				desiredChildWithKeys(map[string]string{test.desiredName: "a"}, test.desiredOn),
				liveChildWithKeys(map[string]string{"1": "a"}, "NO ACTION"))

			c.Assert(diff.ConstraintsAdded.Names(), qt.DeepEquals, []string{test.desiredName})
			c.Assert(diff.ConstraintsRemoved.Names(), qt.DeepEquals, []string{"1"})
		})
	}
}
