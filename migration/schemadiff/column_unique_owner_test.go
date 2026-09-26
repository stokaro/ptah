package schemadiff_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
)

// uniqueKey is one UNIQUE of `c`: its name and its columns.
type uniqueKey struct {
	name    string
	columns []string
}

// liveUniqueTable is a catalog of `c (id, a, b)` holding keys, in the shape
// the MySQL and MariaDB readers report one: a UNIQUE constraint and a unique
// index per key, and a column unique where a key covers that column alone.
func liveUniqueTable(keys ...uniqueKey) *catalog.Database {
	database := &catalog.Database{Tables: []catalog.Table{{Name: "c", Columns: []catalog.Column{
		{Name: "id", DataType: "int", IsNullable: "NO", IsPrimaryKey: true},
		{Name: "a", DataType: "int", IsNullable: "YES"},
		{Name: "b", DataType: "int", IsNullable: "YES"},
	}}}}
	for _, key := range keys {
		database.Constraints = append(database.Constraints, catalog.Constraint{
			Name: key.name, TableName: "c", Type: "UNIQUE", ColumnName: key.columns[0], ColumnNames: key.columns,
		})
		database.Indexes = append(database.Indexes, catalog.Index{
			Name: key.name, TableName: "c", Columns: key.columns, IsUnique: true,
		})
		for i := range database.Tables[0].Columns {
			column := &database.Tables[0].Columns[i]
			column.IsUnique = column.IsUnique || slices.Equal(key.columns, []string{column.Name})
		}
	}
	return database
}

// desiredUniqueTable declares `c (id, a UNIQUE, b)` and the table-level keys
// given, as a schema file read for the dialect holds them.
func desiredUniqueTable(keys ...uniqueKey) *schemamodel.Database {
	database := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "C", Name: "c"}},
		Fields: []schemamodel.Field{
			{StructName: "C", Name: "id", Type: "int", Primary: true},
			{StructName: "C", Name: "a", Type: "int", Nullable: true, Unique: true},
			{StructName: "C", Name: "b", Type: "int", Nullable: true},
		},
	}
	for _, key := range keys {
		database.Constraints = append(database.Constraints, schemamodel.Constraint{
			StructName: "C", Name: key.name, Type: "UNIQUE", Table: "c", Columns: key.columns,
		})
	}
	return database
}

// mysqlEngines are the dialects the rows below were measured on, with the
// catalog shape liveUniqueTable writes.
var mysqlEngines = []string{platform.MySQL, platform.MariaDB}

// TestCompare_AColumnUniqueBesideAnotherUniqueIsSynced covers
// stokaro/ptah#3764. A column's own UNIQUE accounts for one key over that
// column alone, so another key of the table is paired by name. Each row is a
// file and the catalog MySQL 8.4.11 and 26.7.0 and MariaDB 11.8.9 and 12.3.3
// built from it, which Atlas CE v1.3.0 reports synced.
func TestCompare_AColumnUniqueBesideAnotherUniqueIsSynced(t *testing.T) {
	tests := []struct {
		name     string
		declared []uniqueKey
		live     []uniqueKey
	}{
		{
			name:     "a key led by the column",
			declared: []uniqueKey{{name: "a_2", columns: []string{"a", "b"}}},
			live:     []uniqueKey{{name: "a", columns: []string{"a"}}, {name: "a_2", columns: []string{"a", "b"}}},
		},
		{
			name:     "a named key over the column alone",
			declared: []uniqueKey{{name: "uq_a", columns: []string{"a"}}},
			live:     []uniqueKey{{name: "a", columns: []string{"a"}}, {name: "uq_a", columns: []string{"a"}}},
		},
		{
			name:     "a key the column does not lead",
			declared: []uniqueKey{{name: "b", columns: []string{"b", "a"}}},
			live:     []uniqueKey{{name: "a", columns: []string{"a"}}, {name: "b", columns: []string{"b", "a"}}},
		},
		{
			name:     "a declared key that is the only key over the column",
			declared: []uniqueKey{{name: "uq_a", columns: []string{"a"}}},
			live:     []uniqueKey{{name: "uq_a", columns: []string{"a"}}},
		},
	}
	for _, dialect := range mysqlEngines {
		for _, test := range tests {
			t.Run(dialect+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)

				diff := compareForDialect(dialect, desiredUniqueTable(test.declared...), liveUniqueTable(test.live...))

				c.Assert(diff.ConstraintsAdded, qt.HasLen, 0)
				c.Assert(diff.ConstraintsRemoved, qt.HasLen, 0)
				c.Assert(diff.IndexRemovals(), qt.HasLen, 0)
				c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("%+v", diff))
			})
		}
	}
}

// TestCompare_AUniqueBesideAColumnUniqueIsRemoved is the other direction of
// stokaro/ptah#3764: a key the file does not declare is dropped. Read as the
// column's own, it is never planned. Atlas CE v1.3.0 plans each drop, measured
// on the same servers. The last two rows choose which of two
// keys over the column is the column's: the one named after the column, then
// the first by name.
func TestCompare_AUniqueBesideAColumnUniqueIsRemoved(t *testing.T) {
	tests := []struct {
		name string
		live []uniqueKey
		want []string
	}{
		{
			name: "a second key over the column alone",
			live: []uniqueKey{{name: "a", columns: []string{"a"}}, {name: "a_2", columns: []string{"a"}}},
			want: []string{"a_2"},
		},
		{
			name: "a key led by the column",
			live: []uniqueKey{{name: "a", columns: []string{"a"}}, {name: "uq_ab", columns: []string{"a", "b"}}},
			want: []string{"uq_ab"},
		},
		{
			name: "a key led by the column, where the column has no key of its own",
			live: []uniqueKey{{name: "uq_ab", columns: []string{"a", "b"}}},
			want: []string{"uq_ab"},
		},
		{
			name: "a unique index over the column",
			live: []uniqueKey{{name: "a", columns: []string{"a"}}, {name: "ux_a", columns: []string{"a"}}},
			want: []string{"ux_a"},
		},
		{
			name: "the key named after the column is the column's, whatever sorts first",
			live: []uniqueKey{{name: "UQ_A", columns: []string{"a"}}, {name: "a", columns: []string{"a"}}},
			want: []string{"UQ_A"},
		},
		{
			name: "no key named after the column: the first by name is the column's",
			live: []uniqueKey{{name: "uq_y", columns: []string{"a"}}, {name: "uq_x", columns: []string{"a"}}},
			want: []string{"uq_y"},
		},
	}
	for _, dialect := range mysqlEngines {
		for _, test := range tests {
			t.Run(dialect+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)

				diff := compareForDialect(dialect, desiredUniqueTable(), liveUniqueTable(test.live...))

				c.Assert(diff.ConstraintsRemoved.Names(), qt.DeepEquals, test.want)
				c.Assert(diff.ConstraintsAdded, qt.HasLen, 0)
				c.Assert(diff.IndexRemovals(), qt.HasLen, 0)
			})
		}
	}
}

// TestCompare_PostgresColumnUniqueIsNamedForTheTable ranks the name
// PostgreSQL gives a column's own UNIQUE, `<table>_<column>_key`, first:
// measured on PostgreSQL 18, `a int UNIQUE` builds `c_a_key`, so a second key
// over the column is the one to drop even where its name sorts first.
func TestCompare_PostgresColumnUniqueIsNamedForTheTable(t *testing.T) {
	c := qt.New(t)
	live := liveUniqueTable(
		uniqueKey{name: "a_uq", columns: []string{"a"}},
		uniqueKey{name: "c_a_key", columns: []string{"a"}},
	)
	live.Indexes = nil

	diff := compareForDialect(platform.Postgres, desiredUniqueTable(), live)

	c.Assert(diff.ConstraintsRemoved.Names(), qt.DeepEquals, []string{"a_uq"})
	c.Assert(diff.ConstraintsAdded, qt.HasLen, 0)
}

// TestCompare_TheColumnsKeyUnderAnotherNameIsTheColumns is the control for
// the rows above: a column's UNIQUE is compared by its column, not by its
// name, so the one key over the column is the column's whatever it is called.
func TestCompare_TheColumnsKeyUnderAnotherNameIsTheColumns(t *testing.T) {
	for _, dialect := range mysqlEngines {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			diff := compareForDialect(dialect, desiredUniqueTable(),
				liveUniqueTable(uniqueKey{name: "uq_x", columns: []string{"a"}}))

			c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("%+v", diff))
		})
	}
}
