package schemadiff_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/difftypes"
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

// columnChanges lists the columns a diff modifies, each as `table.column:
// change`, sorted.
func columnChanges(diff *difftypes.SchemaDiff) []string {
	var changes []string
	for _, table := range diff.TablesModified {
		for _, column := range table.ColumnsModified {
			for key, change := range column.Changes {
				changes = append(changes, table.TableName+"."+column.ColumnName+": "+key+" "+change)
			}
		}
	}
	slices.Sort(changes)
	return changes
}

// desiredUniqueIndex is desiredUniqueTable with a unique index over a, as
// `CREATE UNIQUE INDEX ux ON c (a)` declares it.
func desiredUniqueIndex() *schemamodel.Database {
	database := desiredUniqueTable()
	database.Indexes = []schemamodel.Index{{StructName: "C", Name: "ux", TableName: "c", Fields: []string{"a"}, Unique: true}}
	return database
}

// TestCompare_AColumnUniqueBesideADeclaredKeyIsItsOwn covers
// stokaro/ptah#3784. On MySQL and MariaDB a column's UNIQUE is a key of its
// own beside a named UNIQUE or a unique index over the same column, so a
// database that holds only the declared one lacks the column's. Measured on
// MySQL 8.4.11 and 26.7.0 and MariaDB 11.8.9 and 12.3.3: the file builds both
// keys, and Atlas CE v1.3.0 plans `ADD UNIQUE INDEX a (a)` against a database
// that holds only the declared one.
func TestCompare_AColumnUniqueBesideADeclaredKeyIsItsOwn(t *testing.T) {
	tests := []struct {
		name    string
		desired *schemamodel.Database
		live    []uniqueKey
		want    []string
	}{
		{
			name:    "a named UNIQUE, and no key of the column's",
			desired: desiredUniqueTable(uniqueKey{name: "uq_a", columns: []string{"a"}}),
			live:    []uniqueKey{{name: "uq_a", columns: []string{"a"}}},
			want:    []string{"c.a: unique false -> true"},
		},
		{
			name:    "a unique index, and no key of the column's",
			desired: desiredUniqueIndex(),
			live:    []uniqueKey{{name: "ux", columns: []string{"a"}}},
			want:    []string{"c.a: unique false -> true"},
		},
		{
			name:    "a named UNIQUE beside the column's key",
			desired: desiredUniqueTable(uniqueKey{name: "uq_a", columns: []string{"a"}}),
			live:    []uniqueKey{{name: "a", columns: []string{"a"}}, {name: "uq_a", columns: []string{"a"}}},
		},
		{
			name:    "a unique index beside the column's key",
			desired: desiredUniqueIndex(),
			live:    []uniqueKey{{name: "a", columns: []string{"a"}}, {name: "ux", columns: []string{"a"}}},
		},
	}
	for _, dialect := range mysqlEngines {
		for _, test := range tests {
			t.Run(dialect+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)

				diff := compareForDialect(dialect, test.desired, liveUniqueTable(test.live...))

				c.Assert(columnChanges(diff), qt.DeepEquals, test.want)
				c.Assert(diff.ConstraintsAdded, qt.HasLen, 0)
				c.Assert(diff.ConstraintsRemoved, qt.HasLen, 0)
				c.Assert(diff.IndexesAdded, qt.HasLen, 0)
				c.Assert(diff.IndexRemovals(), qt.HasLen, 0)
			})
		}
	}
}

// TestCompare_PostgresColumnUniqueBesideADeclaredKey keeps PostgreSQL apart.
// Measured on PostgreSQL 18, `CREATE TABLE c (a int UNIQUE, CONSTRAINT uq_a
// UNIQUE (a))` builds `uq_a` alone, so a database that holds it holds the
// column's key too. A unique index is an object apart, and `a int UNIQUE`
// beside `CREATE UNIQUE INDEX ux ON c (a)` builds `c_a_key` and `ux`, which
// Atlas CE v1.3.0 plans as `ADD CONSTRAINT c_a_key` against a database that
// holds only `ux`.
func TestCompare_PostgresColumnUniqueBesideADeclaredKey(t *testing.T) {
	tests := []struct {
		name    string
		desired *schemamodel.Database
		live    *catalog.Database
		want    []string
	}{
		{
			name:    "a named UNIQUE is the column's key",
			desired: desiredUniqueTable(uniqueKey{name: "uq_a", columns: []string{"a"}}),
			live:    postgresUniqueTable([]uniqueKey{{name: "uq_a", columns: []string{"a"}}}, nil),
		},
		{
			name:    "a unique index is not",
			desired: desiredUniqueIndex(),
			live:    postgresUniqueTable(nil, []uniqueKey{{name: "ux", columns: []string{"a"}}}),
			want:    []string{"c.a: unique false -> true"},
		},
		{
			name:    "a unique index beside the column's key",
			desired: desiredUniqueIndex(),
			live: postgresUniqueTable([]uniqueKey{{name: "c_a_key", columns: []string{"a"}}},
				[]uniqueKey{{name: "ux", columns: []string{"a"}}}),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := compareForDialect(platform.Postgres, test.desired, test.live)

			c.Assert(columnChanges(diff), qt.DeepEquals, test.want)
			c.Assert(diff.ConstraintsAdded, qt.HasLen, 0)
			c.Assert(diff.ConstraintsRemoved, qt.HasLen, 0)
			c.Assert(diff.IndexesAdded, qt.HasLen, 0)
			c.Assert(diff.IndexRemovals(), qt.HasLen, 0)
		})
	}
}

// postgresUniqueTable is a catalog of `c (id, a, b)` in the shape the
// PostgreSQL reader reports: a UNIQUE constraint and its index per constraint,
// an index alone per unique index, and a column unique where either covers
// that column alone.
func postgresUniqueTable(constraints, indexes []uniqueKey) *catalog.Database {
	database := liveUniqueTable(constraints...)
	for _, index := range indexes {
		database.Indexes = append(database.Indexes, catalog.Index{
			Name: index.name, TableName: "c", Columns: index.columns, IsUnique: true,
		})
		for i := range database.Tables[0].Columns {
			column := &database.Tables[0].Columns[i]
			column.IsUnique = column.IsUnique || slices.Equal(index.columns, []string{column.Name})
		}
	}
	return database
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
