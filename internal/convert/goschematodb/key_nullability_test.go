package goschematodb_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/convert/goschematodb"
	"ptah.run/internal/sqlschema"
)

// columnNullability reads a schema file for dialect, converts it to the
// catalog form, and reports each column's IS NULLABLE by `table.column`.
func columnNullability(c *qt.C, sql, dialect string) map[string]string {
	c.Helper()
	desired, _, err := sqlschema.Read([]byte(sql), dialect)
	c.Assert(err, qt.IsNil)
	nullability := make(map[string]string)
	for _, table := range goschematodb.ToDBSchema(&desired, dialect).Tables {
		for _, column := range table.Columns {
			nullability[table.Name+"."+column.Name] = column.IsNullable
		}
	}
	return nullability
}

// TestToDBSchema_KeyColumnsAreNotNullWhereTheServerMakesThemSo describes a
// schema file the way the server that ran it reports it. The file keeps
// `id bigint PRIMARY KEY` as a nullable field; the server holds the column NOT
// NULL on every engine but a SQLite rowid table. Described as nullable, the
// file compared with itself planned SET NOT NULL on every key column
// (stokaro/ptah#3658).
func TestToDBSchema_KeyColumnsAreNotNullWhereTheServerMakesThemSo(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		sql     string
		want    map[string]string
	}{
		{
			name:    "PostgreSQL column and table-level keys",
			dialect: "postgres",
			sql: "CREATE TABLE a (id bigint PRIMARY KEY, note text);\n" +
				"CREATE TABLE b (x bigint, y bigint, note text, PRIMARY KEY (x, y));",
			want: map[string]string{"a.id": "NO", "a.note": "YES", "b.x": "NO", "b.y": "NO", "b.note": "YES"},
		},
		{
			name:    "SQLite rowid table keeps its key nullable",
			dialect: "sqlite",
			sql:     "CREATE TABLE a (id TEXT PRIMARY KEY, note TEXT);",
			want:    map[string]string{"a.id": "YES", "a.note": "YES"},
		},
		{
			name:    "SQLite STRICT table holds its key NOT NULL",
			dialect: "sqlite",
			sql:     "CREATE TABLE a (id TEXT PRIMARY KEY, note TEXT) STRICT;",
			want:    map[string]string{"a.id": "NO", "a.note": "YES"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := columnNullability(c, test.sql, test.dialect)

			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}
