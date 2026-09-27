package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/sqlschema"
)

// TestCompare_PostgresColumnKeyBesideANamedUniqueFromSQL drives the path that
// joins the SQL reader and the comparison on PostgreSQL (stokaro/ptah#3812).
// The reader folds a column's UNIQUE and an equal named UNIQUE of one CREATE
// TABLE into the named key, which is what the server builds; the comparison
// then reads a pair that is still in the model as two keys. Measured on
// PostgreSQL 18.6: the first file builds `uq_a` alone, and the other two build
// `c_a_key` and `uq_a`. Atlas CE v1.3.0 reports the first synced with a
// database that holds only `uq_a`, and plans `ADD CONSTRAINT c_a_key` for the
// other two.
func TestCompare_PostgresColumnKeyBesideANamedUniqueFromSQL(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		live []uniqueKey
		want []string
	}{
		{
			name: "one CREATE TABLE, against the named key alone",
			sql:  "CREATE TABLE c (id int PRIMARY KEY, a int UNIQUE, b int, CONSTRAINT uq_a UNIQUE (a));",
			live: []uniqueKey{{name: "uq_a", columns: []string{"a"}}},
		},
		{
			name: "a later ALTER TABLE, against the named key alone",
			sql: "CREATE TABLE c (id int PRIMARY KEY, a int UNIQUE, b int);\n" +
				"ALTER TABLE c ADD CONSTRAINT uq_a UNIQUE (a);",
			live: []uniqueKey{{name: "uq_a", columns: []string{"a"}}},
			want: []string{"c.a: unique false -> true"},
		},
		{
			name: "a later ALTER TABLE, against both keys",
			sql: "CREATE TABLE c (id int PRIMARY KEY, a int UNIQUE, b int);\n" +
				"ALTER TABLE c ADD CONSTRAINT uq_a UNIQUE (a);",
			live: []uniqueKey{{name: "c_a_key", columns: []string{"a"}}, {name: "uq_a", columns: []string{"a"}}},
		},
		{
			name: "an ALTER TABLE that adds the column and the key, against the named key alone",
			sql: "CREATE TABLE c (id int PRIMARY KEY, b int);\n" +
				"ALTER TABLE c ADD COLUMN a int UNIQUE, ADD CONSTRAINT uq_a UNIQUE (a);",
			live: []uniqueKey{{name: "uq_a", columns: []string{"a"}}},
			want: []string{"c.a: unique false -> true"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired, _, err := sqlschema.Read([]byte(test.sql), platform.Postgres)
			c.Assert(err, qt.IsNil)

			diff := compareForDialect(platform.Postgres, &desired, postgresUniqueTable(test.live, nil))

			c.Assert(columnChanges(diff), qt.DeepEquals, test.want)
			c.Assert(diff.ConstraintsAdded, qt.HasLen, 0)
			c.Assert(diff.ConstraintsRemoved, qt.HasLen, 0)
		})
	}
}
