package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/sqlschema"
)

// columnKeyParents is the table the keys of the rows below reference.
const columnKeyParents = "CREATE TABLE p (id int PRIMARY KEY);\n"

// TestRead_MySQLFamilyColumnKeyNamesFollowTheBody covers stokaro/ptah#3803. A
// column's own UNIQUE takes its index name at the column's place in the body,
// so an index written before the column takes the bare name first. Every row
// was read back from MySQL 8.4.11 and 26.7.0 and MariaDB 11.8.9 and 12.3.3.
// want lists the UNIQUEs and indexes of `c` the model declares; the column's
// own UNIQUE stays on the column, and its name decides the names around it.
func TestRead_MySQLFamilyColumnKeyNamesFollowTheBody(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []string
	}{
		{
			name: "an index over the column written before it",
			sql:  "CREATE TABLE c (id int PRIMARY KEY, KEY (a), a int UNIQUE);",
			want: []string{"a(a)"},
		},
		{
			name: "an index the column leads, written before it",
			sql:  "CREATE TABLE c (id int PRIMARY KEY, b int, KEY (a, b), a int UNIQUE);",
			want: []string{"a(a,b)"},
		},
		{
			name: "a UNIQUE the column leads, written before it",
			sql:  "CREATE TABLE c (id int PRIMARY KEY, b int, UNIQUE KEY (a, b), a int UNIQUE);",
			want: []string{"a(a,b)"},
		},
		{
			name: "a UNIQUE over the column, written before it",
			sql:  "CREATE TABLE c (id int PRIMARY KEY, UNIQUE KEY (a), a int UNIQUE);",
			want: []string{"a(a)"},
		},
		{
			name: "an index named after the column, written before it",
			sql:  "CREATE TABLE c (id int PRIMARY KEY, b int, KEY a (b), a int UNIQUE);",
			want: []string{"a(b)"},
		},
		{
			name: "an index over the column on both sides of it",
			sql:  "CREATE TABLE c (id int PRIMARY KEY, KEY (a), a int UNIQUE, KEY (a));",
			want: []string{"a(a)", "a_3(a)"},
		},
		{
			name: "an index over the column written after it",
			sql:  "CREATE TABLE c (id int PRIMARY KEY, a int UNIQUE, KEY (a));",
			want: []string{"a_2(a)"},
		},
		{
			name: "indexes on both sides of the column",
			sql:  "CREATE TABLE c (id int PRIMARY KEY, b int, KEY (b), a int UNIQUE, KEY (a), KEY (a));",
			want: []string{"a_2(a)", "a_3(a)", "b(b)"},
		},
		{
			name: "an index before one column's UNIQUE and after another's",
			sql:  "CREATE TABLE c (id int PRIMARY KEY, INDEX (a), a int, b int UNIQUE, INDEX (b));",
			want: []string{"a(a)", "b_2(b)"},
		},
	}
	for _, dialect := range mysqlFamily {
		for _, test := range tests {
			t.Run(dialect+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)

				database, _, err := sqlschema.Read([]byte(columnKeyParents+test.sql), dialect)

				c.Assert(err, qt.IsNil)
				c.Assert(keysOf(database, "c"), qt.DeepEquals, test.want)
			})
		}
	}
}

// TestRead_MariaDBColumnReferencesIndexFollowsTheBody is the same rule for the
// index MariaDB builds for a column's REFERENCES, which takes its name, and
// decides which of two identical keys is the later one, at the column's place.
// Every row was read back from MariaDB 11.8.9 and 12.3.3; MySQL 26.7.0 agrees
// where it builds a key from the clause, and Ptah refuses the clause for MySQL.
// want lists the UNIQUEs and indexes of `c` the model declares.
func TestRead_MariaDBColumnReferencesIndexFollowsTheBody(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []string
	}{
		{
			name: "an index named after the column, written before it",
			sql:  "CREATE TABLE c (id int PRIMARY KEY, b int, KEY a (b), a int REFERENCES p(id));",
			want: []string{"a(b)"},
		},
		{
			name: "the column's UNIQUE covers the key",
			sql:  "CREATE TABLE c (id int PRIMARY KEY, b int, KEY a (b), a int UNIQUE REFERENCES p(id));",
			want: []string{"a(b)"},
		},
		{
			name: "a table's key over the column, written before it, leaves nothing when dropped",
			sql: "CREATE TABLE c (id int PRIMARY KEY, CONSTRAINT fk FOREIGN KEY (a) REFERENCES p(id), " +
				"a int REFERENCES p(id));\nALTER TABLE c DROP FOREIGN KEY fk;",
			want: nil,
		},
		{
			name: "a table's key over the column, written after it, leaves its index when dropped",
			sql: "CREATE TABLE c (id int PRIMARY KEY, a int REFERENCES p(id), " +
				"CONSTRAINT fk FOREIGN KEY (a) REFERENCES p(id));\nALTER TABLE c DROP FOREIGN KEY fk;",
			want: []string{"fk(a)"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(columnKeyParents+test.sql), platform.MariaDB)

			c.Assert(err, qt.IsNil)
			c.Assert(keysOf(database, "c"), qt.DeepEquals, test.want)
		})
	}
}

// TestRead_MariaDBColumnReferencesIndexFollowsTheBody_FailurePath refuses a
// document the server refuses because a column's key takes a name first.
// Measured on MariaDB 11.8.9 and 12.3.3, `KEY (a, b), a int REFERENCES p(id),
// d int REFERENCES p(id), KEY d (b)` is `ERROR 1061 (42000): Duplicate key
// name 'd'`: the index of d's key takes d at d's place.
func TestRead_MariaDBColumnReferencesIndexFollowsTheBody_FailurePath(t *testing.T) {
	c := qt.New(t)

	database, _, err := sqlschema.Read([]byte(columnKeyParents+
		"CREATE TABLE c (id int PRIMARY KEY, b int, KEY (a, b), a int REFERENCES p(id), "+
		"d int REFERENCES p(id), KEY d (b));"), platform.MariaDB)

	c.Assert(err, qt.ErrorIs, sqlschema.ErrDuplicateIndexName)
	c.Assert(database.Indexes, qt.HasLen, 0)
}
