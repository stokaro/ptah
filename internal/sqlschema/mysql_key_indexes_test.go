package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/sqlschema"
)

// keyIndexParents are the tables the keys of the rows below reference.
const keyIndexParents = "CREATE TABLE p (id int PRIMARY KEY, x int, y int, UNIQUE KEY (x, y));\n"

// TestRead_KeyIndexOutlivesItsKey covers stokaro/ptah#3763 and the rest of
// the index a server builds for a foreign key. The server keeps that index
// after the key is dropped, and drops it once an added index begins with its
// columns. Every row was read back from MySQL 8.4.11 and 26.7.0 and MariaDB
// 11.8.9, and the rows that name a key the server named `<table>_ibfk_<n>`
// only there; MariaDB 12.3.3 agrees on the others. want lists the UNIQUEs and
// indexes of `c` the model declares; an index the server built for a key it
// still has is the comparison's to recognize, and not declared.
func TestRead_KeyIndexOutlivesItsKey(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []string
	}{
		{
			name: "a named key's index",
			sql: "CREATE TABLE c (id int PRIMARY KEY, a int, CONSTRAINT fk FOREIGN KEY (a) REFERENCES p(id));\n" +
				"ALTER TABLE c DROP FOREIGN KEY fk;",
			want: []string{"fk(a)"},
		},
		{
			name: "an unnamed key's index",
			sql: "CREATE TABLE c (id int PRIMARY KEY, a int, FOREIGN KEY (a) REFERENCES p(id));\n" +
				"ALTER TABLE c DROP FOREIGN KEY c_ibfk_1;",
			want: []string{"a(a)"},
		},
		{
			name: "a key named the way the server names one",
			sql: "CREATE TABLE c (id int PRIMARY KEY, a int, CONSTRAINT c_ibfk_1 FOREIGN KEY (a) REFERENCES p(id));\n" +
				"ALTER TABLE c DROP FOREIGN KEY c_ibfk_1;",
			want: []string{"c_ibfk_1(a)"},
		},
		{
			name: "an unnamed key's index numbered after an index",
			sql: "CREATE TABLE c (id int PRIMARY KEY, a int, b int, KEY a (b), FOREIGN KEY (a) REFERENCES p(id));\n" +
				"ALTER TABLE c DROP FOREIGN KEY c_ibfk_1;",
			want: []string{"a(b)", "a_2(a)"},
		},
		{
			name: "DROP CONSTRAINT leaves the index too",
			sql: "CREATE TABLE c (id int PRIMARY KEY, a int, CONSTRAINT fk FOREIGN KEY (a) REFERENCES p(id));\n" +
				"ALTER TABLE c DROP CONSTRAINT fk;",
			want: []string{"fk(a)"},
		},
		{
			name: "a key another index covers leaves nothing of its own",
			sql: "CREATE TABLE c (id int PRIMARY KEY, a int, KEY k (a), CONSTRAINT fk FOREIGN KEY (a) REFERENCES p(id));\n" +
				"ALTER TABLE c DROP FOREIGN KEY fk;",
			want: []string{"k(a)"},
		},
		{
			name: "DROP FOREIGN KEY keeps an index of the same name",
			sql: "CREATE TABLE c (id int PRIMARY KEY, a int, KEY fk (a), CONSTRAINT fk FOREIGN KEY (a) REFERENCES p(id));\n" +
				"ALTER TABLE c DROP FOREIGN KEY fk;",
			want: []string{"fk(a)"},
		},
		{
			name: "the kept index outlives an index over other columns",
			sql: "CREATE TABLE c (id int PRIMARY KEY, a int, b int, CONSTRAINT fk FOREIGN KEY (a) REFERENCES p(id));\n" +
				"ALTER TABLE c DROP FOREIGN KEY fk;\n" +
				"ALTER TABLE c ADD KEY (b);",
			want: []string{"b(b)", "fk(a)"},
		},
		{
			name: "the kept index gives way to an index that begins with its columns",
			sql: "CREATE TABLE c (id int PRIMARY KEY, a int, b int, CONSTRAINT fk FOREIGN KEY (a) REFERENCES p(id));\n" +
				"ALTER TABLE c DROP FOREIGN KEY fk;\n" +
				"ALTER TABLE c ADD KEY kab (a, b);",
			want: []string{"kab(a,b)"},
		},
		{
			name: "the kept index gives way to a UNIQUE, which takes its name",
			sql: "CREATE TABLE c (id int PRIMARY KEY, a int, FOREIGN KEY (a) REFERENCES p(id));\n" +
				"ALTER TABLE c DROP FOREIGN KEY c_ibfk_1;\n" +
				"ALTER TABLE c ADD UNIQUE (a);",
			want: []string{"a(a)"},
		},
		{
			name: "the kept index serves a later key, and gives way to a UNIQUE",
			sql: "CREATE TABLE c (id int PRIMARY KEY, a int, CONSTRAINT fk FOREIGN KEY (a) REFERENCES p(id));\n" +
				"ALTER TABLE c DROP FOREIGN KEY fk;\n" +
				"ALTER TABLE c ADD CONSTRAINT fk2 FOREIGN KEY (a) REFERENCES p(id);\n" +
				"ALTER TABLE c ADD UNIQUE (a);",
			want: []string{"a(a)"},
		},
		{
			name: "a later key over the same columns builds the index, and the earlier key leaves none",
			sql: "CREATE TABLE c (id int PRIMARY KEY, a int, CONSTRAINT fk FOREIGN KEY (a) REFERENCES p(id));\n" +
				"ALTER TABLE c ADD CONSTRAINT fk2 FOREIGN KEY (a) REFERENCES p(id);\n" +
				"ALTER TABLE c DROP FOREIGN KEY fk;",
			want: nil,
		},
		{
			name: "a longer key's index replaces the shorter key's and outlives it",
			sql: "CREATE TABLE c (id int PRIMARY KEY, a int, b int, CONSTRAINT fk FOREIGN KEY (a) REFERENCES p(id));\n" +
				"ALTER TABLE c ADD CONSTRAINT fk2 FOREIGN KEY (a, b) REFERENCES p(x, y);\n" +
				"ALTER TABLE c DROP FOREIGN KEY fk2;",
			want: []string{"fk2(a,b)"},
		},
		{
			name: "a shorter key reuses a longer key's index, which outlives the longer key",
			sql: "CREATE TABLE c (id int PRIMARY KEY, a int, b int, CONSTRAINT fk FOREIGN KEY (a, b) REFERENCES p(x, y));\n" +
				"ALTER TABLE c ADD CONSTRAINT fk2 FOREIGN KEY (a) REFERENCES p(id);\n" +
				"ALTER TABLE c DROP FOREIGN KEY fk;",
			want: []string{"fk(a,b)"},
		},
		{
			name: "a shorter key that reused a longer key's index leaves nothing when dropped",
			sql: "CREATE TABLE c (id int PRIMARY KEY, a int, b int, CONSTRAINT fk FOREIGN KEY (a, b) REFERENCES p(x, y));\n" +
				"ALTER TABLE c ADD CONSTRAINT fk2 FOREIGN KEY (a) REFERENCES p(id);\n" +
				"ALTER TABLE c DROP FOREIGN KEY fk2;",
			want: nil,
		},
		{
			name: "the later of two identical keys builds the index, which outlives it",
			sql: "CREATE TABLE c (id int PRIMARY KEY, a int, CONSTRAINT fk FOREIGN KEY (a) REFERENCES p(id), " +
				"CONSTRAINT fk2 FOREIGN KEY (a) REFERENCES p(id));\n" +
				"ALTER TABLE c DROP FOREIGN KEY fk2;",
			want: []string{"fk2(a)"},
		},
		{
			name: "the earlier of two identical keys leaves nothing when dropped",
			sql: "CREATE TABLE c (id int PRIMARY KEY, a int, CONSTRAINT fk FOREIGN KEY (a) REFERENCES p(id), " +
				"CONSTRAINT fk2 FOREIGN KEY (a) REFERENCES p(id));\n" +
				"ALTER TABLE c DROP FOREIGN KEY fk;",
			want: nil,
		},
		{
			name: "a key dropped in the statement that adds a UNIQUE the index does not start",
			sql: "CREATE TABLE c (id int PRIMARY KEY, a int, b int, FOREIGN KEY (a, b) REFERENCES p(x, y));\n" +
				"ALTER TABLE c DROP FOREIGN KEY c_ibfk_1, ADD UNIQUE (a);",
			want: []string{"a(a,b)", "a_2(a)"},
		},
	}
	for _, dialect := range mysqlFamily {
		for _, test := range tests {
			t.Run(dialect+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)

				database, _, err := sqlschema.Read([]byte(keyIndexParents+test.sql), dialect)

				c.Assert(err, qt.IsNil)
				c.Assert(keysOf(database, "c"), qt.DeepEquals, test.want)
			})
		}
	}
}

// TestRead_ForeignKeyClauseIndexIsTheKeys covers stokaro/ptah#3766. The index a
// MySQL `FOREIGN KEY name (columns)` clause names is the one the server builds
// for the key, so it follows the key's rule rather than a declaration's: it is
// not built where another index covers the key, and it gives way to an index
// that begins with its columns. On MariaDB the clause names the key, whose
// index is the comparison's to recognize. Measured on MySQL 8.4.11 and 26.7.0
// and MariaDB 11.8.9 and 12.3.3.
func TestRead_ForeignKeyClauseIndexIsTheKeys(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		mysql   []string
		mariadb []string
	}{
		{
			name: "an added UNIQUE takes the place and the name",
			sql: "CREATE TABLE c (id int PRIMARY KEY, a int, FOREIGN KEY idx (a) REFERENCES p(id));\n" +
				"ALTER TABLE c ADD UNIQUE (a);",
			mysql: []string{"a(a)"}, mariadb: []string{"a(a)"},
		},
		{
			name: "ALTER TABLE adds the key, then a UNIQUE",
			sql: "CREATE TABLE c (id int PRIMARY KEY, a int);\n" +
				"ALTER TABLE c ADD FOREIGN KEY idx (a) REFERENCES p(id);\n" +
				"ALTER TABLE c ADD UNIQUE (a);",
			mysql: []string{"a(a)"}, mariadb: []string{"a(a)"},
		},
		{
			name: "a named index over the same columns",
			sql: "CREATE TABLE c (id int PRIMARY KEY, a int, FOREIGN KEY idx (a) REFERENCES p(id));\n" +
				"ALTER TABLE c ADD KEY idx2 (a);",
			mysql: []string{"idx2(a)"}, mariadb: []string{"idx2(a)"},
		},
		{
			name: "an index that begins with its columns",
			sql: "CREATE TABLE c (id int PRIMARY KEY, a int, b int, FOREIGN KEY idx (a) REFERENCES p(id));\n" +
				"ALTER TABLE c ADD KEY kab (a, b);",
			mysql: []string{"kab(a,b)"}, mariadb: []string{"kab(a,b)"},
		},
		{
			name:  "an index the body declares covers the key",
			sql:   "CREATE TABLE c (id int PRIMARY KEY, a int, b int, KEY k (a), FOREIGN KEY idx (a) REFERENCES p(id));",
			mysql: []string{"k(a)"}, mariadb: []string{"k(a)"},
		},
		{
			name:  "an index the body declares later begins with its columns",
			sql:   "CREATE TABLE c (id int PRIMARY KEY, a int, b int, FOREIGN KEY idx (a) REFERENCES p(id), KEY k (a, b));",
			mysql: []string{"k(a,b)"}, mariadb: []string{"k(a,b)"},
		},
		{
			name: "a longer key's index takes its place",
			sql: "CREATE TABLE c (id int PRIMARY KEY, a int, b int, " +
				"FOREIGN KEY idx (a) REFERENCES p(id), FOREIGN KEY (a, b) REFERENCES p(x, y));",
			mysql: nil, mariadb: nil,
		},
		{
			name: "an index over fewer columns does not replace it",
			sql: "CREATE TABLE c (id int PRIMARY KEY, a int, b int, FOREIGN KEY idx (a, b) REFERENCES p(x, y));\n" +
				"ALTER TABLE c ADD KEY (a);",
			mysql: []string{"a(a)", "idx(a,b)"}, mariadb: []string{"a(a)"},
		},
		{
			name: "an index the author declared does not give way",
			sql: "CREATE TABLE c (id int PRIMARY KEY, a int, KEY idx (a), FOREIGN KEY (a) REFERENCES p(id));\n" +
				"ALTER TABLE c ADD UNIQUE (a);",
			mysql: []string{"a(a)", "idx(a)"}, mariadb: []string{"a(a)", "idx(a)"},
		},
	}
	for _, test := range tests {
		t.Run("mysql/"+test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(keyIndexParents+test.sql), platform.MySQL)

			c.Assert(err, qt.IsNil)
			c.Assert(keysOf(database, "c"), qt.DeepEquals, test.mysql)
		})
		t.Run("mariadb/"+test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(keyIndexParents+test.sql), platform.MariaDB)

			c.Assert(err, qt.IsNil)
			c.Assert(keysOf(database, "c"), qt.DeepEquals, test.mariadb)
		})
	}
}

// TestRead_KeyIndexesNameTheIndexesAfterThem names an unnamed index against
// the indexes the server actually builds for the body's keys: none for a key a
// longer key's index or an identical later key's index covers. Measured on
// MySQL 8.4.11 and 26.7.0 and MariaDB 11.8.9 and 12.3.3.
func TestRead_KeyIndexesNameTheIndexesAfterThem(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []string
	}{
		{
			name: "a longer key's index takes the column's name",
			sql: "CREATE TABLE c (id int PRIMARY KEY, a int, b int, FOREIGN KEY (a) REFERENCES p(id), " +
				"FOREIGN KEY (a, b) REFERENCES p(x, y), KEY (a DESC));",
			want: []string{"a_2(a)"},
		},
		{
			name: "an identical later key builds the index",
			sql: "CREATE TABLE c (id int PRIMARY KEY, a int, b int, CONSTRAINT fk FOREIGN KEY (a) REFERENCES p(id), " +
				"CONSTRAINT fk2 FOREIGN KEY (a) REFERENCES p(id), KEY (a DESC));",
			want: []string{"a(a)"},
		},
	}
	for _, dialect := range mysqlFamily {
		for _, test := range tests {
			t.Run(dialect+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)

				database, _, err := sqlschema.Read([]byte(keyIndexParents+test.sql), dialect)

				c.Assert(err, qt.IsNil)
				c.Assert(keysOf(database, "c"), qt.DeepEquals, test.want)
			})
		}
	}
}

// TestReadOnto_KeyIndexOutlivesItsKeyAcrossFiles drops, in a later file of a
// document, a key an earlier file created: the document carries the index the
// server built for it from one file to the next.
func TestReadOnto_KeyIndexOutlivesItsKeyAcrossFiles(t *testing.T) {
	for _, dialect := range mysqlFamily {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			var earlier schemamodel.Database
			document := sqlschema.NewDocument(&earlier)
			first, _, err := sqlschema.ReadOnto([]byte(keyIndexParents+
				"CREATE TABLE c (id int PRIMARY KEY, a int, FOREIGN KEY (a) REFERENCES p(id));"), dialect, document)
			c.Assert(err, qt.IsNil)
			earlier = first

			later, _, err := sqlschema.ReadOnto([]byte("ALTER TABLE c DROP FOREIGN KEY c_ibfk_1;"), dialect, document)

			c.Assert(err, qt.IsNil)
			c.Assert(keysOf(later, "c"), qt.DeepEquals, []string{"a(a)"})
		})
	}
}
