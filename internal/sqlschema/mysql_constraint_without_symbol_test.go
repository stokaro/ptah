package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/sqlschema"
)

// TestRead_MySQLForeignKeyWithoutSymbolNames_HappyPath names a foreign key
// written `CONSTRAINT FOREIGN KEY`, with the keyword and no symbol, in the one
// sequence MySQL numbers every unnamed key of the table from
// (stokaro/ptah#3730). Every expected name was read back from MySQL 8.4.11
// after running the row's SQL.
func TestRead_MySQLForeignKeyWithoutSymbolNames_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []string
	}{
		{
			name: "one key",
			sql:  "CREATE TABLE c (id int PRIMARY KEY, p_id int, CONSTRAINT FOREIGN KEY (p_id) REFERENCES p(id));",
			want: []string{"c c_ibfk_1(p_id)"},
		},
		{
			name: "numbered with the keys written without the keyword",
			sql: "CREATE TABLE c (id int PRIMARY KEY, x int, y int, z int, w int, " +
				"FOREIGN KEY (x) REFERENCES p(id), CONSTRAINT FOREIGN KEY (y) REFERENCES p(id), " +
				"CONSTRAINT c_ibfk_5 FOREIGN KEY (z) REFERENCES p(id), CONSTRAINT FOREIGN KEY (w) REFERENCES p(id));",
			want: []string{"c c_ibfk_1(x)", "c c_ibfk_2(y)", "c c_ibfk_3(w)", "c c_ibfk_5(z)"},
		},
		{
			name: "an index name in the FOREIGN KEY clause",
			sql:  "CREATE TABLE c (id int PRIMARY KEY, p_id int, CONSTRAINT FOREIGN KEY idx_c_p (p_id) REFERENCES p(id));",
			want: []string{"c c_ibfk_1(p_id)"},
		},
		{
			name: "ALTER TABLE takes one more than the highest number",
			sql: "CREATE TABLE c (id int PRIMARY KEY, x int, y int, CONSTRAINT c_ibfk_5 FOREIGN KEY (x) REFERENCES p(id));\n" +
				"ALTER TABLE c ADD CONSTRAINT FOREIGN KEY (y) REFERENCES p(id);",
			want: []string{"c c_ibfk_5(x)", "c c_ibfk_6(y)"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(mysqlNamingParents+test.sql), platform.MySQL)

			c.Assert(err, qt.IsNil)
			c.Assert(foreignKeysOf(database), qt.DeepEquals, test.want)
		})
	}
}

// TestRead_MySQLForeignKeyWithoutSymbolNames_FailurePath refuses a key written
// without a symbol whose derived name another key holds, as MySQL 8.4.11
// refuses it with `ERROR 1826 (HY000): Duplicate foreign key constraint name
// 'c_ibfk_1'`.
func TestRead_MySQLForeignKeyWithoutSymbolNames_FailurePath(t *testing.T) {
	c := qt.New(t)

	database, _, err := sqlschema.Read([]byte(mysqlNamingParents+
		"CREATE TABLE c (id int PRIMARY KEY, x int, y int, "+
		"CONSTRAINT FOREIGN KEY (y) REFERENCES p(id), CONSTRAINT c_ibfk_1 FOREIGN KEY (x) REFERENCES p(id));"),
		platform.MySQL)

	c.Assert(err, qt.ErrorIs, sqlschema.ErrDuplicateForeignKeyName)
	c.Assert(database.Constraints, qt.HasLen, 0)
}

// TestRead_ConstraintWithoutSymbolReadsAsTheClauseAlone reads a whole document
// that writes its constraints after a CONSTRAINT without a symbol into the model
// the same document gives without the keyword: the same kinds, the same
// columns and the same names. MySQL 8.4.11 and MariaDB 11.8.9 build the same
// catalog from both spellings of each row.
func TestRead_ConstraintWithoutSymbolReadsAsTheClauseAlone(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		with    string
		without string
	}{
		{
			name:    "every table-level kind on MySQL",
			dialect: platform.MySQL,
			with: "CREATE TABLE c (id int, a int, b int, p_id int, CONSTRAINT PRIMARY KEY (id), " +
				"CONSTRAINT UNIQUE (a), CONSTRAINT UNIQUE KEY uq (b), CONSTRAINT CHECK (a > 0), " +
				"CONSTRAINT FOREIGN KEY (p_id) REFERENCES p(id));",
			without: "CREATE TABLE c (id int, a int, b int, p_id int, PRIMARY KEY (id), " +
				"UNIQUE (a), UNIQUE KEY uq (b), CHECK (a > 0), " +
				"FOREIGN KEY (p_id) REFERENCES p(id));",
		},
		{
			name:    "every table-level kind on MariaDB",
			dialect: platform.MariaDB,
			with: "CREATE TABLE c (id int, a int, b int, p_id int, CONSTRAINT PRIMARY KEY (id), " +
				"CONSTRAINT UNIQUE (a), CONSTRAINT UNIQUE KEY uq (b), CONSTRAINT CHECK (a > 0), " +
				"CONSTRAINT FOREIGN KEY (p_id) REFERENCES p(id));",
			without: "CREATE TABLE c (id int, a int, b int, p_id int, PRIMARY KEY (id), " +
				"UNIQUE (a), UNIQUE KEY uq (b), CHECK (a > 0), " +
				"FOREIGN KEY (p_id) REFERENCES p(id));",
		},
		{
			name:    "two unique keys on one column",
			dialect: platform.MySQL,
			with:    "CREATE TABLE c (id int PRIMARY KEY, a int, UNIQUE (a), CONSTRAINT UNIQUE (a));",
			without: "CREATE TABLE c (id int PRIMARY KEY, a int, UNIQUE (a), UNIQUE (a));",
		},
		{
			name:    "a check on a column on MySQL",
			dialect: platform.MySQL,
			with:    "CREATE TABLE c (id int PRIMARY KEY, a int CONSTRAINT CHECK (a > 0), b int, CHECK (b > a));",
			without: "CREATE TABLE c (id int PRIMARY KEY, a int CHECK (a > 0), b int, CHECK (b > a));",
		},
		{
			name:    "a reference on a column on MariaDB",
			dialect: platform.MariaDB,
			with:    "CREATE TABLE c (id int PRIMARY KEY, a int CONSTRAINT REFERENCES p(id));",
			without: "CREATE TABLE c (id int PRIMARY KEY, a int REFERENCES p(id));",
		},
		{
			name:    "constraints ALTER TABLE adds on MySQL",
			dialect: platform.MySQL,
			with: "CREATE TABLE c (id int, a int, p_id int);\n" +
				"ALTER TABLE c ADD CONSTRAINT PRIMARY KEY (id), ADD CONSTRAINT UNIQUE (a), " +
				"ADD CONSTRAINT CHECK (a > 0), ADD CONSTRAINT FOREIGN KEY (p_id) REFERENCES p(id);",
			without: "CREATE TABLE c (id int, a int, p_id int);\n" +
				"ALTER TABLE c ADD PRIMARY KEY (id), ADD UNIQUE (a), " +
				"ADD CHECK (a > 0), ADD FOREIGN KEY (p_id) REFERENCES p(id);",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			with, _, err := sqlschema.Read([]byte(mysqlNamingParents+test.with), test.dialect)
			c.Assert(err, qt.IsNil)
			without, _, err := sqlschema.Read([]byte(mysqlNamingParents+test.without), test.dialect)
			c.Assert(err, qt.IsNil)

			c.Assert(with, qt.DeepEquals, without)
		})
	}
}
