package sqlschema_test

import (
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/sqlschema"
)

// mysqlNamingParents are the tables every row's keys reference.
const mysqlNamingParents = "CREATE TABLE p (id int PRIMARY KEY);\n" +
	"CREATE TABLE q (a int, b int, UNIQUE KEY (a, b));\n"

// foreignKeysOf lists every foreign key the model carries as
// `<table> <name>(<columns>)`, sorted.
func foreignKeysOf(database schemamodel.Database) []string {
	var keys []string
	for _, constraint := range database.Constraints {
		if strings.EqualFold(constraint.Type, "FOREIGN KEY") {
			keys = append(keys, constraint.Table+" "+constraint.Name+"("+strings.Join(constraint.Columns, ",")+")")
		}
	}
	slices.Sort(keys)
	return keys
}

// TestRead_MySQLUnnamedForeignKeyNames_HappyPath gives an unnamed foreign key
// the name MySQL gives it, so the model read from a schema file carries the
// name the catalog of a database built from the same file reports
// (stokaro/ptah#3725). Every expected name was read back from MySQL 8.4.11
// after running the row's SQL.
func TestRead_MySQLUnnamedForeignKeyNames_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []string
	}{
		{
			name: "one unnamed key",
			sql:  "CREATE TABLE c (id int PRIMARY KEY, p_id int, FOREIGN KEY (p_id) REFERENCES p(id));",
			want: []string{"c c_ibfk_1(p_id)"},
		},
		{
			name: "two unnamed keys, numbered in the order written",
			sql:  "CREATE TABLE c (id int PRIMARY KEY, x int, y int, FOREIGN KEY (y) REFERENCES p(id), FOREIGN KEY (x) REFERENCES p(id));",
			want: []string{"c c_ibfk_1(y)", "c c_ibfk_2(x)"},
		},
		{
			name: "a named key does not move the count",
			sql: "CREATE TABLE c (id int PRIMARY KEY, x int, y int, " +
				"CONSTRAINT c_ibfk_5 FOREIGN KEY (x) REFERENCES p(id), FOREIGN KEY (y) REFERENCES p(id));",
			want: []string{"c c_ibfk_1(y)", "c c_ibfk_5(x)"},
		},
		{
			name: "a name of the author's beside an unnamed key",
			sql: "CREATE TABLE c (id int PRIMARY KEY, x int, y int, " +
				"CONSTRAINT fk_x FOREIGN KEY (x) REFERENCES p(id), FOREIGN KEY (y) REFERENCES p(id));",
			want: []string{"c c_ibfk_1(y)", "c fk_x(x)"},
		},
		{
			name: "a composite key",
			sql:  "CREATE TABLE c (id int PRIMARY KEY, a int, b int, FOREIGN KEY (a, b) REFERENCES q(a, b));",
			want: []string{"c c_ibfk_1(a,b)"},
		},
		{
			name: "an index name in the FOREIGN KEY clause",
			sql:  "CREATE TABLE c (id int PRIMARY KEY, p_id int, FOREIGN KEY idx_c_p (p_id) REFERENCES p(id));",
			want: []string{"c c_ibfk_1(p_id)"},
		},
		{
			name: "the table keeps its case",
			sql:  "CREATE TABLE MixedChild (id int PRIMARY KEY, p_id int, FOREIGN KEY (p_id) REFERENCES p(id));",
			want: []string{"MixedChild MixedChild_ibfk_1(p_id)"},
		},
		{
			name: "ALTER TABLE adds a key to a table with none",
			sql: "CREATE TABLE a0 (id int PRIMARY KEY, p_id int);\n" +
				"ALTER TABLE a0 ADD FOREIGN KEY (p_id) REFERENCES p(id);",
			want: []string{"a0 a0_ibfk_1(p_id)"},
		},
		{
			name: "ALTER TABLE takes one more than the highest number",
			sql: "CREATE TABLE a4 (id int PRIMARY KEY, x int, y int, z int, " +
				"CONSTRAINT a4_ibfk_5 FOREIGN KEY (x) REFERENCES p(id), FOREIGN KEY (y) REFERENCES p(id));\n" +
				"ALTER TABLE a4 ADD FOREIGN KEY (z) REFERENCES p(id);",
			want: []string{"a4 a4_ibfk_1(y)", "a4 a4_ibfk_5(x)", "a4 a4_ibfk_6(z)"},
		},
		{
			name: "ALTER TABLE after a key was dropped",
			sql: "CREATE TABLE a2 (id int PRIMARY KEY, x int, y int, z int, " +
				"FOREIGN KEY (x) REFERENCES p(id), FOREIGN KEY (y) REFERENCES p(id));\n" +
				"ALTER TABLE a2 DROP FOREIGN KEY a2_ibfk_1;\n" +
				"ALTER TABLE a2 ADD FOREIGN KEY (z) REFERENCES p(id);",
			want: []string{"a2 a2_ibfk_2(y)", "a2 a2_ibfk_3(z)"},
		},
		{
			name: "ALTER TABLE beside a name of the author's",
			sql: "CREATE TABLE a9 (id int PRIMARY KEY, x int, y int, CONSTRAINT fk_a9 FOREIGN KEY (x) REFERENCES p(id));\n" +
				"ALTER TABLE a9 ADD FOREIGN KEY (y) REFERENCES p(id);",
			want: []string{"a9 a9_ibfk_1(y)", "a9 fk_a9(x)"},
		},
		{
			name: "two keys in one ALTER TABLE",
			sql: "CREATE TABLE a3 (id int PRIMARY KEY, x int, y int);\n" +
				"ALTER TABLE a3 ADD FOREIGN KEY (x) REFERENCES p(id), ADD FOREIGN KEY (y) REFERENCES p(id);",
			want: []string{"a3 a3_ibfk_1(x)", "a3 a3_ibfk_2(y)"},
		},
		{
			name: "ALTER TABLE does not count a name in another case",
			sql: "CREATE TABLE MixedAlt (id int PRIMARY KEY, x int, y int, " +
				"CONSTRAINT mixedalt_ibfk_3 FOREIGN KEY (x) REFERENCES p(id));\n" +
				"ALTER TABLE MixedAlt ADD FOREIGN KEY (y) REFERENCES p(id);",
			want: []string{"MixedAlt MixedAlt_ibfk_1(y)", "MixedAlt mixedalt_ibfk_3(x)"},
		},
		{
			name: "ALTER TABLE does not count a number with a leading zero",
			sql: "CREATE TABLE f2 (id int PRIMARY KEY, x int, y int, " +
				"CONSTRAINT f2_ibfk_07 FOREIGN KEY (x) REFERENCES p(id));\n" +
				"ALTER TABLE f2 ADD FOREIGN KEY (y) REFERENCES p(id);",
			want: []string{"f2 f2_ibfk_07(x)", "f2 f2_ibfk_1(y)"},
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

// TestReadOnto_MySQLUnnamedForeignKeyNames_CountsEarlierFiles numbers a key an
// ALTER TABLE in a later file adds after the keys the earlier file declared,
// as the server does when it runs the files in order.
func TestReadOnto_MySQLUnnamedForeignKeyNames_CountsEarlierFiles(t *testing.T) {
	c := qt.New(t)
	earlier, _, err := sqlschema.Read([]byte(mysqlNamingParents+
		"CREATE TABLE c (id int PRIMARY KEY, x int, y int, FOREIGN KEY (x) REFERENCES p(id));"), platform.MySQL)
	c.Assert(err, qt.IsNil)

	later, _, err := sqlschema.ReadOnto([]byte(
		"ALTER TABLE c ADD FOREIGN KEY (y) REFERENCES p(id);"), platform.MySQL, &earlier)

	c.Assert(err, qt.IsNil)
	c.Assert(foreignKeysOf(later), qt.DeepEquals, []string{"c c_ibfk_2(y)"})
}

// TestRead_MySQLUnnamedForeignKeyNames_FailurePath refuses a document whose
// unnamed key takes a name another key already holds. MySQL 8.4.11 answers each
// row with `ERROR 1826 (HY000): Duplicate foreign key constraint name`, and a
// model holding both keys would keep one of them.
func TestRead_MySQLUnnamedForeignKeyNames_FailurePath(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "a name written after the unnamed key",
			sql: "CREATE TABLE c5 (id int PRIMARY KEY, x int, y int, " +
				"FOREIGN KEY (y) REFERENCES p(id), CONSTRAINT c5_ibfk_1 FOREIGN KEY (x) REFERENCES p(id));",
		},
		{
			name: "the name in another case",
			sql: "CREATE TABLE g1 (id int PRIMARY KEY, x int, y int, " +
				"FOREIGN KEY (y) REFERENCES p(id), CONSTRAINT G1_IBFK_1 FOREIGN KEY (x) REFERENCES p(id));",
		},
		{
			name: "the name held by another table",
			sql: "CREATE TABLE z6 (id int PRIMARY KEY, x int, CONSTRAINT b6_ibfk_1 FOREIGN KEY (x) REFERENCES p(id));\n" +
				"CREATE TABLE b6 (id int PRIMARY KEY, x int, FOREIGN KEY (x) REFERENCES p(id));",
		},
		{
			name: "ALTER TABLE onto a name another table holds",
			sql: "CREATE TABLE b7 (id int PRIMARY KEY, x int);\n" +
				"CREATE TABLE z7 (id int PRIMARY KEY, x int, CONSTRAINT b7_ibfk_1 FOREIGN KEY (x) REFERENCES p(id));\n" +
				"ALTER TABLE b7 ADD FOREIGN KEY (x) REFERENCES p(id);",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(mysqlNamingParents+test.sql), platform.MySQL)

			c.Assert(err, qt.ErrorIs, sqlschema.ErrDuplicateForeignKeyName)
			c.Assert(database.Constraints, qt.HasLen, 0)
		})
	}
}

// TestRead_UnnamedForeignKeyStaysUnnamedOnMariaDB keeps the rule to the engine
// it was measured on. MariaDB was not measured, so its model keeps no name and
// the comparison gives the key Ptah's default.
func TestRead_UnnamedForeignKeyStaysUnnamedOnMariaDB(t *testing.T) {
	c := qt.New(t)

	database, _, err := sqlschema.Read([]byte(mysqlNamingParents+
		"CREATE TABLE c (id int PRIMARY KEY, p_id int, FOREIGN KEY (p_id) REFERENCES p(id));"), platform.MariaDB)

	c.Assert(err, qt.IsNil)
	c.Assert(foreignKeysOf(database), qt.DeepEquals, []string{"c (p_id)"})
}

// TestToDatabase_AnUnnamedForeignKeysIndexTakesItsColumnsName covers the index
// namespace half of stokaro/ptah#3725. MySQL names the index it builds for an
// unnamed key after the key's first column, at the key's position in the
// statement, so an unnamed index over the same column takes the next name.
// Measured on MySQL 8.4.11: the descending key cannot back the foreign key
// there, so the server builds one of its own.
func TestToDatabase_AnUnnamedForeignKeysIndexTakesItsColumnsName(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []string
	}{
		{
			name: "the key written first",
			sql:  "CREATE TABLE c (id int PRIMARY KEY, a int, FOREIGN KEY (a) REFERENCES p(id), KEY (a DESC));",
			want: []string{"a_2"},
		},
		{
			name: "the index written first",
			sql:  "CREATE TABLE c (id int PRIMARY KEY, a int, KEY (a DESC), FOREIGN KEY (a) REFERENCES p(id));",
			want: []string{"a"},
		},
		{
			name: "an index the key can use builds nothing",
			sql:  "CREATE TABLE c (id int PRIMARY KEY, a int, FOREIGN KEY (a) REFERENCES p(id), KEY (a));",
			want: []string{"a"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database := mysqlSchema(c, "CREATE TABLE p (id int PRIMARY KEY);"+test.sql)

			c.Assert(indexNames(database), qt.DeepEquals, test.want)
		})
	}
}
