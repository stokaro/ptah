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

// mysqlFamily is the dialects whose unnamed foreign keys the rows below were
// measured on.
var mysqlFamily = []string{platform.MySQL, platform.MariaDB}

// TestRead_MySQLUnnamedForeignKeyNames_HappyPath gives an unnamed foreign key
// the name MySQL and MariaDB give it, so the model read from a schema file
// carries the name the catalog of a database built from the same file reports
// (stokaro/ptah#3725, stokaro/ptah#3743). Every expected name was read back
// from MySQL 8.4.11, MySQL 26.7.0 and MariaDB 11.8.9 after running the row's
// SQL, and the three agree on every row.
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
			name: "CONSTRAINT without a symbol counts with the unnamed keys",
			sql: "CREATE TABLE c (id int PRIMARY KEY, x int, y int, z int, w int, " +
				"FOREIGN KEY (x) REFERENCES p(id), CONSTRAINT FOREIGN KEY (y) REFERENCES p(id), " +
				"CONSTRAINT c_ibfk_5 FOREIGN KEY (z) REFERENCES p(id), CONSTRAINT FOREIGN KEY (w) REFERENCES p(id));",
			want: []string{"c c_ibfk_1(x)", "c c_ibfk_2(y)", "c c_ibfk_3(w)", "c c_ibfk_5(z)"},
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
		{
			name: "ALTER TABLE does not count a key it names before the unnamed one",
			sql: "CREATE TABLE c (id int PRIMARY KEY, a int, b int, FOREIGN KEY (a) REFERENCES p(id));\n" +
				"ALTER TABLE c ADD CONSTRAINT c_ibfk_9 FOREIGN KEY (a) REFERENCES p(id), " +
				"ADD FOREIGN KEY (b) REFERENCES p(id);",
			want: []string{"c c_ibfk_1(a)", "c c_ibfk_2(b)", "c c_ibfk_9(a)"},
		},
		{
			name: "ALTER TABLE does not count a key it names after the unnamed one",
			sql: "CREATE TABLE c (id int PRIMARY KEY, a int, b int, FOREIGN KEY (a) REFERENCES p(id));\n" +
				"ALTER TABLE c ADD CONSTRAINT FOREIGN KEY (b) REFERENCES p(id), " +
				"ADD CONSTRAINT c_ibfk_9 FOREIGN KEY (a) REFERENCES p(id);",
			want: []string{"c c_ibfk_1(a)", "c c_ibfk_2(b)", "c c_ibfk_9(a)"},
		},
		{
			name: "ALTER TABLE counts a key the same statement drops",
			sql: "CREATE TABLE c (id int PRIMARY KEY, a int, b int, CONSTRAINT c_ibfk_3 FOREIGN KEY (a) REFERENCES p(id));\n" +
				"ALTER TABLE c DROP FOREIGN KEY c_ibfk_3, ADD FOREIGN KEY (b) REFERENCES p(id);",
			want: []string{"c c_ibfk_4(b)"},
		},
	}
	for _, dialect := range mysqlFamily {
		for _, test := range tests {
			t.Run(dialect+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)

				database, _, err := sqlschema.Read([]byte(mysqlNamingParents+test.sql), dialect)

				c.Assert(err, qt.IsNil)
				c.Assert(foreignKeysOf(database), qt.DeepEquals, test.want)
			})
		}
	}
}

// TestRead_ForeignKeyClauseIndexName follows the one rule the engines disagree
// on: an index name written inside a FOREIGN KEY clause with no symbol names
// the key on MariaDB and only the key's index on MySQL. Measured, `FOREIGN KEY
// idx_c_p (p_id)` holds key `c_ibfk_1` over index `idx_c_p` on MySQL 8.4.11
// and 26.7.0, and key `idx_c_p` over index `idx_c_p` on MariaDB 11.8.9, in
// CREATE TABLE and in ALTER TABLE ... ADD alike.
func TestRead_ForeignKeyClauseIndexName(t *testing.T) {
	tests := []struct {
		name        string
		dialect     string
		sql         string
		wantKeys    []string
		wantIndexes []string
	}{
		{
			name:        "MySQL, CREATE TABLE",
			dialect:     platform.MySQL,
			sql:         "CREATE TABLE c (id int PRIMARY KEY, p_id int, FOREIGN KEY idx_c_p (p_id) REFERENCES p(id));",
			wantKeys:    []string{"c c_ibfk_1(p_id)"},
			wantIndexes: []string{"idx_c_p"},
		},
		{
			name:    "MySQL, ALTER TABLE",
			dialect: platform.MySQL,
			sql: "CREATE TABLE c (id int PRIMARY KEY, p_id int);\n" +
				"ALTER TABLE c ADD FOREIGN KEY idx_c_p (p_id) REFERENCES p(id);",
			wantKeys:    []string{"c c_ibfk_1(p_id)"},
			wantIndexes: []string{"idx_c_p"},
		},
		{
			name:        "MariaDB, CREATE TABLE",
			dialect:     platform.MariaDB,
			sql:         "CREATE TABLE c (id int PRIMARY KEY, p_id int, FOREIGN KEY idx_c_p (p_id) REFERENCES p(id));",
			wantKeys:    []string{"c idx_c_p(p_id)"},
			wantIndexes: make([]string, 0),
		},
		{
			name:    "MariaDB, ALTER TABLE",
			dialect: platform.MariaDB,
			sql: "CREATE TABLE c (id int PRIMARY KEY, p_id int);\n" +
				"ALTER TABLE c ADD FOREIGN KEY idx_c_p (p_id) REFERENCES p(id);",
			wantKeys:    []string{"c idx_c_p(p_id)"},
			wantIndexes: make([]string, 0),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(mysqlNamingParents+test.sql), test.dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(foreignKeysOf(database), qt.DeepEquals, test.wantKeys)
			c.Assert(indexNames(database), qt.DeepEquals, test.wantIndexes)
		})
	}
}

// columnForeignKeysOf lists every foreign key a column declares as
// `<name>(<column>)`, sorted.
func columnForeignKeysOf(database schemamodel.Database) []string {
	var keys []string
	for _, field := range database.Fields {
		if field.Foreign != "" {
			keys = append(keys, field.ForeignKeyName+"("+field.Name+")")
		}
	}
	slices.Sort(keys)
	return keys
}

// TestRead_MariaDBColumnForeignKeyNames names the key a column declares with
// REFERENCES, which MariaDB builds and numbers with the table's other unnamed
// keys. Measured on MariaDB 11.8.9 and MySQL 26.7.0; MySQL 8.4 builds nothing
// from the clause, and the parser refuses it for MySQL.
func TestRead_MariaDBColumnForeignKeyNames(t *testing.T) {
	tests := []struct {
		name        string
		sql         string
		wantColumns []string
		wantTable   []string
	}{
		{
			name:        "a column's key comes before the table's",
			sql:         "CREATE TABLE c (id int PRIMARY KEY, a int REFERENCES p(id), b int, FOREIGN KEY (b) REFERENCES p(id));",
			wantColumns: []string{"c_ibfk_1(a)"},
			wantTable:   []string{"c c_ibfk_2(b)"},
		},
		{
			name:        "a key the column names does not move the count",
			sql:         "CREATE TABLE c (id int PRIMARY KEY, a int CONSTRAINT fkx REFERENCES p(id), b int REFERENCES p(id));",
			wantColumns: []string{"c_ibfk_1(b)", "fkx(a)"},
		},
		{
			name: "ADD COLUMN numbers in the statement's order",
			sql: "CREATE TABLE c (id int PRIMARY KEY, a int, CONSTRAINT c_ibfk_2 FOREIGN KEY (a) REFERENCES p(id));\n" +
				"ALTER TABLE c ADD COLUMN b int REFERENCES p(id), ADD FOREIGN KEY (a) REFERENCES p(id);",
			wantColumns: []string{"c_ibfk_3(b)"},
			wantTable:   []string{"c c_ibfk_2(a)", "c c_ibfk_4(a)"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(mysqlNamingParents+test.sql), platform.MariaDB)

			c.Assert(err, qt.IsNil)
			c.Assert(columnForeignKeysOf(database), qt.DeepEquals, test.wantColumns)
			c.Assert(foreignKeysOf(database), qt.DeepEquals, test.wantTable)
		})
	}
}

// TestReadOnto_MySQLUnnamedForeignKeyNames_CountsEarlierFiles numbers a key an
// ALTER TABLE in a later file adds after the keys the earlier file declared,
// as the server does when it runs the files in order.
func TestReadOnto_MySQLUnnamedForeignKeyNames_CountsEarlierFiles(t *testing.T) {
	for _, dialect := range mysqlFamily {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			earlier, _, err := sqlschema.Read([]byte(mysqlNamingParents+
				"CREATE TABLE c (id int PRIMARY KEY, x int, y int, FOREIGN KEY (x) REFERENCES p(id));"), dialect)
			c.Assert(err, qt.IsNil)

			later, _, err := sqlschema.ReadOnto([]byte(
				"ALTER TABLE c ADD FOREIGN KEY (y) REFERENCES p(id);"), dialect, &earlier)

			c.Assert(err, qt.IsNil)
			c.Assert(foreignKeysOf(later), qt.DeepEquals, []string{"c c_ibfk_2(y)"})
		})
	}
}

// TestRead_MySQLUnnamedForeignKeyNames_FailurePath refuses a document whose
// unnamed key takes a name another key already holds. MySQL 8.4.11 and 26.7.0
// answer each row with `ERROR 1826 (HY000): Duplicate foreign key constraint
// name`, MariaDB 11.8.9 with `ERROR 1005 (HY000)`, and a model holding both
// keys would keep one of them.
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
	for _, dialect := range mysqlFamily {
		for _, test := range tests {
			t.Run(dialect+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)

				database, _, err := sqlschema.Read([]byte(mysqlNamingParents+test.sql), dialect)

				c.Assert(err, qt.ErrorIs, sqlschema.ErrDuplicateForeignKeyName)
				c.Assert(database.Constraints, qt.HasLen, 0)
			})
		}
	}
}

// TestRead_MariaDBColumnForeignKeyNames_FailurePath refuses a derived name a
// column's key collides with, in either direction. Measured on MariaDB
// 11.8.9, each row is `ERROR 1005 (HY000)`, with errno 150 on one table and
// errno 121 across two.
func TestRead_MariaDBColumnForeignKeyNames_FailurePath(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "a column's derived name a table-level key holds",
			sql: "CREATE TABLE c (id int PRIMARY KEY, a int REFERENCES p(id), b int, " +
				"CONSTRAINT c_ibfk_1 FOREIGN KEY (b) REFERENCES p(id));",
		},
		{
			name: "a derived name a column's key holds",
			sql: "CREATE TABLE c (id int PRIMARY KEY, a int CONSTRAINT c_ibfk_1 REFERENCES p(id), b int, " +
				"FOREIGN KEY (b) REFERENCES p(id));",
		},
		{
			name: "a derived name another table's column holds",
			sql: "CREATE TABLE other (id int PRIMARY KEY, a int CONSTRAINT c_ibfk_1 REFERENCES p(id));\n" +
				"CREATE TABLE c (id int PRIMARY KEY, a int REFERENCES p(id));",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(mysqlNamingParents+test.sql), platform.MariaDB)

			c.Assert(err, qt.ErrorIs, sqlschema.ErrDuplicateForeignKeyName)
			c.Assert(database.Constraints, qt.HasLen, 0)
		})
	}
}

// TestRead_UnnamedForeignKeyStaysUnnamedOutsideTheMySQLFamily keeps the rule
// to the engines it was measured on. SQLite keeps no name for the key, so its
// model keeps none and the comparison gives the key Ptah's default.
func TestRead_UnnamedForeignKeyStaysUnnamedOutsideTheMySQLFamily(t *testing.T) {
	c := qt.New(t)

	database, _, err := sqlschema.Read([]byte(mysqlNamingParents+
		"CREATE TABLE c (id int PRIMARY KEY, p_id int, FOREIGN KEY (p_id) REFERENCES p(id));"), platform.SQLite)

	c.Assert(err, qt.IsNil)
	c.Assert(foreignKeysOf(database), qt.DeepEquals, []string{"c (p_id)"})
}

// TestToDatabase_AnUnnamedForeignKeysIndexTakesItsColumnsName covers the index
// namespace half of stokaro/ptah#3725 and stokaro/ptah#3743. MySQL and MariaDB
// name the index they build for an unnamed key after the key's first column,
// at the key's position in the statement, so an unnamed index over the same
// column takes the next name. Measured on MySQL 8.4.11, 26.7.0 and MariaDB
// 11.8.9. A descending key cannot back the foreign key on MySQL, so the server
// builds one of its own; MariaDB reuses it and builds nothing.
func TestToDatabase_AnUnnamedForeignKeysIndexTakesItsColumnsName(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		sql     string
		want    []string
	}{
		{
			name:    "MySQL, the key written first",
			dialect: platform.MySQL,
			sql:     "CREATE TABLE c (id int PRIMARY KEY, a int, FOREIGN KEY (a) REFERENCES p(id), KEY (a DESC));",
			want:    []string{"a_2"},
		},
		{
			name:    "MySQL, the index written first",
			dialect: platform.MySQL,
			sql:     "CREATE TABLE c (id int PRIMARY KEY, a int, KEY (a DESC), FOREIGN KEY (a) REFERENCES p(id));",
			want:    []string{"a"},
		},
		{
			name:    "MariaDB, a descending index the key can use builds nothing",
			dialect: platform.MariaDB,
			sql:     "CREATE TABLE c (id int PRIMARY KEY, a int, FOREIGN KEY (a) REFERENCES p(id), KEY (a DESC));",
			want:    []string{"a"},
		},
		{
			name:    "an index the key can use builds nothing",
			dialect: platform.MySQL,
			sql:     "CREATE TABLE c (id int PRIMARY KEY, a int, FOREIGN KEY (a) REFERENCES p(id), KEY (a));",
			want:    []string{"a"},
		},
		{
			name:    "MySQL, a composite key written first",
			dialect: platform.MySQL,
			sql:     "CREATE TABLE c (id int PRIMARY KEY, a int, b int, FOREIGN KEY (a, b) REFERENCES q(a, b), KEY (a));",
			want:    []string{"a_2"},
		},
		{
			name:    "MariaDB, a composite key written first",
			dialect: platform.MariaDB,
			sql:     "CREATE TABLE c (id int PRIMARY KEY, a int, b int, FOREIGN KEY (a, b) REFERENCES q(a, b), KEY (a));",
			want:    []string{"a_2"},
		},
		{
			name:    "MariaDB, a composite key written after the index",
			dialect: platform.MariaDB,
			sql:     "CREATE TABLE c (id int PRIMARY KEY, a int, b int, KEY (a), FOREIGN KEY (a, b) REFERENCES q(a, b));",
			want:    []string{"a"},
		},
		{
			name:    "MariaDB, an index that covers a column's key takes the column's name",
			dialect: platform.MariaDB,
			sql:     "CREATE TABLE c (id int PRIMARY KEY, a int REFERENCES p(id), b int, KEY (a, b));",
			want:    []string{"a"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(mysqlNamingParents+test.sql), test.dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(indexNames(database), qt.DeepEquals, test.want)
		})
	}
}

// TestToDatabase_AColumnsForeignKeyIndexHoldsItsName_FailurePath refuses an
// index that takes the name the index of a column's key holds. Measured on
// MariaDB 11.8.9 and MySQL 26.7.0, both rows are `ERROR 1061 Duplicate key
// name`.
func TestToDatabase_AColumnsForeignKeyIndexHoldsItsName_FailurePath(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "an unnamed key's index",
			sql:  "CREATE TABLE c (id int PRIMARY KEY, a int REFERENCES p(id), KEY a (id));",
		},
		{
			name: "a named key's index",
			sql:  "CREATE TABLE c (id int PRIMARY KEY, a int CONSTRAINT fkx REFERENCES p(id), KEY fkx (id));",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(mysqlNamingParents+test.sql), platform.MariaDB)

			c.Assert(err, qt.ErrorIs, sqlschema.ErrDuplicateIndexName)
			c.Assert(database.Indexes, qt.HasLen, 0)
		})
	}
}

// TestRead_MariaDBRestatedColumnKey_FailurePath refuses a restated column that
// carries REFERENCES. Measured on MariaDB 11.8.9 and 12.3.3, the server skips
// the column and still adds a second key over it, which a restatement cannot
// say.
func TestRead_MariaDBRestatedColumnKey_FailurePath(t *testing.T) {
	c := qt.New(t)

	database, _, err := sqlschema.Read([]byte(mysqlNamingParents+
		"CREATE TABLE c (id int PRIMARY KEY, a int REFERENCES p(id));\n"+
		"ALTER TABLE c ADD COLUMN IF NOT EXISTS a int REFERENCES p(id);"), platform.MariaDB)

	c.Assert(err, qt.ErrorIs, sqlschema.ErrUnmodeledStatement)
	c.Assert(err, qt.ErrorMatches, `(?s).*skips the column but still adds the foreign key.*`)
	c.Assert(database.Fields, qt.HasLen, 0)
}

// TestRead_MariaDBRestatedColumnWithoutKey is the control: a restated column
// with no REFERENCES creates nothing on the server and is read as nothing.
func TestRead_MariaDBRestatedColumnWithoutKey(t *testing.T) {
	c := qt.New(t)

	database, _, err := sqlschema.Read([]byte(mysqlNamingParents+
		"CREATE TABLE c (id int PRIMARY KEY, a int REFERENCES p(id), b int);\n"+
		"ALTER TABLE c ADD COLUMN IF NOT EXISTS b int;"), platform.MariaDB)

	c.Assert(err, qt.IsNil)
	c.Assert(columnForeignKeysOf(database), qt.DeepEquals, []string{"c_ibfk_1(a)"})
}
