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

// keysOf lists every UNIQUE and every index of table as `<name>(<columns>)`,
// sorted: the objects that share one index namespace on MySQL and MariaDB.
func keysOf(database schemamodel.Database, table string) []string {
	var keys []string
	for _, constraint := range database.Constraints {
		if strings.EqualFold(constraint.Type, "UNIQUE") && constraint.Table == table {
			keys = append(keys, constraint.Name+"("+strings.Join(constraint.Columns, ",")+")")
		}
	}
	for _, index := range database.Indexes {
		if index.TableName == table {
			keys = append(keys, index.Name+"("+strings.Join(index.Fields, ",")+")")
		}
	}
	slices.Sort(keys)
	return keys
}

// alterIndexParents are the tables the foreign keys of the rows below
// reference.
const alterIndexParents = "CREATE TABLE p (id int PRIMARY KEY, x int, y int, UNIQUE KEY (x, y));\n"

// TestRead_AlterTableUnnamedIndexNames_HappyPath gives an unnamed UNIQUE or
// index an ALTER TABLE adds the name MySQL and MariaDB give it, so a schema
// file compares equal to the database its own SQL built (stokaro/ptah#3742).
// Every row was read back from MySQL 8.4.11, MySQL 26.7.0 and MariaDB 11.8.9
// after running its SQL, and the three agree on each.
func TestRead_AlterTableUnnamedIndexNames_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []string
	}{
		{
			name: "the first column's name",
			sql: "CREATE TABLE c (id int PRIMARY KEY, a int);\n" +
				"ALTER TABLE c ADD UNIQUE (a);",
			want: []string{"a(a)"},
		},
		{
			name: "an index holds the name",
			sql: "CREATE TABLE c (id int PRIMARY KEY, a int, b int, KEY a (b));\n" +
				"ALTER TABLE c ADD UNIQUE (a);",
			want: []string{"a(b)", "a_2(a)"},
		},
		{
			name: "an index holds the name in another case",
			sql: "CREATE TABLE c (id int PRIMARY KEY, a int, b int, KEY A (b));\n" +
				"ALTER TABLE c ADD UNIQUE (a);",
			want: []string{"A(b)", "a_2(a)"},
		},
		{
			name: "the numbered name is taken too",
			sql: "CREATE TABLE c (id int PRIMARY KEY, a int, b int, KEY a (b), KEY a_2 (b));\n" +
				"ALTER TABLE c ADD UNIQUE (a);",
			want: []string{"a(b)", "a_2(b)", "a_3(a)"},
		},
		{
			name: "a column's own UNIQUE holds the name",
			sql: "CREATE TABLE c (id int PRIMARY KEY, a int UNIQUE, b int);\n" +
				"ALTER TABLE c ADD UNIQUE (a, b);",
			want: []string{"a_2(a,b)"},
		},
		{
			name: "every spelling of an unnamed key in one statement",
			sql: "CREATE TABLE c (id int PRIMARY KEY, a int, b int);\n" +
				"ALTER TABLE c ADD UNIQUE KEY (a), ADD UNIQUE INDEX (a, b), " +
				"ADD CONSTRAINT UNIQUE (b), ADD CONSTRAINT UNIQUE KEY (b, a);",
			want: []string{"a(a)", "a_2(a,b)", "b(b)", "b_2(b,a)"},
		},
		{
			name: "indexes and a UNIQUE share the namespace",
			sql: "CREATE TABLE c (id int PRIMARY KEY, a int);\n" +
				"ALTER TABLE c ADD INDEX (a), ADD KEY (a), ADD UNIQUE (a);",
			want: []string{"a(a)", "a_2(a)", "a_3(a)"},
		},
		{
			name: "a name the same statement writes is claimed in order",
			sql: "CREATE TABLE c (id int PRIMARY KEY, a int, b int);\n" +
				"ALTER TABLE c ADD UNIQUE (a), ADD UNIQUE a_2 (b), ADD UNIQUE (a);",
			want: []string{"a(a)", "a_2(b)", "a_3(a)"},
		},
		{
			name: "one statement after another",
			sql: "CREATE TABLE c (id int PRIMARY KEY, a int, b int);\n" +
				"ALTER TABLE c ADD UNIQUE (a);\n" +
				"ALTER TABLE c ADD UNIQUE (a, b);",
			want: []string{"a(a)", "a_2(a,b)"},
		},
		{
			name: "PRIMARY is never derived",
			sql: "CREATE TABLE c (id int PRIMARY KEY, `PRIMARY` int);\n" +
				"ALTER TABLE c ADD UNIQUE (`PRIMARY`);",
			want: []string{"PRIMARY_2(PRIMARY)"},
		},
		{
			name: "a named key's index holds the key's name",
			sql: "CREATE TABLE c (id int PRIMARY KEY, a int, b int, CONSTRAINT b FOREIGN KEY (a) REFERENCES p(id));\n" +
				"ALTER TABLE c ADD UNIQUE (b);",
			want: []string{"b_2(b)"},
		},
		{
			name: "an unnamed key's index holds its column's name",
			sql: "CREATE TABLE c (id int PRIMARY KEY, a int, b int, FOREIGN KEY (a, b) REFERENCES p(x, y));\n" +
				"ALTER TABLE c ADD UNIQUE (a);",
			want: []string{"a_2(a)"},
		},
		{
			name: "an unnamed key's index lets go of its name for an index that covers the key",
			sql: "CREATE TABLE c (id int PRIMARY KEY, a int, FOREIGN KEY (a) REFERENCES p(id));\n" +
				"ALTER TABLE c ADD UNIQUE (a);",
			want: []string{"a(a)"},
		},
		{
			name: "a covering index with more columns",
			sql: "CREATE TABLE c (id int PRIMARY KEY, a int, b int, FOREIGN KEY (a) REFERENCES p(id));\n" +
				"ALTER TABLE c ADD UNIQUE (a, b);",
			want: []string{"a(a,b)"},
		},
		{
			name: "a named key's index lets go too",
			sql: "CREATE TABLE c (id int PRIMARY KEY, a int, CONSTRAINT fk FOREIGN KEY (a) REFERENCES p(id));\n" +
				"ALTER TABLE c ADD UNIQUE (a);",
			want: []string{"a(a)"},
		},
		{
			name: "a covering index added after another operation",
			sql: "CREATE TABLE c (id int PRIMARY KEY, a int, b int, FOREIGN KEY (a) REFERENCES p(id));\n" +
				"ALTER TABLE c ADD UNIQUE (b), ADD KEY (a);",
			want: []string{"a(a)", "b(b)"},
		},
		{
			name: "a key another index covers holds no name",
			sql: "CREATE TABLE c (id int PRIMARY KEY, a int, b int, KEY (a, b), FOREIGN KEY (b) REFERENCES p(id));\n" +
				"ALTER TABLE c ADD UNIQUE (b), ADD INDEX (a), ADD UNIQUE (b);",
			want: []string{"a(a,b)", "a_2(a)", "b(b)", "b_2(b)"},
		},
	}
	for _, dialect := range mysqlFamily {
		for _, test := range tests {
			t.Run(dialect+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)

				database, _, err := sqlschema.Read([]byte(alterIndexParents+test.sql), dialect)

				c.Assert(err, qt.IsNil)
				c.Assert(keysOf(database, "c"), qt.DeepEquals, test.want)
			})
		}
	}
}

// TestRead_AlterTableUnnamedIndexNames_DescendingKey is the row the engines
// answer differently. MySQL does not back a foreign key with a descending
// leading part, so its own index keeps `a`; MariaDB does, so the added index
// covers the key and takes the name the key's index lets go of.
func TestRead_AlterTableUnnamedIndexNames_DescendingKey(t *testing.T) {
	tests := []struct {
		dialect string
		want    []string
	}{
		{dialect: platform.MySQL, want: []string{"a_2(a)"}},
		{dialect: platform.MariaDB, want: []string{"a(a)"}},
	}
	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(alterIndexParents+
				"CREATE TABLE c (id int PRIMARY KEY, a int, FOREIGN KEY (a) REFERENCES p(id));\n"+
				"ALTER TABLE c ADD KEY (a DESC);"), test.dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(keysOf(database, "c"), qt.DeepEquals, test.want)
		})
	}
}

// TestReadOnto_AlterTableUnnamedIndexNames_CountsEarlierFiles names an index
// an ALTER TABLE in a later file adds against the indexes the earlier file
// declared, as the server does when it runs the files in order.
func TestReadOnto_AlterTableUnnamedIndexNames_CountsEarlierFiles(t *testing.T) {
	for _, dialect := range mysqlFamily {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			earlier, _, err := sqlschema.Read([]byte(
				"CREATE TABLE c (id int PRIMARY KEY, a int, b int, KEY a (b));"), dialect)
			c.Assert(err, qt.IsNil)

			later, _, err := sqlschema.ReadOnto([]byte("ALTER TABLE c ADD UNIQUE (a);"), dialect, &earlier)

			c.Assert(err, qt.IsNil)
			c.Assert(keysOf(later, "c"), qt.DeepEquals, []string{"a_2(a)"})
		})
	}
}

// TestRead_AlterTableUnnamedIndexNames_FailurePath refuses a derived name the
// engine would refuse, or that Ptah cannot compare, as the CREATE TABLE path
// does. MariaDB does not cut a long name before its number: measured on
// 11.8.9, a 63-character column whose name is taken yields a 65-character
// name and `ERROR 1280 (42000): Incorrect index name`.
func TestRead_AlterTableUnnamedIndexNames_FailurePath(t *testing.T) {
	long := strings.Repeat("c", 63)
	tests := []struct {
		name    string
		dialect string
		sql     string
		wantErr error
	}{
		{
			name:    "a name MariaDB will not take",
			dialect: platform.MariaDB,
			sql: "CREATE TABLE t (id int PRIMARY KEY, " + long + " int, b int, KEY " + long + " (b));\n" +
				"ALTER TABLE t ADD UNIQUE (" + long + ");",
			wantErr: sqlschema.ErrIndexNameTooLong,
		},
		{
			name:    "a name no rule compares",
			dialect: platform.MySQL,
			sql: "CREATE TABLE t (id int PRIMARY KEY, `ä` int);\n" +
				"ALTER TABLE t ADD UNIQUE (`ä`);",
			wantErr: sqlschema.ErrNonASCIIIndexName,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(test.sql), test.dialect)

			c.Assert(err, qt.ErrorIs, test.wantErr)
			c.Assert(database.Constraints, qt.HasLen, 0)
		})
	}
}

// TestRead_AlterTableUnnamedIndexNames_MySQLCutsALongName is the other half of
// the row above. MySQL cuts the column's name to 61 bytes before the number,
// measured on 9.7.2, so the same document is one it accepts.
func TestRead_AlterTableUnnamedIndexNames_MySQLCutsALongName(t *testing.T) {
	c := qt.New(t)
	long := strings.Repeat("c", 63)

	database, _, err := sqlschema.Read([]byte(
		"CREATE TABLE t (id int PRIMARY KEY, "+long+" int, b int, KEY "+long+" (b));\n"+
			"ALTER TABLE t ADD UNIQUE ("+long+");"), platform.MySQL)

	c.Assert(err, qt.IsNil)
	c.Assert(keysOf(database, "t"), qt.DeepEquals, []string{long[:61] + "_2(" + long + ")", long + "(b)"})
}
