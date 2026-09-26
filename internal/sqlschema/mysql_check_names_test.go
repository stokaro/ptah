package sqlschema_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/internal/sqlschema"
)

// TestRead_MySQLUnnamedCheckNames_HappyPath gives an unnamed CHECK the name
// MySQL gives it, so a schema file compares equal to the database its own SQL
// built (stokaro/ptah#3741). Each expected name was read back from
// information_schema on MySQL 8.4.11 and 26.7.0 after running the row's SQL.
func TestRead_MySQLUnnamedCheckNames_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []string
	}{
		{
			name: "on a column and on the table, in the order written",
			sql:  "CREATE TABLE c (id int PRIMARY KEY, a int CHECK (a > 0), b int, CHECK (b > 0), CHECK (a < b));",
			want: []string{"c c_chk_1: a > 0", "c c_chk_2: b > 0", "c c_chk_3: a < b"},
		},
		{
			name: "a table-level CHECK written before the columns",
			sql:  "CREATE TABLE c2 (CHECK (b > 0), a int CHECK (a > 0), b int);",
			want: []string{"c2 c2_chk_1: b > 0", "c2 c2_chk_2: a > 0"},
		},
		{
			name: "a table-level CHECK between two columns",
			sql:  "CREATE TABLE c6 (a int, CHECK (a > 0), b int CHECK (b > 0));",
			want: []string{"c6 c6_chk_1: a > 0", "c6 c6_chk_2: b > 0"},
		},
		{
			name: "a named CHECK does not move the count",
			sql:  "CREATE TABLE c3 (a int, CONSTRAINT c3_chk_5 CHECK (a > 0), CHECK (a < 9));",
			want: []string{"c3 c3_chk_1: a < 9", "c3 c3_chk_5: a > 0"},
		},
		{
			name: "two on one column",
			sql:  "CREATE TABLE c4 (a int CHECK (a > 0) CHECK (a < 10));",
			want: []string{"c4 c4_chk_1: a > 0", "c4 c4_chk_2: a < 10"},
		},
		{
			name: "CONSTRAINT without a symbol",
			sql:  "CREATE TABLE f4 (a int, CONSTRAINT CHECK (a > 0), b int CHECK (b > 0), CONSTRAINT CHECK (b < 9));",
			want: []string{"f4 f4_chk_1: a > 0", "f4 f4_chk_2: b > 0", "f4 f4_chk_3: b < 9"},
		},
		{
			name: "the table keeps its case",
			sql:  "CREATE TABLE MixedCase (a int CHECK (a > 0));",
			want: []string{"MixedCase MixedCase_chk_1: a > 0"},
		},
		{
			name: "a name of 64 characters",
			sql:  "CREATE TABLE t_fifty_eight_characters_long_table_name_for_measuring_lim (a int CHECK (a > 0));",
			want: []string{
				"t_fifty_eight_characters_long_table_name_for_measuring_lim " +
					"t_fifty_eight_characters_long_table_name_for_measuring_lim_chk_1: a > 0",
			},
		},
		{
			name: "ALTER TABLE takes one more than the highest",
			sql: "CREATE TABLE al (a int, b int);\n" +
				"ALTER TABLE al ADD CHECK (a > 0);\n" +
				"ALTER TABLE al ADD CONSTRAINT al_chk_5 CHECK (a > 1);\n" +
				"ALTER TABLE al ADD CHECK (a > 2);\n" +
				"ALTER TABLE al ADD COLUMN x int CHECK (x > 0);\n" +
				"ALTER TABLE al DROP CONSTRAINT al_chk_6;\n" +
				"ALTER TABLE al ADD CHECK (a > 3);",
			want: []string{"al al_chk_1: a > 0", "al al_chk_5: a > 1", "al al_chk_7: x > 0", "al al_chk_8: a > 3"},
		},
		{
			name: "one ALTER TABLE numbers its CHECKs in the order written",
			sql: "CREATE TABLE al7 (a int, b int);\n" +
				"ALTER TABLE al7 ADD CHECK (a > 1), ADD CHECK (b > 1), ADD COLUMN c int CHECK (c > 0);\n" +
				"CREATE TABLE b4 (a int, b int);\n" +
				"ALTER TABLE b4 ADD COLUMN c int CHECK (c > 0), ADD CHECK (a > 1);",
			want: []string{
				"al7 al7_chk_1: a > 1", "al7 al7_chk_2: b > 1", "al7 al7_chk_3: c > 0",
				"b4 b4_chk_1: c > 0", "b4 b4_chk_2: a > 1",
			},
		},
		{
			name: "ALTER TABLE does not count a CHECK the same statement names",
			sql: "CREATE TABLE s1 (a int);\n" +
				"ALTER TABLE s1 ADD CONSTRAINT s1_chk_5 CHECK (a > 1), ADD CHECK (a > 2);",
			want: []string{"s1 s1_chk_1: a > 2", "s1 s1_chk_5: a > 1"},
		},
		{
			name: "ALTER TABLE does not count a CHECK the same statement drops",
			sql: "CREATE TABLE s2 (a int, CONSTRAINT s2_chk_3 CHECK (a > 1));\n" +
				"ALTER TABLE s2 DROP CONSTRAINT s2_chk_3, ADD CHECK (a > 2);",
			want: []string{"s2 s2_chk_1: a > 2"},
		},
		{
			name: "ALTER TABLE drops before it names, whichever it writes first",
			sql: "CREATE TABLE s5 (a int, CONSTRAINT s5_chk_3 CHECK (a > 1));\n" +
				"ALTER TABLE s5 ADD CHECK (a > 2), DROP CONSTRAINT s5_chk_3;",
			want: []string{"s5 s5_chk_1: a > 2"},
		},
		{
			name: "ALTER TABLE reads a leading zero",
			sql: "CREATE TABLE al3 (a int, CONSTRAINT al3_chk_07 CHECK (a > 1));\n" +
				"ALTER TABLE al3 ADD CHECK (a > 3);",
			want: []string{"al3 al3_chk_07: a > 1", "al3 al3_chk_8: a > 3"},
		},
		{
			name: "ALTER TABLE does not read the table in another case",
			sql: "CREATE TABLE MixedAlt (a int, CONSTRAINT mixedalt_chk_3 CHECK (a > 1));\n" +
				"ALTER TABLE MixedAlt ADD CHECK (a > 3);",
			want: []string{"MixedAlt MixedAlt_chk_1: a > 3", "MixedAlt mixedalt_chk_3: a > 1"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(test.sql), "mysql")

			c.Assert(err, qt.IsNil)
			c.Assert(checkNames(database), qt.DeepEquals, sorted(test.want))
		})
	}
}

// TestRead_MySQLUnnamedCheckNames_FailurePath refuses a document MySQL
// refuses, because the name it would give an unnamed CHECK is taken or too
// long. Each row's SQL is refused by MySQL 8.4.11 and 26.7.0 with the error the
// message names.
func TestRead_MySQLUnnamedCheckNames_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantIs  error
		wantErr string
	}{
		{
			name:    "a named CHECK written after the unnamed one",
			sql:     "CREATE TABLE f2 (a int, CHECK (a > 0), CONSTRAINT f2_chk_1 CHECK (a < 9));",
			wantIs:  sqlschema.ErrDuplicateCheckName,
			wantErr: `(?s).*f2_chk_1, the name MySQL gives an unnamed CHECK of f2, is held by another CHECK, and MySQL answers ERROR 3822.*`,
		},
		{
			name:    "a named CHECK in another case",
			sql:     "CREATE TABLE f3 (a int, CONSTRAINT F3_CHK_1 CHECK (a > 0), CHECK (a < 9));",
			wantIs:  sqlschema.ErrDuplicateCheckName,
			wantErr: `(?s).*F3_CHK_1, the name MySQL gives an unnamed CHECK of f3, is held by another CHECK.*`,
		},
		{
			name:    "a CHECK of another table",
			sql:     "CREATE TABLE f6 (a int, CONSTRAINT f7_CHK_1 CHECK (a > 0));\nCREATE TABLE f7 (a int CHECK (a > 0));",
			wantIs:  sqlschema.ErrDuplicateCheckName,
			wantErr: `(?s).*f7_CHK_1, the name MySQL gives an unnamed CHECK of f7, is held by another CHECK.*`,
		},
		{
			name:    "a named CHECK the count reaches",
			sql:     "CREATE TABLE b5 (a int, CONSTRAINT b5_chk_2 CHECK (a > 1), CHECK (a > 2), CHECK (a > 3));",
			wantIs:  sqlschema.ErrDuplicateCheckName,
			wantErr: `(?s).*b5_chk_2, the name MySQL gives an unnamed CHECK of b5, is held by another CHECK.*`,
		},
		{
			name:    "a name longer than 64 characters",
			sql:     "CREATE TABLE t_fifty_nine_characters_long_table_name_for_measuring_limit (a int CHECK (a > 0));",
			wantIs:  sqlschema.ErrCheckNameTooLong,
			wantErr: `(?s).*t_fifty_nine_characters_long_table_name_for_measuring_limit_chk_1, the name MySQL gives an unnamed CHECK of .*, is longer than 64 characters, and MySQL answers ERROR 1059.*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(test.sql), "mysql")

			c.Assert(err, qt.ErrorIs, test.wantIs)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(database.Tables, qt.HasLen, 0)
		})
	}
}

// TestRead_MariaDBUnnamedCheckNames_HappyPath gives an unnamed CHECK the name
// MariaDB gives it: a CHECK on a column is named after the column, and one on
// the table `CONSTRAINT_<n>`. Each expected name was read back from
// information_schema.CHECK_CONSTRAINTS on MariaDB 11.8.9 after running the
// row's SQL.
func TestRead_MariaDBUnnamedCheckNames_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []string
	}{
		{
			name: "on a column and on the table",
			sql:  "CREATE TABLE c (id int PRIMARY KEY, a int CHECK (a > 0), b int, CHECK (b > 0), CHECK (a < b));",
			want: []string{"c a: a > 0", "c CONSTRAINT_1: b > 0", "c CONSTRAINT_2: a < b"},
		},
		{
			name: "a named CHECK written after the unnamed one takes its number",
			sql:  "CREATE TABLE e2 (a int, CHECK (a < 9), CONSTRAINT CONSTRAINT_1 CHECK (a > 0));",
			want: []string{"e2 CONSTRAINT_2: a < 9", "e2 CONSTRAINT_1: a > 0"},
		},
		{
			name: "a named CHECK in another case",
			sql:  "CREATE TABLE e3 (a int, CONSTRAINT constraint_1 CHECK (a > 0), CHECK (a < 9));",
			want: []string{"e3 constraint_1: a > 0", "e3 CONSTRAINT_2: a < 9"},
		},
		{
			name: "the first free number",
			sql:  "CREATE TABLE e4 (a int, CHECK (a < 9), CONSTRAINT CONSTRAINT_2 CHECK (a > 0), CHECK (a < 8));",
			want: []string{"e4 CONSTRAINT_1: a < 9", "e4 CONSTRAINT_2: a > 0", "e4 CONSTRAINT_3: a < 8"},
		},
		{
			name: "a column's CHECK takes its column's name",
			sql:  "CREATE TABLE g1 (CONSTRAINT_1 int CHECK (CONSTRAINT_1 > 0), CHECK (CONSTRAINT_1 < 9));",
			want: []string{"g1 CONSTRAINT_1: CONSTRAINT_1 > 0", "g1 CONSTRAINT_2: CONSTRAINT_1 < 9"},
		},
		{
			name: "the column keeps its case",
			sql:  "CREATE TABLE e9 (`Mixed` int CHECK (`Mixed` > 0));",
			want: []string{"e9 Mixed: `Mixed` > 0"},
		},
		{
			name: "CONSTRAINT without a symbol",
			sql:  "CREATE TABLE c5 (a int, CONSTRAINT CHECK (a > 0), CHECK (a < 9));",
			want: []string{"c5 CONSTRAINT_1: a > 0", "c5 CONSTRAINT_2: a < 9"},
		},
		{
			name: "ALTER TABLE takes the first free number",
			sql: "CREATE TABLE al (a int, b int);\n" +
				"ALTER TABLE al ADD CHECK (a > 0);\n" +
				"ALTER TABLE al ADD CONSTRAINT CONSTRAINT_5 CHECK (a > 1);\n" +
				"ALTER TABLE al ADD CHECK (a > 2);\n" +
				"ALTER TABLE al ADD COLUMN x int CHECK (x > 0);\n" +
				"ALTER TABLE al ADD CHECK (a > 3), ADD CHECK (a > 4);",
			want: []string{
				"al x: x > 0", "al CONSTRAINT_1: a > 0", "al CONSTRAINT_5: a > 1", "al CONSTRAINT_2: a > 2",
				"al CONSTRAINT_3: a > 3", "al CONSTRAINT_4: a > 4",
			},
		},
		{
			name: "ALTER TABLE avoids a name the same statement writes later",
			sql: "CREATE TABLE s1 (a int);\n" +
				"ALTER TABLE s1 ADD CHECK (a > 2), ADD CONSTRAINT CONSTRAINT_1 CHECK (a > 1);",
			want: []string{"s1 CONSTRAINT_1: a > 1", "s1 CONSTRAINT_2: a > 2"},
		},
		{
			name: "ALTER TABLE reuses the number a CHECK the same statement drops held",
			sql: "CREATE TABLE s2 (a int, CHECK (a > 1));\n" +
				"ALTER TABLE s2 DROP CONSTRAINT CONSTRAINT_1, ADD CHECK (a > 2);",
			want: []string{"s2 CONSTRAINT_1: a > 2"},
		},
		{
			name: "ALTER TABLE reuses a number a dropped CHECK freed",
			sql: "CREATE TABLE e6 (a int, CONSTRAINT x CHECK (a > 0), CHECK (a < 9));\n" +
				"ALTER TABLE e6 DROP CONSTRAINT CONSTRAINT_1;\n" +
				"ALTER TABLE e6 ADD CHECK (a < 8);",
			want: []string{"e6 x: a > 0", "e6 CONSTRAINT_1: a < 8"},
		},
		{
			name: "a column restated with IF NOT EXISTS",
			sql: "CREATE TABLE g2 (a int CHECK (a > 0));\n" +
				"ALTER TABLE g2 ADD COLUMN IF NOT EXISTS a int CHECK (a > 0);",
			want: []string{"g2 a: a > 0"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(test.sql), "mariadb")

			c.Assert(err, qt.IsNil)
			c.Assert(checkNames(database), qt.DeepEquals, sorted(test.want))
		})
	}
}

// TestRead_MySQLFamilyCheckNameDroppedLater_FailurePath refuses an unnamed
// CHECK whose derived name the same ALTER TABLE drops after adding it. The
// server drops first and keeps the CHECK under that name -- measured, `s5_chk_1`
// on MySQL 8.4.11 and 26.7.0 and `CONSTRAINT_1` on MariaDB 11.8.9 -- while the
// reader applies the operations in the order written, where the drop would take
// the CHECK just added.
func TestRead_MySQLFamilyCheckNameDroppedLater_FailurePath(t *testing.T) {
	tests := []struct {
		dialect string
		sql     string
		wantErr string
	}{
		{
			dialect: "mysql",
			sql:     "CREATE TABLE s5 (a int, CONSTRAINT s5_chk_1 CHECK (a > 1));\nALTER TABLE s5 ADD CHECK (a > 2), DROP CONSTRAINT s5_chk_1;",
			wantErr: `(?s).*ALTER TABLE s5 adds an unnamed CHECK the server names s5_chk_1, and drops s5_chk_1 later in the same statement.*`,
		},
		{
			dialect: "mariadb",
			sql:     "CREATE TABLE s5 (a int, CHECK (a > 1));\nALTER TABLE s5 ADD CHECK (a > 2), DROP CONSTRAINT CONSTRAINT_1;",
			wantErr: `(?s).*ALTER TABLE s5 adds an unnamed CHECK the server names CONSTRAINT_1, and drops CONSTRAINT_1 later in the same statement.*`,
		},
	}
	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(test.sql), test.dialect)

			c.Assert(err, qt.ErrorIs, sqlschema.ErrUnmodeledStatement)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(database.Tables, qt.HasLen, 0)
		})
	}
}

// TestRead_MariaDBUnnamedCheckNames_FailurePath refuses a table-level CHECK
// named like a column that carries a CHECK. MariaDB 11.8.9 names the column's
// CHECK after the column and answers ERROR 1826 to the second.
func TestRead_MariaDBUnnamedCheckNames_FailurePath(t *testing.T) {
	c := qt.New(t)

	database, _, err := sqlschema.Read([]byte("CREATE TABLE e8 (a int CHECK (a > 0), CONSTRAINT a CHECK (a < 9));"), "mariadb")

	c.Assert(err, qt.ErrorIs, sqlschema.ErrDuplicateCheckName)
	c.Assert(err, qt.ErrorMatches, `(?s).*a names a CHECK of e8, and so does the CHECK on column a, which MariaDB names after the column; MariaDB answers ERROR 1826.*`)
	c.Assert(database.Tables, qt.HasLen, 0)
}

// sorted returns the names in the order checkNames lists them.
func sorted(names []string) []string {
	return slices.Sorted(slices.Values(names))
}

// TestToDatabase_MySQLChecksOfATableThatRecordedNoColumnOrder names the CHECKs
// of a node that says where its constraints sit and not where its columns do:
// one whose Columns were assigned rather than added. The CHECKs on columns are
// numbered first, which is the order a table written column by column declares
// them in. The same two CHECKs parsed from `CREATE TABLE c2 (CHECK (b > 0), a int
// CHECK (a > 0), b int)` are numbered the other way round, which the reader
// rows measure.
func TestToDatabase_MySQLChecksOfATableThatRecordedNoColumnOrder(t *testing.T) {
	c := qt.New(t)
	table := ast.NewCreateTable("c2")
	table.AddConstraint(&ast.ConstraintNode{Type: ast.CheckConstraint, Expression: "b > 0"})
	table.Columns = []*ast.ColumnNode{ast.NewColumn("a", "int").SetCheck("a > 0"), ast.NewColumn("b", "int")}

	database, err := sqlschema.ToDatabase(&ast.StatementList{Statements: []ast.Node{table}}, platform.MySQL)

	c.Assert(err, qt.IsNil)
	c.Assert(checkNames(database), qt.DeepEquals, []string{"c2 c2_chk_1: a > 0", "c2 c2_chk_2: b > 0"})
}
