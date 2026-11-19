package parser_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/internal/parser"
)

// parsedColumnCollation parses one CREATE TABLE for dialect and returns its
// first column's collation.
func parsedColumnCollation(c *qt.C, dialect, sql string) (string, error) {
	c.Helper()
	statements, err := parser.NewParser(sql, parser.WithDialect(dialect)).Parse()
	if err != nil {
		return "", err
	}
	createTable, ok := statements.Statements[0].(*ast.CreateTableNode)
	c.Assert(ok, qt.IsTrue)
	return createTable.Columns[0].Collate, nil
}

// TestParser_PostgreSQLColumnCollation_HappyPath pins the spelling a PostgreSQL
// column's COLLATE takes in the model: the name the server finds, with an
// unquoted name folded to lower case as PostgreSQL folds it, and a schema and
// a name each quoted.
func TestParser_PostgreSQLColumnCollation_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{name: "a quoted name", sql: `CREATE TABLE t (a text COLLATE "C");`, want: "C"},
		{name: "an unquoted name folds", sql: `CREATE TABLE t (a text COLLATE Unicode);`, want: "unicode"},
		{name: "a dot inside quotes", sql: `CREATE TABLE t (a text COLLATE "en_US.utf8");`, want: "en_US.utf8"},
		{name: "a schema and a mixed-case name", sql: `CREATE TABLE t (a text COLLATE App."MixedCase" NOT NULL);`,
			want: `"app"."MixedCase"`},
		{name: "after a constraint", sql: `CREATE TABLE t (a text NOT NULL COLLATE "POSIX");`, want: "POSIX"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := parsedColumnCollation(c, "postgres", test.sql)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

func TestParser_PostgreSQLColumnCollation_FailurePath(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{name: "a string constant", sql: `CREATE TABLE t (a text COLLATE 'C');`,
			want: `line 1, column 32: expected collation name: expected identifier, got String`},
		{name: "three parts", sql: `CREATE TABLE t (a text COLLATE a.b.c);`,
			want: `line 1, column 32: invalid collation name: "a.b.c" names 3 parts, and a collation is a name or a schema and a name`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := parsedColumnCollation(c, "postgres", test.sql)

			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(got, qt.Equals, "")
		})
	}
}

// TestParser_ColumnCollationOnAnotherDialect is the control: a dialect whose
// descriptor states neither Columns.Collations nor qualified collations keeps
// the collation as written.
func TestParser_ColumnCollationOnAnotherDialect(t *testing.T) {
	tests := []struct {
		dialect string
		sql     string
		want    string
	}{
		{dialect: "cockroachdb", sql: `CREATE TABLE t (a STRING COLLATE "en_US");`, want: `"en_US"`},
		{dialect: "yugabytedb", sql: `CREATE TABLE t (a text COLLATE "C");`, want: `"C"`},
	}
	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)

			got, err := parsedColumnCollation(c, test.dialect, test.sql)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// TestParser_MySQLColumnCollation_HappyPath pins the MySQL family's spelling:
// the name unquoted from a string or a backquoted identifier, both of which
// MySQL 8.4 and MariaDB 11.8 take after COLLATE, in the case it is written.
func TestParser_MySQLColumnCollation_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{name: "a bare name keeps its case", sql: `CREATE TABLE t (a varchar(10) COLLATE UTF8MB4_bin);`, want: "UTF8MB4_bin"},
		{name: "a single-quoted name", sql: `CREATE TABLE t (a varchar(10) COLLATE 'utf8mb4_bin');`, want: "utf8mb4_bin"},
		{name: "a double-quoted name", sql: `CREATE TABLE t (a varchar(10) COLLATE "utf8mb4_bin");`, want: "utf8mb4_bin"},
		{name: "a backquoted name", sql: "CREATE TABLE t (a varchar(10) COLLATE `utf8mb4_bin`);", want: "utf8mb4_bin"},
		{name: "after a constraint", sql: `CREATE TABLE t (a varchar(10) NOT NULL COLLATE utf8mb4_bin);`, want: "utf8mb4_bin"},
	}
	for _, dialect := range []string{"mysql", "mariadb"} {
		for _, test := range tests {
			t.Run(dialect+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)

				got, err := parsedColumnCollation(c, dialect, test.sql)

				c.Assert(err, qt.IsNil)
				c.Assert(got, qt.Equals, test.want)
			})
		}
	}
}

// TestParser_SQLiteColumnCollation_HappyPath pins SQLite's spelling: the name
// unquoted from any of the quotes sqlite3 takes there, in the case it is
// written, as the reader reports a column's collation.
func TestParser_SQLiteColumnCollation_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{name: "a bare name keeps its case", sql: `CREATE TABLE t (a TEXT COLLATE NoCase);`, want: "NoCase"},
		{name: "a double-quoted name", sql: `CREATE TABLE t (a TEXT COLLATE "NoCase");`, want: "NoCase"},
		{name: "a single-quoted name", sql: `CREATE TABLE t (a TEXT COLLATE 'rtrim');`, want: "rtrim"},
		{name: "a backquoted name", sql: "CREATE TABLE t (a TEXT COLLATE `nocase`);", want: "nocase"},
		{name: "a bracketed name", sql: `CREATE TABLE t (a TEXT COLLATE [binary]);`, want: "binary"},
		{name: "after a constraint", sql: `CREATE TABLE t (a TEXT NOT NULL COLLATE RTRIM);`, want: "RTRIM"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := parsedColumnCollation(c, "sqlite", test.sql)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

func TestParser_SQLiteColumnCollation_FailurePath(t *testing.T) {
	c := qt.New(t)

	got, err := parsedColumnCollation(c, "sqlite", `CREATE TABLE t (a TEXT COLLATE 1);`)

	c.Assert(err, qt.ErrorMatches, `line 1, column 32: expected collation name: "1" is not a name`)
	c.Assert(got, qt.Equals, "")
}
