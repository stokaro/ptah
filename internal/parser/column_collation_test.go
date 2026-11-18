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
// descriptor does not state Columns.Collations keeps the collation as written.
func TestParser_ColumnCollationOnAnotherDialect(t *testing.T) {
	tests := []struct {
		dialect string
		sql     string
		want    string
	}{
		{dialect: "cockroachdb", sql: `CREATE TABLE t (a STRING COLLATE "en_US");`, want: `"en_US"`},
		{dialect: "sqlite", sql: `CREATE TABLE t (a TEXT COLLATE NOCASE);`, want: "NOCASE"},
		{dialect: "mysql", sql: `CREATE TABLE t (a varchar(10) COLLATE utf8mb4_bin);`, want: "utf8mb4_bin"},
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
