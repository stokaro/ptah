package parser_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/engine/builtin"
	"ptah.run/internal/parser"
	"ptah.run/internal/sqlschema"
)

// createTable parses sql under dialect and answers its one CREATE TABLE.
func createTable(c *qt.C, dialect, sql string) *ast.CreateTableNode {
	c.Helper()
	statements := parsed(c, dialect, sql)
	c.Assert(statements, qt.HasLen, 1)
	table, ok := statements[0].(*ast.CreateTableNode)
	c.Assert(ok, qt.IsTrue)
	return table
}

// TestParser_ReadsAColumnCommentAsItsText reads a column's COMMENT clause as
// the text the server stores. Measured on MySQL 8.4.11, a column written
// `x int COMMENT 'hi'` holds `hi`, a doubled quote and a backslash escape are
// read as the server reads them, and of two COMMENT clauses the last is kept.
// Read with its keyword and quotes, the comment reached the database as
// `COMMENT 'hi'` (stokaro/ptah#3874).
func TestParser_ReadsAColumnCommentAsItsText(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		column  string
		want    string
	}{
		{name: "mysql", dialect: platform.MySQL, column: "x int COMMENT 'hi'", want: "hi"},
		{name: "mariadb", dialect: platform.MariaDB, column: "x int COMMENT 'hi'", want: "hi"},
		{name: "a doubled quote", dialect: platform.MySQL, column: "x int COMMENT 'it''s'", want: "it's"},
		{name: "a backslash escape", dialect: platform.MySQL, column: `x int COMMENT 'a\'b\sc'`, want: "a'bsc"},
		{name: "the last of two clauses", dialect: platform.MySQL, column: "x int COMMENT 'first' COMMENT 'second'", want: "second"},
		{name: "after ON UPDATE", dialect: platform.MariaDB, column: "x timestamp ON UPDATE CURRENT_TIMESTAMP COMMENT 'hi'", want: "hi"},
		{name: "clickhouse", dialect: platform.ClickHouse, column: "x Int32 COMMENT 'hi'", want: "hi"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			table := createTable(c, test.dialect, "CREATE TABLE c ("+test.column+");")

			c.Assert(table.Columns, qt.HasLen, 1)
			c.Assert(table.Columns[0].Comment, qt.Equals, test.want)
		})
	}
}

// TestParser_ReadsATableCommentAsItsText reads a table's COMMENT option as
// its text, with or without the `=` MySQL, MariaDB and ClickHouse all take.
// Read with its quotes, a MySQL table comment reached the database as `'tbl'`,
// and `COMMENT 'tbl'` was refused (stokaro/ptah#3874).
func TestParser_ReadsATableCommentAsItsText(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		sql     string
	}{
		{name: "mysql with an equals sign", dialect: platform.MySQL, sql: "CREATE TABLE c (id int) COMMENT='tbl';"},
		{name: "mysql without one", dialect: platform.MySQL, sql: "CREATE TABLE c (id int) COMMENT 'tbl';"},
		{name: "mariadb beside other options", dialect: platform.MariaDB, sql: "CREATE TABLE c (id int) ENGINE=InnoDB COMMENT = 'tbl' CHARSET=utf8mb4;"},
		{name: "clickhouse", dialect: platform.ClickHouse, sql: "CREATE TABLE c (id Int32) ENGINE = MergeTree ORDER BY id COMMENT 'tbl';"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			table := createTable(c, test.dialect, test.sql)

			c.Assert(table.Comment, qt.Equals, "tbl")
		})
	}
}

// TestParser_ReadsACommentThatIsNotAString_FailurePath refuses a COMMENT whose
// value is not a string constant, as the server does.
func TestParser_ReadsACommentThatIsNotAString_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr string
	}{
		{name: "a column", sql: "CREATE TABLE c (x int COMMENT 42);", wantErr: `.*expected a string constant for column comment.*`},
		{name: "a table", sql: "CREATE TABLE c (x int) COMMENT = 42;", wantErr: `.*expected a string constant for table comment.*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parser.NewParser(test.sql, parser.WithDialect(platform.MySQL)).Parse()

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(statements, qt.IsNil)
		})
	}
}

// TestSQLSchemaWritesAMySQLCommentOnce reads a MySQL table and renders it
// again, which is the path `migrate diff` and `schema apply` take from a
// schema file to the statement a server receives: each comment is written
// once, as the text it holds.
func TestSQLSchemaWritesAMySQLCommentOnce(t *testing.T) {
	for _, dialect := range []string{platform.MySQL, platform.MariaDB} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			database, _, err := sqlschema.Read([]byte("CREATE TABLE c (id int PRIMARY KEY, x int COMMENT 'it''s') COMMENT='tbl';"), dialect)
			c.Assert(err, qt.IsNil)

			rendered, err := builtin.GetOrderedCreateStatements(&database, dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(rendered, qt.HasLen, 1)
			c.Assert(rendered[0], qt.Contains, "`x` int COMMENT 'it''s'")
			c.Assert(rendered[0], qt.Contains, "COMMENT='tbl'")
			c.Assert(rendered[0], qt.Not(qt.Contains), "COMMENT 'COMMENT")
		})
	}
}
