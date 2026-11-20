package sqlutil_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/sqlutil"
)

// A comment or default whose text holds a line that reads like a delimiter
// directive reaches the server whole. Each row is one statement followed by a
// second, so a directive mistaken for text-level syntax shows twice: the
// literal loses a line, and the semicolon stops ending the first statement.
func TestSplitStatementsForDialect_DirectiveLookingLiteral_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		first   string
	}{
		{
			name:    "postgres table comment with the Atlas form",
			dialect: "postgres",
			first:   "COMMENT ON TABLE t IS 'c\n-- atlas:delimiter $$\nd'",
		},
		{
			name:    "postgres default with the client form",
			dialect: "postgres",
			first:   "ALTER TABLE t ALTER COLUMN n SET DEFAULT 'a\nDELIMITER $$\nb'",
		},
		{
			name:    "postgres E string with a backslash before the quote",
			dialect: "postgres",
			first:   "COMMENT ON TABLE t IS E'c\\'\nDELIMITER $$\nd'",
		},
		{
			name:    "mysql inline comment with the Atlas form",
			dialect: "mysql",
			first:   "CREATE TABLE t (n TEXT COMMENT 'x\n-- atlas:delimiter $$\ny')",
		},
		{
			name:    "mysql backslash-escaped quote",
			dialect: "mysql",
			first:   "CREATE TABLE t (n TEXT COMMENT 'x\\'\nDELIMITER $$\ny')",
		},
		{
			name:    "sqlite default with the Atlas form",
			dialect: "sqlite",
			first:   "CREATE TABLE t (n TEXT DEFAULT 'x\n-- atlas:delimiter $$\ny')",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := sqlutil.SplitStatementsForDialect(test.dialect, test.first+";\nSELECT 1;\n")

			c.Assert(got, qt.DeepEquals, []string{test.first, "SELECT 1"})
		})
	}
}

func TestSplitStatements_DirectiveLookingLiteral_HappyPath(t *testing.T) {
	c := qt.New(t)
	first := "COMMENT ON TABLE t IS 'c\n-- atlas:delimiter $$\nd'"

	got := sqlutil.SplitStatements(first + ";\nSELECT 1;\n")

	c.Assert(got, qt.DeepEquals, []string{first, "SELECT 1"})
}

// The control: the same two directives written outside a literal still select
// the delimiter.
func TestSplitStatementsForDialect_DirectiveOutsideALiteral_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		script string
	}{
		{name: "Atlas form", script: "SELECT 'x';\n-- atlas:delimiter $$\nSELECT 1; SELECT 2$$\n"},
		{name: "client form", script: "SELECT 'x';\nDELIMITER $$\nSELECT 1; SELECT 2$$\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := sqlutil.SplitStatementsForDialect("mysql", test.script)

			c.Assert(got, qt.DeepEquals, []string{"SELECT 'x'", "SELECT 1", "SELECT 2"})
		})
	}
}

// A dollar sign is an ordinary character in a MySQL-family name, so `a$b$` opens
// directive between two such names is still honored.
func TestSplitStatementsForDialect_DollarInANameBesideADirective_HappyPath(t *testing.T) {
	script := "SELECT a$b$ FROM t;\nDELIMITER $$\nSELECT 1; SELECT 2$$\nDELIMITER ;\nSELECT c$b$ FROM t;\n"
	tests := []struct {
		name    string
		dialect string
		want    []string
	}{
		{name: "mysql", dialect: "mysql", want: []string{"SELECT a$b$ FROM t", "SELECT 1", "SELECT 2", "SELECT c$b$ FROM t"}},
		{name: "mariadb", dialect: "mariadb", want: []string{"SELECT a$b$ FROM t", "SELECT 1", "SELECT 2", "SELECT c$b$ FROM t"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := sqlutil.SplitStatementsForDialect(test.dialect, script)

			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// Each family reads a script by the rules its descriptor states, and a mistake
// in one family's rule hides or invents a directive: a line the family reads as
// text must stay in its statement, and a line it reads as a directive must act.
// The ClickHouse `//` row ends in an open literal because the statement lexer
// reads `//` as two operators; what it pins is that the directive was honored.
// Every row holds a directive that only the row's dialects read as text, or,
// for the MySQL rows, as a directive.
func TestSplitStatementsForDialect_FamilyRules_HappyPath(t *testing.T) {
	dollar := "SELECT $q$\nDELIMITER $$\n$q$;\nSELECT 2;\n"
	hash := "# it's\nDELIMITER $$\nSELECT 1; SELECT 2$$\n"
	bracket := "SELECT [a'b] FROM t;\nDELIMITER $$\nSELECT 1; SELECT 2$$\n"
	arrayIndex := "SELECT x[']'];\nDELIMITER $$\nSELECT 1; SELECT 2$$\n"
	mysqlDollar := "SELECT $x$ 1;\nDELIMITER //\nSELECT 2; SELECT 3//\nSELECT $x$ 4;\n"
	clickhouseSlash := "SELECT 1; // it's\nDELIMITER $$\nSELECT 2; SELECT 3$$\n"
	heredoc := "SELECT $$it's$$;\nDELIMITER //\nSELECT 1; SELECT 2//\nDELIMITER ;\nSELECT 3;\n"
	tests := []struct {
		name    string
		dialect string
		script  string
		want    []string
	}{
		{name: "postgres dollar quote", dialect: "postgres", script: dollar, want: []string{"SELECT $q$\nDELIMITER $$\n$q$", "SELECT 2"}},
		{name: "cockroachdb dollar quote", dialect: "cockroachdb", script: dollar, want: []string{"SELECT $q$\nDELIMITER $$\n$q$", "SELECT 2"}},
		{name: "yugabytedb dollar quote", dialect: "yugabytedb", script: dollar, want: []string{"SELECT $q$\nDELIMITER $$\n$q$", "SELECT 2"}},
		{name: "mysql hash comment", dialect: "mysql", script: hash, want: []string{"SELECT 1", "SELECT 2"}},
		{name: "mariadb hash comment", dialect: "mariadb", script: hash, want: []string{"SELECT 1", "SELECT 2"}},
		{name: "clickhouse hash comment", dialect: "clickhouse", script: hash, want: []string{"SELECT 1", "SELECT 2"}},
		{name: "sqlserver bracketed name", dialect: "sqlserver", script: bracket, want: []string{"SELECT [a'b] FROM t", "SELECT 1", "SELECT 2"}},
		{name: "sqlite bracketed name", dialect: "sqlite", script: bracket, want: []string{"SELECT [a'b] FROM t", "SELECT 1", "SELECT 2"}},
		{name: "postgres square brackets are not a name", dialect: "postgres", script: arrayIndex, want: []string{"SELECT x[']']", "SELECT 1", "SELECT 2"}},
		{name: "mysql reads no dollar quote", dialect: "mysql", script: mysqlDollar, want: []string{"SELECT $x$ 1;\nSELECT 2; SELECT 3;\nSELECT $x$ 4"}},
		{name: "mariadb reads no dollar quote", dialect: "mariadb", script: mysqlDollar, want: []string{"SELECT $x$ 1;\nSELECT 2; SELECT 3;\nSELECT $x$ 4"}},
		{name: "clickhouse slash comment", dialect: "clickhouse", script: clickhouseSlash, want: []string{"SELECT 1", "// it's\nSELECT 2; SELECT 3;"}},
		{name: "clickhouse heredoc", dialect: "clickhouse", script: heredoc, want: []string{"SELECT $$it's$$", "SELECT 1", "SELECT 2", "SELECT 3"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := sqlutil.SplitStatementsForDialect(test.dialect, test.script)

			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}
