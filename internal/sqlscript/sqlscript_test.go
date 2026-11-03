package sqlscript_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/sqlutil"
	"ptah.run/internal/sqlscript"
)

func TestTerminator(t *testing.T) {
	tests := []struct {
		name      string
		statement string
		want      string
		// lineComment is what EndsInLineComment answers.
		lineComment bool
	}{
		{
			name:      "an executable statement",
			statement: `DROP POLICY IF EXISTS "tenant_only" ON "site_media_settings"`,
			want:      ";",
		},
		{
			name:      "a statement led by its comment",
			statement: "-- Drop RLS policy tenant_only\nDROP POLICY IF EXISTS \"tenant_only\" ON \"site_media_settings\"",
			want:      ";",
		},
		{
			name:        "a line comment alone",
			statement:   "-- NOTE: RLS policies were removed from table site_media_settings - verify if RLS should be disabled",
			want:        "",
			lineComment: true,
		},
		{
			name:      "several comments and blank lines",
			statement: "-- WARNING: first\n\n-- second\n/* block */",
			want:      "",
		},
		{
			name:      "a comment marker inside a string literal",
			statement: "SELECT '-- not a comment'",
			want:      ";",
		},
		{
			name:      "an empty statement",
			statement: "",
			want:      "",
		},
		{
			name:        "a statement that ends in a line comment",
			statement:   "CREATE MATERIALIZED VIEW \"order_lc\" AS\nSELECT count(*) AS n FROM orders WITH NO DATA -- empty too",
			want:        "\n;",
			lineComment: true,
		},
		{
			name:        "a statement that ends in a hash comment",
			statement:   "CREATE VIEW v AS SELECT 1 # one",
			want:        "\n;",
			lineComment: true,
		},
		{
			name:      "a statement whose line comment ends its line",
			statement: "CREATE VIEW v AS SELECT 1 -- one\n",
			want:      ";",
		},
		{
			name:      "a statement with a line comment before its last line",
			statement: "CREATE VIEW v AS -- one\nSELECT 1",
			want:      ";",
		},
		{
			name:      "a statement that ends in a block comment",
			statement: "CREATE VIEW v AS SELECT 1 /* one */",
			want:      ";",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(sqlscript.Terminator(test.statement), qt.Equals, test.want)
			c.Assert(sqlscript.CommentOnly(test.statement), qt.Equals, test.want == "")
			c.Assert(sqlscript.EndsInLineComment(test.statement), qt.Equals, test.lineComment)
		})
	}
}

// TestTerminator_EndsTheStatement writes each executable statement with its
// terminator and another statement after it, and splits the script again. The
// script holds two statements only where the terminator ended the first one: a
// semicolon behind a line comment would leave the second statement inside the
// first.
func TestTerminator_EndsTheStatement(t *testing.T) {
	statements := []string{
		`DROP POLICY IF EXISTS "tenant_only" ON "site_media_settings"`,
		"SELECT count(*) AS n FROM orders WITH NO DATA -- empty too",
		"SELECT 1 # one",
		"SELECT 1 /* one */",
		"SELECT '-- not a comment'",
	}
	for _, statement := range statements {
		t.Run(statement, func(t *testing.T) {
			c := qt.New(t)
			script := statement + sqlscript.Terminator(statement) + " SELECT 2;"
			c.Assert(sqlutil.SplitSQLStatements(script), qt.DeepEquals, []string{statement, "SELECT 2"})
		})
	}
}

// TestBatchEnd ends the batch after an executable SQL Server statement, and
// nowhere else: not after a note, which runs nothing, and not on a dialect
// whose scripts have no batches.
func TestBatchEnd(t *testing.T) {
	tests := []struct {
		name      string
		dialect   string
		statement string
		want      string
	}{
		{name: "a SQL Server statement", dialect: "sqlserver", statement: "CREATE TABLE t (id int)", want: "\nGO"},
		{name: "a SQL Server spelling", dialect: "mssql", statement: "CREATE TABLE t (id int)", want: "\nGO"},
		{name: "a SQL Server note", dialect: "sqlserver", statement: "-- NOTE: nothing to run"},
		{name: "PostgreSQL", dialect: "postgres", statement: "CREATE TABLE t (id int)"},
		{name: "no dialect", statement: "CREATE TABLE t (id int)"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(sqlscript.BatchEnd(test.dialect, test.statement), qt.Equals, test.want)
		})
	}
}

// TestStatementEnd writes the terminator and the batch end after a statement
// a splitter returned. A T-SQL body that is not a BEGIN ... END block keeps
// its last semicolon through the split, and takes no second one.
func TestStatementEnd(t *testing.T) {
	tests := []struct {
		name      string
		dialect   string
		statement string
		want      string
	}{
		{name: "a SQL Server statement", dialect: "sqlserver", statement: "DROP TABLE t", want: ";\nGO"},
		{
			name: "a SQL Server body that keeps its semicolon", dialect: "sqlserver",
			statement: "CREATE TRIGGER trg ON t AFTER INSERT AS SET NOCOUNT ON;", want: "\nGO",
		},
		{name: "a SQL Server note", dialect: "sqlserver", statement: "-- NOTE: nothing to run"},
		{name: "PostgreSQL", dialect: "postgres", statement: "DROP TABLE t", want: ";"},
		{name: "a PostgreSQL note", dialect: "postgres", statement: "-- NOTE: nothing to run"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(sqlscript.StatementEnd(test.dialect, test.statement), qt.Equals, test.want)
		})
	}
}

// TestStatementEnd_SplitsBack writes SQL Server statements, in the shape a
// splitter returns them, as a script and splits it back as the migrator does.
// A trigger or a procedure whose body is not a BEGIN ... END block runs to the
// end of its batch, so written with a semicolon alone after it, it took the
// statements after it into its body.
func TestStatementEnd_SplitsBack(t *testing.T) {
	statements := []string{
		"CREATE TRIGGER trg ON t AFTER INSERT AS SET NOCOUNT ON;",
		"CREATE TABLE u (id int)",
		"CREATE OR ALTER PROCEDURE p AS SELECT 1 AS one;",
		"CREATE VIEW v AS SELECT id FROM u",
	}
	var script strings.Builder
	for _, statement := range statements {
		script.WriteString(statement + sqlscript.StatementEnd("sqlserver", statement) + "\n")
	}
	c := qt.New(t)

	split := sqlutil.SplitSQLStatementsForDialect(script.String(), "sqlserver")

	c.Assert(split, qt.DeepEquals, statements)
}

// TestWithBatchEnd puts the separator after a rendered statement's last line
// and keeps the line breaks it ended with after the separator.
func TestWithBatchEnd(t *testing.T) {
	tests := []struct {
		name      string
		dialect   string
		statement string
		want      string
	}{
		{name: "SQL Server", dialect: "sqlserver", statement: "CREATE TABLE t (\n  id int\n);\n", want: "CREATE TABLE t (\n  id int\n);\nGO\n"},
		{name: "a SQL Server note", dialect: "sqlserver", statement: "-- skipped\n", want: "-- skipped\n"},
		{name: "PostgreSQL", dialect: "postgres", statement: "CREATE TABLE t (id int);\n", want: "CREATE TABLE t (id int);\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(sqlscript.WithBatchEnd(test.dialect, test.statement), qt.Equals, test.want)
		})
	}
}
