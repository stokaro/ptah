package lintscan_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/lintext"
	"ptah.run/internal/lintscan"
)

// T-SQL control flow exposes the statements it can run, and a statement can
// start without a semicolon. Conditions retain their own source spans so a
// comment above an IF cannot suppress a statement inside its body.
func TestSplit_TransactSQLStatements(t *testing.T) {
	for _, test := range []struct {
		name string
		sql  string
		want []splitStatement
	}{
		{
			name: "a standalone drop",
			sql:  "DROP TABLE dbo.users;",
			want: []splitStatement{{SQL: "DROP TABLE dbo.users", Line: 1}},
		},
		{
			name: "a drop inside a conditional block",
			sql:  "IF OBJECT_ID(N'dbo.users') IS NOT NULL\nBEGIN\n  DROP TABLE dbo.users;\nEND;",
			want: []splitStatement{
				{SQL: "IF OBJECT_ID(N'dbo.users') IS NOT NULL", Line: 1},
				{SQL: "DROP TABLE dbo.users", Line: 3},
			},
		},
		{
			name: "a drop after an unseparated statement",
			sql:  "CREATE TABLE dbo.a (id int) DROP TABLE dbo.users",
			want: []splitStatement{
				{SQL: "CREATE TABLE dbo.a (id int)", Line: 1},
				{SQL: "DROP TABLE dbo.users", Line: 1},
			},
		},
		{
			name: "nested conditions and both TRY CATCH branches",
			sql: "IF 1 = 1\nBEGIN\n  WHILE 1 = 1\n    DROP TABLE dbo.a;\nEND\nELSE\n" +
				"BEGIN TRY\n  DROP TABLE dbo.b;\nEND TRY\nBEGIN CATCH\n  DROP TABLE dbo.c;\nEND CATCH;",
			want: []splitStatement{
				{SQL: "IF 1 = 1", Line: 1},
				{SQL: "WHILE 1 = 1", Line: 3},
				{SQL: "DROP TABLE dbo.a", Line: 4},
				{SQL: "DROP TABLE dbo.b", Line: 8},
				{SQL: "DROP TABLE dbo.c", Line: 11},
			},
		},
		{
			name: "a transaction is distinct from a control flow block",
			sql:  "BEGIN TRAN;\nBEGIN\n  DROP TABLE dbo.users;\nEND;\nCOMMIT;",
			want: []splitStatement{
				{SQL: "BEGIN TRAN", Line: 1},
				{SQL: "DROP TABLE dbo.users", Line: 3},
				{SQL: "COMMIT", Line: 5},
			},
		},
		{
			name: "an INSERT retains its EXEC source",
			sql:  "BEGIN INSERT INTO dbo.audit EXEC dbo.read_audit; DROP TABLE dbo.users; END;",
			want: []splitStatement{
				{SQL: "INSERT INTO dbo.audit EXEC dbo.read_audit", Line: 1},
				{SQL: "DROP TABLE dbo.users", Line: 1},
			},
		},
		{
			name: "a schema and its inline elements",
			sql:  "CREATE SCHEMA app\n  CREATE TABLE app.items (id int)\n  GRANT SELECT ON app.items TO public;",
			want: []splitStatement{
				{SQL: "CREATE SCHEMA app", Line: 1},
				{SQL: "CREATE TABLE app.items (id int)", Line: 2},
				{SQL: "GRANT SELECT ON app.items TO public", Line: 3},
			},
		},
		{
			name: "a schema with no inline element",
			sql:  "CREATE SCHEMA app;",
			want: []splitStatement{{SQL: "CREATE SCHEMA app", Line: 1}},
		},
		{
			name: "a MERGE keeps its arms together",
			sql:  "IF 1 = 1\nMERGE dbo.t AS t USING dbo.s AS s ON t.id = s.id WHEN MATCHED THEN DELETE;",
			want: []splitStatement{
				{SQL: "IF 1 = 1", Line: 1},
				{SQL: "MERGE dbo.t AS t USING dbo.s AS s ON t.id = s.id WHEN MATCHED THEN DELETE", Line: 2},
			},
		},
		{
			name: "unreadable text remains visible",
			sql:  "END;\nDROP TABLE dbo.users;",
			want: []splitStatement{{SQL: "END", Line: 1}, {SQL: "DROP TABLE dbo.users", Line: 2}},
		},
		{
			name: "the statements an EXEC of a literal runs",
			sql:  "-- reviewed\nEXEC(N'DROP TABLE dbo.a; TRUNCATE TABLE dbo.b');\nEXEC sp_executesql N'DELETE FROM dbo.c';",
			want: []splitStatement{
				{SQL: "DROP TABLE dbo.a", Line: 2, Comments: []string{"-- reviewed"}},
				{SQL: "TRUNCATE TABLE dbo.b", Line: 2, Comments: []string{"-- reviewed"}},
				{SQL: "DELETE FROM dbo.c", Line: 3},
			},
		},
		{
			name: "a literal over several lines and an EXEC inside it",
			sql:  "EXEC(N'\nDROP TABLE dbo.a;\nEXEC(N''DROP TABLE dbo.b'')')",
			want: []splitStatement{
				{SQL: "DROP TABLE dbo.a", Line: 2},
				{SQL: "DROP TABLE dbo.b", Line: 3},
			},
		},
		{
			name: "an EXEC of a variable, of a concatenation and of an unreadable literal",
			sql:  "EXEC(@sql);\nEXEC(N'SELECT 1' + N'');\nEXEC(N'END');",
			want: []splitStatement{
				{SQL: "EXEC(@sql)", Line: 1},
				{SQL: "EXEC(N'SELECT 1' + N'')", Line: 2},
				{SQL: "EXEC(N'END')", Line: 3},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statements := lintscan.Split(test.sql, lintscan.ForDialect("sqlserver"))
			var got []splitStatement
			for _, statement := range statements {
				got = append(got, splitStatement{SQL: statement.SQL, Line: statement.Line, Comments: statement.Comments})
			}
			c.Assert(got, qt.DeepEquals, test.want)
			c.Assert(lintext.ValidateSplit(
				lintext.SplitRequest{Sources: []lintext.Source{{Name: "1.up.sql", SQL: test.sql}}},
				lintext.SplitResult{Files: [][]lintext.Statement{statements}},
			), qt.IsNil)
		})
	}
}

func TestSplit_TransactSQLComments(t *testing.T) {
	for _, test := range []struct {
		name string
		sql  string
		want [][]string
	}{
		{"inside a block", "BEGIN\n-- reviewed\nDROP TABLE t;\nEND;", [][]string{{"-- reviewed"}}},
		{"a blank line inside a block", "BEGIN\n-- reviewed\n\nDROP TABLE t;\nEND;", [][]string{nil}},
		{"a comment before the block", "-- block\nBEGIN\nDROP TABLE t;\nEND;", [][]string{nil}},
		{"a condition and its body", "-- condition\nIF 1 = 1\n-- body\nDROP TABLE t;", [][]string{{"-- condition"}, {"-- body"}}},
		{"a comment on the condition's line", "IF 1 = 1 -- condition\nDROP TABLE t;", [][]string{nil, nil}},
		{"a trailing comment", "BEGIN\nSELECT 1; -- reviewed\nDROP TABLE t;\nEND;", [][]string{nil, nil}},
		{"an unseparated pair", "SELECT 1\n-- reviewed\nDROP TABLE t;", [][]string{nil, {"-- reviewed"}}},
		{"a comment on GO", "SELECT 1;\nGO -- reviewed\nDROP TABLE t;", [][]string{nil, nil}},
		{"a comment after GO", "SELECT 1;\nGO 2\n-- reviewed\nDROP TABLE t;", [][]string{nil, {"-- reviewed"}}},
		{"a schema and its elements", "-- schema\nCREATE SCHEMA app\n-- table\nCREATE TABLE app.items (id int)\n-- grant\nGRANT SELECT ON app.items TO public;", [][]string{{"-- schema"}, {"-- table"}, {"-- grant"}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(commentsOf(test.sql, lintscan.ForDialect("sqlserver")), qt.DeepEquals, test.want)
		})
	}
}

// A module stores its emitted piece. GO is a separator only outside a quoted
// value, a bracketed name and a comment, including a nested block comment.
func TestSplit_TransactSQLBatches(t *testing.T) {
	for _, test := range []struct {
		name string
		sql  string
		want []splitStatement
	}{
		{
			name: "an unterminated procedure piece includes statements after its END",
			sql:  "CREATE PROCEDURE dbo.p AS BEGIN SELECT 1; END\nDROP TABLE dbo.stored;",
			want: []splitStatement{{SQL: "CREATE PROCEDURE dbo.p AS BEGIN SELECT 1; END\nDROP TABLE dbo.stored;", Line: 1}},
		},
		{
			name: "a terminated procedure piece leaves the following drop executable",
			sql:  "CREATE PROCEDURE dbo.p AS BEGIN SELECT 1; END;\nDROP TABLE dbo.users;",
			want: []splitStatement{
				{SQL: "CREATE PROCEDURE dbo.p AS BEGIN SELECT 1; END;", Line: 1},
				{SQL: "DROP TABLE dbo.users", Line: 2},
			},
		},
		{
			name: "a piece is not text inside a preceding comment",
			sql:  "CREATE PROCEDURE dbo.p AS BEGIN SELECT 1; END;\n-- DROP TABLE dbo.users;\nDROP TABLE dbo.users;",
			want: []splitStatement{
				{SQL: "CREATE PROCEDURE dbo.p AS BEGIN SELECT 1; END;", Line: 1},
				{SQL: "DROP TABLE dbo.users", Line: 3, Comments: []string{"-- DROP TABLE dbo.users;"}},
			},
		},
		{
			name: "a client delimiter ends the procedure piece",
			sql:  "-- atlas:delimiter //\nCREATE PROCEDURE dbo.p AS BEGIN SELECT 1; END//\nDROP TABLE dbo.users//",
			want: []splitStatement{
				{SQL: "CREATE PROCEDURE dbo.p AS BEGIN SELECT 1; END", Line: 2, Comments: []string{"-- atlas:delimiter //"}},
				{SQL: "DROP TABLE dbo.users", Line: 3},
			},
		},
		{
			name: "a trigger body ends at GO",
			sql:  "CREATE TRIGGER dbo.trg ON dbo.t AFTER INSERT AS BEGIN DROP TABLE dbo.stored; END;\nGO\nDROP TABLE dbo.users;",
			want: []splitStatement{
				{SQL: "CREATE TRIGGER dbo.trg ON dbo.t AFTER INSERT AS BEGIN DROP TABLE dbo.stored; END;", Line: 1},
				{SQL: "DROP TABLE dbo.users", Line: 3},
			},
		},
		{
			name: "a function ends at GO",
			sql:  "CREATE FUNCTION dbo.f() RETURNS int AS BEGIN RETURN 1; END;\nGO\nDROP TABLE dbo.users;",
			want: []splitStatement{
				{SQL: "CREATE FUNCTION dbo.f() RETURNS int AS BEGIN RETURN 1; END;", Line: 1},
				{SQL: "DROP TABLE dbo.users", Line: 3},
			},
		},
		{
			name: "a view ends at GO",
			sql:  "CREATE VIEW dbo.v AS SELECT 1 AS id;\nGO\nDROP TABLE dbo.users;",
			want: []splitStatement{
				{SQL: "CREATE VIEW dbo.v AS SELECT 1 AS id;", Line: 1},
				{SQL: "DROP TABLE dbo.users", Line: 3},
			},
		},
		{
			name: "GO inside text and nested comments",
			sql: "SELECT N'first\nGO\nlast' AS [quoted\nGO\nname];\n/* outer\n /* inner */\n GO\n*/\n" +
				"GO 2 -- repeat\n-- reviewed\nDROP TABLE dbo.users;",
			want: []splitStatement{
				{SQL: "SELECT N'first\nGO\nlast' AS [quoted\nGO\nname]", Line: 1},
				{SQL: "DROP TABLE dbo.users", Line: 12, Comments: []string{"-- reviewed"}},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statements := lintscan.Split(test.sql, lintscan.ForDialect("sqlserver"))
			var got []splitStatement
			for _, statement := range statements {
				got = append(got, splitStatement{SQL: statement.SQL, Line: statement.Line, Comments: statement.Comments})
			}
			c.Assert(got, qt.DeepEquals, test.want)
			c.Assert(lintext.ValidateSplit(
				lintext.SplitRequest{Sources: []lintext.Source{{Name: "1.up.sql", SQL: test.sql}}},
				lintext.SplitResult{Files: [][]lintext.Statement{statements}},
			), qt.IsNil)
		})
	}
}

func TestSplit_TransactSQLSourceForms(t *testing.T) {
	c := qt.New(t)
	const sql = "-- π\nSELECT 1;\nGO 2 -- repeat\nIF 1 = 1\nBEGIN\n  -- reviewed\n  drop /* x */ table [Users];\nEND;"
	statements := lintscan.Split(sql, lintscan.ForDialect("sqlserver"))
	c.Assert(statements, qt.HasLen, 3)
	start := strings.Index(sql, "drop /* x */")
	const text = "drop /* x */ table [Users]"
	c.Assert(statements[2], qt.DeepEquals, lintext.Statement{
		Start: start, End: start + len(text), Line: 7, SQL: text, Canonical: "DROP TABLE [Users]",
		Words: []string{"DROP", "TABLE", "[Users]"}, SourceWords: []string{"drop", "table", "[Users]"},
		Comments: []string{"-- reviewed"},
	})
}

func TestSplit_TransactSQLQuotedIdentifier(t *testing.T) {
	c := qt.New(t)
	const sql = "BEGIN DROP TABLE [a]]b; DROP TABLE hidden]; DROP TABLE dbo.users; END;"
	statements := lintscan.Split(sql, lintscan.ForDialect("sqlserver"))
	c.Assert(statements, qt.HasLen, 2)
	c.Assert(statements[0].SQL, qt.Equals, "DROP TABLE [a]]b; DROP TABLE hidden]")
	c.Assert(statements[0].Words, qt.DeepEquals, []string{"DROP", "TABLE", "[a]]b; DROP TABLE hidden]"})
	c.Assert(statements[1].SQL, qt.Equals, "DROP TABLE dbo.users")
}

// A statement an EXEC runs from a literal that doubles a quote is read as the
// server runs it: Runs carries the text with each doubled quote read as one,
// and the words come from that text, while SQL stays the source range.
func TestSplit_TransactSQLExecLiteralRuns(t *testing.T) {
	c := qt.New(t)
	sql := "EXEC(N'UPDATE dbo.t SET n = ''a''; DROP TABLE dbo.u');"

	statements := lintscan.Split(sql, lintscan.ForDialect("sqlserver"))

	got := make([][]string, 0, len(statements))
	for _, statement := range statements {
		got = append(got, []string{statement.SQL, statement.Runs, strings.Join(statement.Words, " ")})
	}
	c.Assert(got, qt.DeepEquals, [][]string{
		{"UPDATE dbo.t SET n = ''a''", "UPDATE dbo.t SET n = 'a'", "UPDATE DBO . T SET N = 'a'"},
		{"DROP TABLE dbo.u", "", "DROP TABLE DBO . U"},
	})
	c.Assert(lintext.ValidateSplit(
		lintext.SplitRequest{Sources: []lintext.Source{{Name: "1.up.sql", SQL: sql}}},
		lintext.SplitResult{Files: [][]lintext.Statement{statements}},
	), qt.IsNil)
}
