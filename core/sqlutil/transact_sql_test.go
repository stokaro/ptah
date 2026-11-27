package sqlutil_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/sqlutil"
)

// TestSplitSQLStatementsForDialect_SQLServerEndsAStatementWhereTheServerDoes
// pins the pieces a T-SQL script is sent as, each a batch of its own. Each row
// is a shape that sqlcmd runs as written on SQL Server 2022 and 2025. Cut at
// every semicolon outside a routine body, the blocks and the MERGE rows fail
// there with Msg 102 or Msg 10713: a block cut at an inner semicolon leaves an
// END without its BEGIN, and a MERGE sent without its semicolon is refused.
// Cut at every statement the server reads, the rows with no semicolon between
// two statements fail with Msg 137, since a variable lives in its batch.
func TestSplitSQLStatementsForDialect_SQLServerEndsAStatementWhereTheServerDoes(t *testing.T) {
	for _, test := range []struct {
		name   string
		script string
		want   []string
	}{
		{
			name:   "two statements with no semicolon between them",
			script: "CREATE TABLE dbo.a (id int) TRUNCATE TABLE dbo.x",
			want:   []string{"CREATE TABLE dbo.a (id int) TRUNCATE TABLE dbo.x"},
		},
		{
			name:   "a DECLARE and the statement that reads the variable",
			script: "DECLARE @n int = 1\nINSERT INTO dbo.t VALUES (@n);\nSELECT 1;",
			want:   []string{"DECLARE @n int = 1\nINSERT INTO dbo.t VALUES (@n)", "SELECT 1"},
		},
		{
			name:   "a statement and a procedure after it with no semicolon",
			script: "CREATE TABLE dbo.t (id int)\n-- the procedure\nCREATE PROCEDURE dbo.p AS BEGIN SELECT 1; END;\nSELECT 2;",
			want:   []string{"CREATE TABLE dbo.t (id int)", "-- the procedure\nCREATE PROCEDURE dbo.p AS BEGIN SELECT 1; END", "SELECT 2"},
		},
		{
			name:   "a statement and CREATE SCHEMA after it with no semicolon",
			script: "SELECT 1 CREATE SCHEMA s AUTHORIZATION dbo;",
			want:   []string{"SELECT 1", "CREATE SCHEMA s AUTHORIZATION dbo"},
		},
		{
			name:   "a block is one statement",
			script: "BEGIN\n  CREATE TABLE dbo.x (id int);\n  INSERT INTO dbo.x VALUES (1);\nEND;\nSELECT 1;\n",
			want:   []string{"BEGIN\n  CREATE TABLE dbo.x (id int);\n  INSERT INTO dbo.x VALUES (1);\nEND", "SELECT 1"},
		},
		{
			name:   "a nested block is part of the block around it",
			script: "BEGIN INSERT INTO dbo.t VALUES ('outer'); BEGIN INSERT INTO dbo.t VALUES ('inner'); END; END;\nSELECT 1;",
			want:   []string{"BEGIN INSERT INTO dbo.t VALUES ('outer'); BEGIN INSERT INTO dbo.t VALUES ('inner'); END; END", "SELECT 1"},
		},
		{
			name:   "an IF with its block",
			script: "IF OBJECT_ID(N'dbo.x') IS NULL\nBEGIN\n  CREATE TABLE dbo.x (id int);\n  INSERT INTO dbo.x VALUES (1);\nEND;\nSELECT 1;",
			want:   []string{"IF OBJECT_ID(N'dbo.x') IS NULL\nBEGIN\n  CREATE TABLE dbo.x (id int);\n  INSERT INTO dbo.x VALUES (1);\nEND", "SELECT 1"},
		},
		{
			name:   "an IF with an ELSE",
			script: "IF 1 = 0 BEGIN INSERT INTO dbo.t VALUES ('then'); END ELSE BEGIN INSERT INTO dbo.t VALUES ('else-1'); INSERT INTO dbo.t VALUES ('else-2'); END;\nSELECT 1;",
			want:   []string{"IF 1 = 0 BEGIN INSERT INTO dbo.t VALUES ('then'); END ELSE BEGIN INSERT INTO dbo.t VALUES ('else-1'); INSERT INTO dbo.t VALUES ('else-2'); END", "SELECT 1"},
		},
		{
			name:   "an IF whose statement is no block",
			script: "IF 1 = 1 INSERT INTO dbo.t VALUES (1); SELECT 1;",
			want:   []string{"IF 1 = 1 INSERT INTO dbo.t VALUES (1)", "SELECT 1"},
		},
		{
			name:   "a TRY block with its CATCH block",
			script: "BEGIN TRY\n  INSERT INTO dbo.t VALUES ('try');\n  SELECT 1 / 0;\nEND TRY\nBEGIN CATCH\n  INSERT INTO dbo.t VALUES ('catch');\nEND CATCH;\nSELECT 1;",
			want:   []string{"BEGIN TRY\n  INSERT INTO dbo.t VALUES ('try');\n  SELECT 1 / 0;\nEND TRY\nBEGIN CATCH\n  INSERT INTO dbo.t VALUES ('catch');\nEND CATCH", "SELECT 1"},
		},
		{
			name:   "a WHILE with its block",
			script: "WHILE (SELECT COUNT(*) FROM dbo.t) < 6 BEGIN INSERT INTO dbo.t VALUES ('z'); END;\nSELECT 1;",
			want:   []string{"WHILE (SELECT COUNT(*) FROM dbo.t) < 6 BEGIN INSERT INTO dbo.t VALUES ('z'); END", "SELECT 1"},
		},
		{
			name:   "a MERGE keeps its semicolon",
			script: "MERGE dbo.t AS t USING (SELECT 1 AS id) AS s ON t.id = s.id WHEN NOT MATCHED THEN INSERT (id) VALUES (s.id);\nSELECT 1;",
			want:   []string{"MERGE dbo.t AS t USING (SELECT 1 AS id) AS s ON t.id = s.id WHEN NOT MATCHED THEN INSERT (id) VALUES (s.id);", "SELECT 1"},
		},
		{
			name:   "a MERGE that ends the script keeps its semicolon",
			script: "MERGE dbo.t AS t USING (SELECT 1 AS id) AS s ON t.id = s.id WHEN NOT MATCHED THEN INSERT (id) VALUES (s.id);\n",
			want:   []string{"MERGE dbo.t AS t USING (SELECT 1 AS id) AS s ON t.id = s.id WHEN NOT MATCHED THEN INSERT (id) VALUES (s.id);"},
		},
		{
			name:   "an IF that runs a MERGE keeps its semicolon",
			script: "IF 1 = 1 MERGE dbo.t AS t USING (SELECT 1 AS id) AS s ON t.id = s.id WHEN NOT MATCHED THEN INSERT (id) VALUES (s.id);\nSELECT 1;",
			want:   []string{"IF 1 = 1 MERGE dbo.t AS t USING (SELECT 1 AS id) AS s ON t.id = s.id WHEN NOT MATCHED THEN INSERT (id) VALUES (s.id);", "SELECT 1"},
		},
		{
			name:   "a stray semicolon between two statements",
			script: "SELECT 1;;\n; SELECT 2;",
			want:   []string{"SELECT 1", "SELECT 2"},
		},
		{
			name:   "a semicolon, END and GO in a string, a name and comments",
			script: "INSERT INTO dbo.t ([semi;END]) VALUES ('[;],END,go'); -- ; END\n/* GO; END */ SELECT 1;\n",
			want:   []string{"INSERT INTO dbo.t ([semi;END]) VALUES ('[;],END,go')", "-- ; END\n/* GO; END */ SELECT 1"},
		},
		{
			name:   "GO ends a batch between two blocks",
			script: "BEGIN INSERT INTO dbo.t VALUES (1); END\nGO\nBEGIN INSERT INTO dbo.t VALUES (2); END\n",
			want:   []string{"BEGIN INSERT INTO dbo.t VALUES (1); END", "BEGIN INSERT INTO dbo.t VALUES (2); END"},
		},
		{
			name:   "GO with a count repeats each piece of its batch",
			script: "BEGIN INSERT INTO dbo.t VALUES (1); END\nSELECT 1\nGO 2\nSELECT 2;\n",
			want:   []string{"BEGIN INSERT INTO dbo.t VALUES (1); END\nSELECT 1", "BEGIN INSERT INTO dbo.t VALUES (1); END\nSELECT 1", "SELECT 2"},
		},
		{
			name:   "GO 0 discards its batch",
			script: "SELECT 1;\nGO 0\nSELECT 2;\n",
			want:   []string{"SELECT 2"},
		},
		{
			name:   "a procedure ends at the END of its body",
			script: "CREATE PROCEDURE dbo.p AS BEGIN SELECT 1; SELECT 2; END;\nIF 1 = 1 BEGIN SELECT 3; END;\n",
			want:   []string{"CREATE PROCEDURE dbo.p AS BEGIN SELECT 1; SELECT 2; END", "IF 1 = 1 BEGIN SELECT 3; END"},
		},
		{
			name:   "CREATE SCHEMA ends at the first semicolon after it",
			script: "CREATE SCHEMA s CREATE TABLE t (id int) GRANT SELECT ON t TO public;\nSELECT 1;\n",
			want:   []string{"CREATE SCHEMA s CREATE TABLE t (id int) GRANT SELECT ON t TO public", "SELECT 1"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := sqlutil.SplitSQLStatementsForDialect(test.script, "sqlserver")

			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// TestSplitSQLStatementsForDialect_SQLServerCutsAnUnreadableBatchAtEachSemicolon
// pins the fallback for a batch whose statement boundaries the reader cannot
// find: it is cut at each semicolon outside a routine body, so a statement
// after a stray END is still sent on its own rather than as part of the text
// before it.
func TestSplitSQLStatementsForDialect_SQLServerCutsAnUnreadableBatchAtEachSemicolon(t *testing.T) {
	c := qt.New(t)

	got := sqlutil.SplitSQLStatementsForDialect("SELECT 1; END; SELECT 2;\nGO\nBEGIN SELECT 3; END\n", "sqlserver")

	c.Assert(got, qt.DeepEquals, []string{"SELECT 1", "END", "SELECT 2", "BEGIN SELECT 3; END"})
}

// TestSplitSourceStatements_SQLServerEndsAStatementWhereTheServerDoes pins the
// source text of each statement a T-SQL script is sent as. The migrator pairs
// the two lists by position, to hash each statement and to name the one that
// failed, so every row also holds as many statements as
// [sqlutil.SplitSQLStatementsForDialect] returns for the script. A statement
// with no semicolon after it keeps the rest of its text, as on every dialect.
func TestSplitSourceStatements_SQLServerEndsAStatementWhereTheServerDoes(t *testing.T) {
	for _, test := range []struct {
		name   string
		script string
		want   []sqlutil.SourceStatement
	}{
		{
			name:   "two statements with no semicolon between them",
			script: "CREATE TABLE dbo.a (id int) TRUNCATE TABLE dbo.x\n",
			want: []sqlutil.SourceStatement{
				{Text: "CREATE TABLE dbo.a (id int) TRUNCATE TABLE dbo.x\n"},
			},
		},
		{
			name:   "a statement and a procedure after it with no semicolon",
			script: "CREATE TABLE dbo.t (id int)\nCREATE PROCEDURE dbo.p AS BEGIN SELECT 1; END;\n",
			want: []sqlutil.SourceStatement{
				{Text: "CREATE TABLE dbo.t (id int)"},
				{Text: "CREATE PROCEDURE dbo.p AS BEGIN SELECT 1; END;", Terminated: true},
			},
		},
		{
			name:   "a block and a MERGE",
			script: "BEGIN SELECT 1; END;\nMERGE dbo.t AS t USING (SELECT 1 AS id) AS s ON t.id = s.id WHEN NOT MATCHED THEN INSERT (id) VALUES (s.id);\n",
			want: []sqlutil.SourceStatement{
				{Text: "BEGIN SELECT 1; END;", Terminated: true},
				{Text: "MERGE dbo.t AS t USING (SELECT 1 AS id) AS s ON t.id = s.id WHEN NOT MATCHED THEN INSERT (id) VALUES (s.id);", Terminated: true},
			},
		},
		{
			name:   "GO lines belong to no statement",
			script: "SELECT 1;\nGO\nSELECT 2;\nGO 2\nSELECT 3;\nGO 0\n",
			want: []sqlutil.SourceStatement{
				{Text: "SELECT 1;", Terminated: true},
				{Text: "SELECT 2;", Terminated: true},
				{Text: "SELECT 2;", Terminated: true},
			},
		},
		{
			name:   "a comment after the last statement stays with it",
			script: "SELECT 1\n-- trailing\n",
			want: []sqlutil.SourceStatement{
				{Text: "SELECT 1\n-- trailing\n"},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := sqlutil.SplitSourceStatements(test.script, "sqlserver")

			c.Assert(got, qt.DeepEquals, test.want)
			c.Assert(got, qt.HasLen, len(sqlutil.SplitSQLStatementsForDialect(test.script, "sqlserver")))
		})
	}
}
