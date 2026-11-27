//go:build integration

package integration_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// logRows reads dbo.log back as one line, its messages in order.
const logRows = "SELECT STRING_AGG(CONCAT(msg, ''), ',') WITHIN GROUP (ORDER BY msg) FROM dbo.log"

// sqlServerStatementBoundaryScripts are migrations, each with the query that
// reads its result back and the result a run leaves. sqlcmd runs every script
// but three as written on SQL Server 2022 and 2025 and leaves the same result.
// The three are a statement and a procedure after it with no semicolon and a
// procedure and the statement after its body, which the server refuses in one
// batch with Msg 111, and CREATE SCHEMA and the statement after its elements,
// which it refuses with Msg 156. Those rows show Ptah's own cut, which runs
// the routine or the schema as a batch of its own. Cut at every semicolon outside a
// routine body, a block reaches the server as a piece ending at an inner
// semicolon, which the server refuses with Msg 102, a MERGE sent without its
// semicolon is refused with Msg 10713, and a procedure sent after a statement
// in one batch is refused with Msg 111. Cut at every statement the server
// reads, a variable is out of scope in the statement after its DECLARE (Msg
// 137).
var sqlServerStatementBoundaryScripts = []struct {
	name  string
	up    string
	query string
	want  string
}{
	{
		name:  "statements with no semicolon between them",
		up:    "CREATE TABLE dbo.log (msg nvarchar(100))\nINSERT INTO dbo.log VALUES (N'one')\nINSERT INTO dbo.log VALUES (N'two')\n",
		query: logRows,
		want:  "one,two",
	},
	{
		name:  "a DECLARE and the statement that reads the variable with no semicolon",
		up:    "CREATE TABLE dbo.log (msg nvarchar(100));\nDECLARE @m nvarchar(100) = N'declared'\nINSERT INTO dbo.log VALUES (@m);\n",
		query: logRows,
		want:  "declared",
	},
	{
		name: "a statement and a procedure after it with no semicolon",
		up: "CREATE TABLE dbo.log (msg nvarchar(100))\n" +
			"CREATE PROCEDURE dbo.p AS BEGIN INSERT INTO dbo.log VALUES (N'p'); END;\nEXEC dbo.p;\n",
		query: logRows,
		want:  "p",
	},
	{
		name: "an IF with its block",
		up: "CREATE TABLE dbo.log (msg nvarchar(100));\n" +
			"IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'x') BEGIN CREATE TABLE dbo.x (id int); INSERT INTO dbo.x VALUES (1); END;\n",
		query: "SELECT CONCAT(COUNT(*), '') FROM dbo.x",
		want:  "1",
	},
	{
		name: "a TRY block with its CATCH block",
		up: "CREATE TABLE dbo.log (msg nvarchar(100));\n" +
			"BEGIN TRY\n  INSERT INTO dbo.log VALUES (N'try');\n  SELECT 1/0;\nEND TRY\nBEGIN CATCH\n  INSERT INTO dbo.log VALUES (N'catch');\nEND CATCH;\n",
		query: logRows,
		want:  "catch,try",
	},
	{
		name: "a nested block",
		up: "CREATE TABLE dbo.log (msg nvarchar(100));\n" +
			"BEGIN\n  INSERT INTO dbo.log VALUES (N'outer');\n  BEGIN\n    INSERT INTO dbo.log VALUES (N'inner');\n  END;\nEND;\n",
		query: logRows,
		want:  "inner,outer",
	},
	{
		name: "an IF with an ELSE",
		up: "CREATE TABLE dbo.log (msg nvarchar(100));\n" +
			"IF 1 = 0\nBEGIN\n  INSERT INTO dbo.log VALUES (N'then');\nEND\nELSE\nBEGIN\n  INSERT INTO dbo.log VALUES (N'else-1');\n  INSERT INTO dbo.log VALUES (N'else-2');\nEND;\n",
		query: logRows,
		want:  "else-1,else-2",
	},
	{
		name: "a WHILE with its block",
		up: "CREATE TABLE dbo.log (msg nvarchar(100));\nINSERT INTO dbo.log VALUES (N'a'), (N'b'), (N'c');\n" +
			"WHILE EXISTS (SELECT 1 FROM dbo.log WHERE msg <> N'z')\nBEGIN\n  UPDATE TOP (1) dbo.log SET msg = N'z' WHERE msg <> N'z';\n  INSERT INTO dbo.log VALUES (N'z');\nEND;\n",
		query: logRows,
		want:  "z,z,z,z,z,z",
	},
	{
		name: "a procedure and the statement after its body",
		up: "CREATE TABLE dbo.log (msg nvarchar(100));\n" +
			"CREATE PROCEDURE dbo.p AS\nBEGIN\n  INSERT INTO dbo.log VALUES (N'p-1');\n  INSERT INTO dbo.log VALUES (N'p-2');\nEND;\nEXEC dbo.p;\n",
		query: "SELECT CONCAT(STRING_AGG(msg, ',') WITHIN GROUP (ORDER BY msg), '|', CHARINDEX('EXEC', OBJECT_DEFINITION(OBJECT_ID('dbo.p')))) FROM dbo.log",
		want:  "p-1,p-2|0",
	},
	{
		name: "a MERGE that ends the migration",
		up: "CREATE TABLE dbo.log (id int PRIMARY KEY, msg nvarchar(100));\n" +
			"MERGE dbo.log AS t USING (SELECT 1 AS id, N'merged' AS msg) AS s ON t.id = s.id\nWHEN NOT MATCHED THEN INSERT (id, msg) VALUES (s.id, s.msg);\n",
		query: logRows,
		want:  "merged",
	},
	{
		name: "a MERGE and a statement after it",
		up: "CREATE TABLE dbo.log (id int PRIMARY KEY, msg nvarchar(100));\n" +
			"MERGE dbo.log AS t USING (SELECT 1 AS id, N'merged' AS msg) AS s ON t.id = s.id\nWHEN NOT MATCHED THEN INSERT (id, msg) VALUES (s.id, s.msg);\n" +
			"INSERT INTO dbo.log VALUES (2, N'after');\n",
		query: logRows,
		want:  "after,merged",
	},
	{
		name: "GO, GO with a count, and a semicolon, END and GO in strings and comments",
		up: "CREATE TABLE dbo.log (msg nvarchar(100));\nGO\nINSERT INTO dbo.log VALUES (N'go');\nGO 3\n" +
			"INSERT INTO dbo.log VALUES (N'x; END GO'); -- ; END\n/* ; GO\nEND */\n" +
			"IF 1 = 1 BEGIN INSERT INTO dbo.log VALUES (N'[;]'); INSERT INTO dbo.log VALUES (N'END'); END\nGO\n",
		query: logRows,
		want:  "[;],END,go,go,go,x; END GO",
	},
	{
		name: "a DECLARE and the statement that reads the variable in one block",
		up: "CREATE TABLE dbo.log (msg nvarchar(100));\n" +
			"BEGIN\n  DECLARE @m nvarchar(100) = N'declared';\n  INSERT INTO dbo.log VALUES (@m);\nEND;\n",
		query: logRows,
		want:  "declared",
	},
	{
		name:  "CREATE SCHEMA and the statement after its elements",
		up:    "CREATE SCHEMA app CREATE TABLE log (msg nvarchar(100));\nINSERT INTO app.log VALUES (N'in app');\n",
		query: "SELECT STRING_AGG(msg, ',') FROM app.log",
		want:  "in app",
	},
}

// TestMigrationsUpSendsSQLServerStatementsWhereTheServerEndsThemLiveE2E applies
// each script through `ptah migrations up` and reads the result back.
func TestMigrationsUpSendsSQLServerStatementsWhereTheServerEndsThemLiveE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.SQLServer)
	for _, test := range sqlServerStatementBoundaryScripts {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()
			databaseURL, db := newSQLServerMigrationDatabase(c, ctx, dbURL, "")
			dir := c.TempDir()
			c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_init.up.sql"), []byte(test.up), 0o600), qt.IsNil)
			c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_init.down.sql"), []byte("SELECT 1;\n"), 0o600), qt.IsNil)

			out, err := runPtahNativeWithError("migrations", "up", "--db-url", databaseURL, "--migrations-dir", dir)

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			var got string
			c.Assert(db.QueryRowContext(ctx, test.query).Scan(&got), qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// TestCompatMigrateApplySendsSQLServerStatementsWhereTheServerEndsThemLiveE2E
// applies each script through `ptah-compat migrate apply`, which sends the
// statements the native migrator does, and reads the result back.
func TestCompatMigrateApplySendsSQLServerStatementsWhereTheServerEndsThemLiveE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.SQLServer)
	for _, test := range sqlServerStatementBoundaryScripts {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()
			databaseURL, db := newSQLServerMigrationDatabase(c, ctx, dbURL, "")
			dir := c.TempDir()
			c.Assert(os.WriteFile(filepath.Join(dir, "20260101000000_init.sql"), []byte(test.up), 0o600), qt.IsNil)
			out, err := runCompatVerb("migrate", "hash", "--dir", "file://"+filepath.ToSlash(dir))
			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))

			out, err = runCompatVerb("migrate", "apply", "--url", databaseURL, "--dir", "file://"+filepath.ToSlash(dir))

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			var got string
			c.Assert(db.QueryRowContext(ctx, test.query).Scan(&got), qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}
