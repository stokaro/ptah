//go:build integration

package integration_test

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// sqlServerTransactionControlScripts are migrations whose transaction control
// sits inside a statement the migrator sends whole: a block, a TRY block with
// its CATCH block, or an IF. Run, the COMMIT inside the first one makes its
// table and row durable while the run that sent it reports nothing applied, so
// each is refused before any statement runs, as a COMMIT on its own is. refusal
// is the part of the refusal that names the statement.
var sqlServerTransactionControlScripts = []struct {
	name    string
	up      string
	refusal string
}{
	{
		name: "a COMMIT inside a block",
		up: "CREATE TABLE dbo.log (msg nvarchar(100));\n" +
			"BEGIN\n  INSERT INTO dbo.log VALUES (N'before-commit');\n  COMMIT;\nEND;\n" +
			"INSERT INTO dbo.missing VALUES (1);\n",
		refusal: "tx-mode file up statement 2 because it controls transaction state",
	},
	{
		name: "a ROLLBACK in a CATCH block",
		up: "CREATE TABLE dbo.log (msg nvarchar(100));\n" +
			"BEGIN TRY\n  INSERT INTO dbo.log VALUES (N'try');\n  SELECT 1/0;\nEND TRY\nBEGIN CATCH\n  ROLLBACK;\nEND CATCH;\n",
		refusal: "tx-mode file up statement 2 because it controls transaction state",
	},
	{
		name: "a COMMIT an IF runs",
		up: "CREATE TABLE dbo.log (msg nvarchar(100));\nINSERT INTO dbo.log VALUES (N'before-commit');\n" +
			"IF @@TRANCOUNT > 0 COMMIT;\nINSERT INTO dbo.missing VALUES (1);\n",
		refusal: "tx-mode file up statement 3 because it controls transaction state",
	},
	{
		name: "a COMMIT in the block of an IF",
		up: "CREATE TABLE dbo.log (msg nvarchar(100));\n" +
			"IF 1 = 1 BEGIN INSERT INTO dbo.log VALUES (N'before-commit'); COMMIT; END;\n",
		refusal: "tx-mode file up statement 2 because it controls transaction state",
	},
	{
		name: "a transaction a TRY block opens and commits",
		up: "CREATE TABLE dbo.log (msg nvarchar(100));\n" +
			"BEGIN TRY\n  BEGIN TRAN;\n  INSERT INTO dbo.log VALUES (N'inner');\n  COMMIT;\nEND TRY\n" +
			"BEGIN CATCH\n  SELECT ERROR_MESSAGE();\nEND CATCH;\n",
		refusal: "tx-mode file up statement 2 because it controls transaction state",
	},
	{
		name: "a COMMIT an EXEC of a literal runs",
		up: "CREATE TABLE dbo.log (msg nvarchar(100));\n" +
			"INSERT INTO dbo.log VALUES (N'before-commit');\nEXEC(N'COMMIT');\n",
		refusal: "tx-mode file up statement 3 because it controls transaction state",
	},
	{
		name: "a ROLLBACK sp_executesql runs",
		up: "CREATE TABLE dbo.log (msg nvarchar(100));\n" +
			"EXEC sp_executesql N'ROLLBACK';\n",
		refusal: "tx-mode file up statement 2 because it controls transaction state",
	},
	{
		name: "a COMMIT inside a block of a file no transaction wraps",
		up: "-- atlas:txmode none\nCREATE TABLE dbo.log (msg nvarchar(100));\n" +
			"BEGIN\n  INSERT INTO dbo.log VALUES (N'before-commit');\n  COMMIT;\nEND;\n",
		refusal: "up statement 2 without a transaction because",
	},
}

// TestMigrationsUpRefusesSQLServerTransactionControlInsideAStatementLiveE2E
// applies each script through `ptah migrations up`: the run is refused, and the
// database holds nothing the script creates.
func TestMigrationsUpRefusesSQLServerTransactionControlInsideAStatementLiveE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.SQLServer)
	for _, test := range sqlServerTransactionControlScripts {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()
			databaseURL, _ := newSQLServerMigrationDatabase(c, ctx, dbURL, "")
			dir := c.TempDir()
			c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_init.up.sql"), []byte(test.up), 0o600), qt.IsNil)
			c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_init.down.sql"), []byte("SELECT 1;\n"), 0o600), qt.IsNil)

			out, err := runPtahNativeWithError("migrations", "up", "--db-url", databaseURL, "--migrations-dir", dir)

			c.Assert(err, qt.ErrorMatches, `(?s).*migration 1 cannot run `+regexp.QuoteMeta(test.refusal)+`.*`, qt.Commentf("%s", out))
			c.Assert(sqlServerUserObjects(c, databaseURL), qt.HasLen, 0)
		})
	}
}

// TestCompatMigrateApplyRefusesSQLServerTransactionControlInsideAStatementLiveE2E
// applies each script through `ptah-compat migrate apply`, which refuses what
// the native migrator refuses.
func TestCompatMigrateApplyRefusesSQLServerTransactionControlInsideAStatementLiveE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.SQLServer)
	for _, test := range sqlServerTransactionControlScripts {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()
			databaseURL, _ := newSQLServerMigrationDatabase(c, ctx, dbURL, "")
			dir := c.TempDir()
			c.Assert(os.WriteFile(filepath.Join(dir, "20260101000000_init.sql"), []byte(test.up), 0o600), qt.IsNil)
			out, err := runCompatVerb("migrate", "hash", "--dir", "file://"+filepath.ToSlash(dir))
			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))

			out, err = runCompatVerb("migrate", "apply", "--url", databaseURL, "--dir", "file://"+filepath.ToSlash(dir))

			c.Assert(err, qt.IsNotNil)
			c.Assert(out, qt.Matches, `(?s).*migration 20260101000000 cannot run `+regexp.QuoteMeta(test.refusal)+`.*`)
			c.Assert(sqlServerUserObjects(c, databaseURL), qt.HasLen, 0)
		})
	}
}

// sqlServerStoredTransactionControlScripts are migrations whose transaction
// control sits in a body the server stores rather than runs: a procedure that
// commits, a trigger that rolls back, and a procedure that rolls back to a
// savepoint. Each applies, and objects is what the database then holds.
var sqlServerStoredTransactionControlScripts = []struct {
	name    string
	up      string
	objects []string
}{
	{
		name: "a procedure that commits",
		up: "CREATE TABLE dbo.log (msg nvarchar(100));\n" +
			"CREATE PROCEDURE dbo.commit_work AS\nBEGIN\n  BEGIN TRAN;\n  INSERT INTO dbo.log VALUES (N'p');\n  COMMIT;\nEND\n",
		objects: []string{"P commit_work", "U log"},
	},
	{
		name: "a trigger that rolls back",
		up: "CREATE TABLE dbo.log (msg nvarchar(100));\n" +
			"CREATE TRIGGER dbo.log_guard ON dbo.log AFTER INSERT AS\nBEGIN\n" +
			"  IF EXISTS (SELECT 1 FROM inserted WHERE msg = N'bad') ROLLBACK;\nEND\n",
		objects: []string{"TR log_guard", "U log"},
	},
	{
		name: "a procedure that rolls back to a savepoint",
		up: "CREATE TABLE dbo.log (msg nvarchar(100));\n" +
			"CREATE PROCEDURE dbo.savepoint_work AS\nBEGIN\n  SAVE TRANSACTION before_write;\n" +
			"  INSERT INTO dbo.log VALUES (N's');\n  ROLLBACK TRANSACTION before_write;\nEND\n",
		objects: []string{"P savepoint_work", "U log"},
	},
}

// TestMigrationsUpAppliesSQLServerTransactionControlABodyStoresLiveE2E applies
// each script through `ptah migrations up`: the control the server stores is
// not refused, and the database holds the objects the script creates.
func TestMigrationsUpAppliesSQLServerTransactionControlABodyStoresLiveE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.SQLServer)
	for _, test := range sqlServerStoredTransactionControlScripts {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()
			databaseURL, _ := newSQLServerMigrationDatabase(c, ctx, dbURL, "")
			dir := c.TempDir()
			c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_init.up.sql"), []byte(test.up), 0o600), qt.IsNil)
			c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_init.down.sql"), []byte("SELECT 1;\n"), 0o600), qt.IsNil)

			out, err := runPtahNativeWithError("migrations", "up", "--db-url", databaseURL, "--migrations-dir", dir)

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			c.Assert(sqlServerUserObjects(c, databaseURL), qt.DeepEquals, test.objects)
		})
	}
}

// TestCompatMigrateApplyAppliesSQLServerTransactionControlABodyStoresLiveE2E
// applies each script through `ptah-compat migrate apply`.
func TestCompatMigrateApplyAppliesSQLServerTransactionControlABodyStoresLiveE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.SQLServer)
	for _, test := range sqlServerStoredTransactionControlScripts {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()
			databaseURL, _ := newSQLServerMigrationDatabase(c, ctx, dbURL, "")
			dir := c.TempDir()
			c.Assert(os.WriteFile(filepath.Join(dir, "20260101000000_init.sql"), []byte(test.up), 0o600), qt.IsNil)
			out, err := runCompatVerb("migrate", "hash", "--dir", "file://"+filepath.ToSlash(dir))
			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))

			out, err = runCompatVerb("migrate", "apply", "--url", databaseURL, "--dir", "file://"+filepath.ToSlash(dir))

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			c.Assert(sqlServerUserObjects(c, databaseURL), qt.DeepEquals, test.objects)
		})
	}
}
