//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// sqlServerPartialFailureScripts are migrations no transaction wraps whose
// last statement fails, each with the statements a run applies before it, the
// statements the file holds, and the failed statement as the native revision
// records it, as it was sent, and as the Atlas-format revision records it, as
// the file writes it. A block, a MERGE and a comment are each one statement or
// none, so the count and the failed statement are the ones the file writes:
// cut at every semicolon, the block of the first script would count as three
// statements and the failure would name its END.
var sqlServerPartialFailureScripts = []struct {
	name    string
	up      string
	applied int
	total   int
	sent    string
	written string
}{
	{
		name: "a GO line, a block and a failing INSERT",
		up: "-- atlas:txmode none\nCREATE TABLE dbo.log (msg nvarchar(100));\nGO\n" +
			"BEGIN\n  INSERT INTO dbo.log VALUES (N'one');\n  INSERT INTO dbo.log VALUES (N'two');\nEND;\n" +
			"INSERT INTO dbo.missing VALUES (1);\n",
		applied: 2, total: 3, sent: "INSERT INTO dbo.missing VALUES (1)", written: "INSERT INTO dbo.missing VALUES (1);",
	},
	{
		name: "a MERGE and a failing INSERT",
		up: "-- atlas:txmode none\nCREATE TABLE dbo.log (id int PRIMARY KEY, msg nvarchar(100));\n" +
			"MERGE dbo.log AS t USING (SELECT 1 AS id, N'one' AS msg) AS s ON t.id = s.id\n" +
			"WHEN NOT MATCHED THEN INSERT (id, msg) VALUES (s.id, s.msg);\n" +
			"INSERT INTO dbo.missing VALUES (1);\n",
		applied: 2, total: 3, sent: "INSERT INTO dbo.missing VALUES (1)", written: "INSERT INTO dbo.missing VALUES (1);",
	},
	{
		name: "a comment that holds a comment and a failing INSERT",
		up: "-- atlas:txmode none\nCREATE TABLE dbo.log (msg nvarchar(100));\n" +
			"/* a /* b */ c */;\nINSERT INTO dbo.missing VALUES (1);\n",
		applied: 1, total: 2, sent: "INSERT INTO dbo.missing VALUES (1)", written: "INSERT INTO dbo.missing VALUES (1);",
	},
	{
		name: "a client delimiter and a failing INSERT",
		up: "-- atlas:txmode none\n-- atlas:delimiter //\nCREATE TABLE dbo.log (msg nvarchar(100))\n//\n" +
			"INSERT INTO dbo.missing VALUES (1)\n//\n",
		applied: 1, total: 2, sent: "INSERT INTO dbo.missing VALUES (1)", written: "INSERT INTO dbo.missing VALUES (1)",
	},
}

// sqlServerRevision reads the applied and total counts and the failed
// statement of the one revision in table.
func sqlServerRevision(c *qt.C, ctx context.Context, db *sql.DB, table string) string {
	c.Helper()
	var applied, total int
	var failed string
	// #nosec G202 -- table is one of the two revision table names the tests pass.
	query := "SELECT applied, total, COALESCE(error_stmt, '') FROM " + table
	c.Assert(db.QueryRowContext(ctx, query).Scan(&applied, &total, &failed), qt.IsNil)
	return fmt.Sprintf("%d/%d %s", applied, total, failed)
}

// TestMigrationsUpRecordsTheSQLServerStatementThatFailedLiveE2E applies each
// script through `ptah migrations up` and reads the revision it leaves.
func TestMigrationsUpRecordsTheSQLServerStatementThatFailedLiveE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.SQLServer)
	for _, test := range sqlServerPartialFailureScripts {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()
			databaseURL, db := newSQLServerMigrationDatabase(c, ctx, dbURL, "")
			dir := c.TempDir()
			c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_init.up.sql"), []byte(test.up), 0o600), qt.IsNil)
			c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_init.down.sql"), []byte("SELECT 1;\n"), 0o600), qt.IsNil)

			out, err := runPtahNativeWithError("migrations", "up", "--db-url", databaseURL, "--migrations-dir", dir)

			c.Assert(err, qt.IsNotNil, qt.Commentf("%s", out))
			c.Assert(sqlServerRevision(c, ctx, db, "schema_migrations"), qt.Equals, fmt.Sprintf("%d/%d %s", test.applied, test.total, test.sent))
		})
	}
}

// TestCompatMigrateApplyRecordsTheSQLServerStatementThatFailedLiveE2E applies
// each script through `ptah-compat migrate apply` and reads the revision it
// leaves.
func TestCompatMigrateApplyRecordsTheSQLServerStatementThatFailedLiveE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.SQLServer)
	for _, test := range sqlServerPartialFailureScripts {
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

			c.Assert(err, qt.IsNotNil, qt.Commentf("%s", out))
			c.Assert(sqlServerRevision(c, ctx, db, "atlas_schema_revisions"), qt.Equals, fmt.Sprintf("%d/%d %s", test.applied, test.total, test.written))
		})
	}
}

// sqlServerResumedScript is a migration no transaction wraps that fails after
// a block committed. The block runs only INSERTs, which leave no session state
// a resumed run has to restore, so once the missing table exists the run
// resumes after the block: each row it inserted is there once.
const sqlServerResumedScript = "-- atlas:txmode none\nCREATE TABLE dbo.log (msg nvarchar(100));\n" +
	"BEGIN\n  DECLARE @m nvarchar(100) = N'one';\n  INSERT INTO dbo.log VALUES (@m);\n  INSERT INTO dbo.log VALUES (N'two');\nEND;\n" +
	"INSERT INTO dbo.missing VALUES (1);\n"

// TestMigrationsUpResumesAfterASQLServerBlockLiveE2E fails the script through
// `ptah migrations up`, creates the table it lacked, and resumes it with
// --allow-dirty.
func TestMigrationsUpResumesAfterASQLServerBlockLiveE2E(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	databaseURL, db := newSQLServerMigrationDatabase(c, ctx, dbtarget.URL(t, dbtarget.SQLServer), "")
	dir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_init.up.sql"), []byte(sqlServerResumedScript), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_init.down.sql"), []byte("SELECT 1;\n"), 0o600), qt.IsNil)
	out, err := runPtahNativeWithError("migrations", "up", "--db-url", databaseURL, "--migrations-dir", dir)
	c.Assert(err, qt.IsNotNil, qt.Commentf("%s", out))
	_, err = db.ExecContext(ctx, "CREATE TABLE dbo.missing (id int)")
	c.Assert(err, qt.IsNil)

	out, err = runPtahNativeWithError("migrations", "up", "--db-url", databaseURL, "--migrations-dir", dir, "--allow-dirty")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	var rows string
	c.Assert(db.QueryRowContext(ctx, logRows).Scan(&rows), qt.IsNil)
	c.Assert(rows, qt.Equals, "one,two")
	c.Assert(sqlServerRevision(c, ctx, db, "schema_migrations"), qt.Equals, "3/3 ")
}

// TestCompatMigrateApplyResumesAfterASQLServerBlockLiveE2E is the same run
// through `ptah-compat migrate apply`, resumed with --allow-dirty too.
func TestCompatMigrateApplyResumesAfterASQLServerBlockLiveE2E(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	databaseURL, db := newSQLServerMigrationDatabase(c, ctx, dbtarget.URL(t, dbtarget.SQLServer), "")
	dir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "20260101000000_init.sql"), []byte(sqlServerResumedScript), 0o600), qt.IsNil)
	out, err := runCompatVerb("migrate", "hash", "--dir", "file://"+filepath.ToSlash(dir))
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	out, err = runCompatVerb("migrate", "apply", "--url", databaseURL, "--dir", "file://"+filepath.ToSlash(dir))
	c.Assert(err, qt.IsNotNil, qt.Commentf("%s", out))
	_, err = db.ExecContext(ctx, "CREATE TABLE dbo.missing (id int)")
	c.Assert(err, qt.IsNil)

	out, err = runCompatVerb("migrate", "apply", "--url", databaseURL, "--dir", "file://"+filepath.ToSlash(dir), "--allow-dirty")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	var rows string
	c.Assert(db.QueryRowContext(ctx, logRows).Scan(&rows), qt.IsNil)
	c.Assert(rows, qt.Equals, "one,two")
	c.Assert(sqlServerRevision(c, ctx, db, "atlas_schema_revisions"), qt.Equals, "3/3 ")
}

// sqlServerPartlyCommittedScripts are migrations no transaction wraps that fail
// inside a statement that runs several: a block, and two INSERTs with no
// semicolon between them. The server commits the INSERT of 'a' before the one
// that fails, so running the statement again would write 'a' twice. Once the
// missing table exists, a retry refuses to run the statement again and leaves
// the rows and the revision as the failed run left them.
var sqlServerPartlyCommittedScripts = []struct {
	name string
	up   string
}{
	{
		name: "a block that fails after an INSERT",
		up: "-- atlas:txmode none\nCREATE TABLE dbo.log (msg nvarchar(10));\n" +
			"BEGIN\n  INSERT INTO dbo.log VALUES (N'a');\n  INSERT INTO dbo.missing VALUES (1);\nEND;\n" +
			"INSERT INTO dbo.log VALUES (N'after');\n",
	},
	{
		name: "two INSERTs with no semicolon between them",
		up: "-- atlas:txmode none\nCREATE TABLE dbo.log (msg nvarchar(10));\n" +
			"INSERT INTO dbo.log VALUES (N'a') INSERT INTO dbo.missing VALUES (1);\n" +
			"INSERT INTO dbo.log VALUES (N'after');\n",
	},
}

// sqlServerPartlyCommittedRefusal is the part of the retry's refusal that
// names the statement.
const sqlServerPartlyCommittedRefusal = "cannot resume automatically: statement 2 runs several statements outside a transaction"

// TestMigrationsUpRefusesToRerunAPartlyCommittedSQLServerStatementLiveE2E fails
// each script through `ptah migrations up`, creates the missing table, and
// retries with --allow-dirty. `ptah migrations status` then names the resume
// after the failed statement.
func TestMigrationsUpRefusesToRerunAPartlyCommittedSQLServerStatementLiveE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.SQLServer)
	for _, test := range sqlServerPartlyCommittedScripts {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()
			databaseURL, db := newSQLServerMigrationDatabase(c, ctx, dbURL, "")
			dir := c.TempDir()
			c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_init.up.sql"), []byte(test.up), 0o600), qt.IsNil)
			c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_init.down.sql"), []byte("SELECT 1;\n"), 0o600), qt.IsNil)
			out, err := runPtahNativeWithError("migrations", "up", "--db-url", databaseURL, "--migrations-dir", dir)
			c.Assert(err, qt.IsNotNil, qt.Commentf("%s", out))
			_, err = db.ExecContext(ctx, "CREATE TABLE dbo.missing (id int)")
			c.Assert(err, qt.IsNil)

			out, err = runPtahNativeWithError("migrations", "up", "--db-url", databaseURL, "--migrations-dir", dir, "--allow-dirty")

			c.Assert(err, qt.ErrorMatches, `(?s).*migration 1 `+regexp.QuoteMeta(sqlServerPartlyCommittedRefusal)+`.*`, qt.Commentf("%s", out))
			var rows string
			c.Assert(db.QueryRowContext(ctx, logRows).Scan(&rows), qt.IsNil)
			c.Assert(rows, qt.Equals, "a")
			c.Assert(sqlServerRevisionCounts(c, ctx, db, "schema_migrations"), qt.Equals, "1/3")
			status, _ := runPtahNativeWithError("migrations", "status", "--db-url", databaseURL, "--migrations-dir", dir)
			c.Assert(status, qt.Contains, "This migration failed in statement 2 of 3, which runs several statements outside a transaction")
			c.Assert(status, qt.Contains, "'ptah migrations repair --version 1 --resume-from 3")
		})
	}
}

// TestCompatMigrateApplyRefusesToRerunAPartlyCommittedSQLServerStatementLiveE2E
// is the same run through `ptah-compat migrate apply`.
func TestCompatMigrateApplyRefusesToRerunAPartlyCommittedSQLServerStatementLiveE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.SQLServer)
	for _, test := range sqlServerPartlyCommittedScripts {
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
			c.Assert(err, qt.IsNotNil, qt.Commentf("%s", out))
			_, err = db.ExecContext(ctx, "CREATE TABLE dbo.missing (id int)")
			c.Assert(err, qt.IsNil)

			out, err = runCompatVerb("migrate", "apply", "--url", databaseURL, "--dir", "file://"+filepath.ToSlash(dir), "--allow-dirty")

			c.Assert(err, qt.IsNotNil)
			c.Assert(out, qt.Matches, `(?s).*migration 20260101000000 `+regexp.QuoteMeta(sqlServerPartlyCommittedRefusal)+`.*`)
			var rows string
			c.Assert(db.QueryRowContext(ctx, logRows).Scan(&rows), qt.IsNil)
			c.Assert(rows, qt.Equals, "a")
			c.Assert(sqlServerRevisionCounts(c, ctx, db, "atlas_schema_revisions"), qt.Equals, "1/3")
		})
	}
}

// sqlServerRolledBackBlock fails inside a block that a file transaction wraps.
// The rollback undoes the INSERT of 'a', so a retry from the block writes each
// row once and is not refused.
const sqlServerRolledBackBlock = "BEGIN\n  INSERT INTO dbo.log VALUES (N'a');\n  INSERT INTO dbo.missing VALUES (1);\nEND;\n" +
	"INSERT INTO dbo.log VALUES (N'after');\n"

// TestMigrationsUpRerunsASQLServerBlockATransactionRolledBackLiveE2E is the
// control for the refusal above: under the file transaction the same block
// runs again on a retry, and every row is written once. The status hint names
// that retry.
func TestMigrationsUpRerunsASQLServerBlockATransactionRolledBackLiveE2E(t *testing.T) {
	c := qt.New(t)
	dbURL := dbtarget.URL(c, dbtarget.SQLServer)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	databaseURL, db := newSQLServerMigrationDatabase(c, ctx, dbURL, "")
	_, err := db.ExecContext(ctx, "CREATE TABLE dbo.log (msg nvarchar(10))")
	c.Assert(err, qt.IsNil)
	dir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_init.up.sql"), []byte(sqlServerRolledBackBlock), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_init.down.sql"), []byte("SELECT 1;\n"), 0o600), qt.IsNil)
	out, err := runPtahNativeWithError("migrations", "up", "--db-url", databaseURL, "--migrations-dir", dir)
	c.Assert(err, qt.IsNotNil, qt.Commentf("%s", out))
	status, _ := runPtahNativeWithError("migrations", "status", "--db-url", databaseURL, "--migrations-dir", dir)
	c.Assert(status, qt.Contains, "No statement of this migration reached the database.")
	_, err = db.ExecContext(ctx, "CREATE TABLE dbo.missing (id int)")
	c.Assert(err, qt.IsNil)

	out, err = runPtahNativeWithError("migrations", "up", "--db-url", databaseURL, "--migrations-dir", dir, "--allow-dirty")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	var rows string
	c.Assert(db.QueryRowContext(ctx, logRows).Scan(&rows), qt.IsNil)
	c.Assert(rows, qt.Equals, "a,after")
}

// TestCompatMigrateApplyRerunsASQLServerBlockATransactionRolledBackLiveE2E is
// the same control through `ptah-compat migrate apply`.
func TestCompatMigrateApplyRerunsASQLServerBlockATransactionRolledBackLiveE2E(t *testing.T) {
	c := qt.New(t)
	dbURL := dbtarget.URL(c, dbtarget.SQLServer)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	databaseURL, db := newSQLServerMigrationDatabase(c, ctx, dbURL, "")
	_, err := db.ExecContext(ctx, "CREATE TABLE dbo.log (msg nvarchar(10))")
	c.Assert(err, qt.IsNil)
	dir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "20260101000000_init.sql"), []byte(sqlServerRolledBackBlock), 0o600), qt.IsNil)
	out, err := runCompatVerb("migrate", "hash", "--dir", "file://"+filepath.ToSlash(dir))
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	out, err = runCompatVerb("migrate", "apply", "--url", databaseURL, "--dir", "file://"+filepath.ToSlash(dir))
	c.Assert(err, qt.IsNotNil, qt.Commentf("%s", out))
	_, err = db.ExecContext(ctx, "CREATE TABLE dbo.missing (id int)")
	c.Assert(err, qt.IsNil)

	out, err = runCompatVerb("migrate", "apply", "--url", databaseURL, "--dir", "file://"+filepath.ToSlash(dir), "--allow-dirty")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	var rows string
	c.Assert(db.QueryRowContext(ctx, logRows).Scan(&rows), qt.IsNil)
	c.Assert(rows, qt.Equals, "a,after")
}

// sqlServerRevisionCounts reads the applied and total counts of the one
// revision in table.
func sqlServerRevisionCounts(c *qt.C, ctx context.Context, db *sql.DB, table string) string {
	c.Helper()
	var applied, total int
	// #nosec G202 -- table is one of the two revision table names the tests pass.
	c.Assert(db.QueryRowContext(ctx, "SELECT applied, total FROM "+table).Scan(&applied, &total), qt.IsNil)
	return fmt.Sprintf("%d/%d", applied, total)
}
