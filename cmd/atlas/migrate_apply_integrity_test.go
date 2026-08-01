package atlas_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/cmd/atlas"
	"github.com/stokaro/ptah/dbschema"
)

// These tests pin the measured Atlas semantics from stokaro/ptah#954 and
// stokaro/ptah#955 on the ptah-compat surface: the `-- atlas:checkpoint`
// directive (fresh database bootstraps from the checkpoint, pre-checkpoint
// database skips it silently) and the apply-time atlas.sum integrity gate
// (a tampered hashed directory refuses before executing anything, with output
// byte-identical to `migrate validate`).

const compatCheckpointVersion = "20260801100335"

// writeCompatCheckpointDir writes the measured fixture layout: two
// pre-checkpoint migrations and a single-statement checkpoint, hashed with
// atlas.sum.
func writeCompatCheckpointDir(c *qt.C, dir string) {
	c.Helper()
	writeAtlasApplyProjectMigration(c, dir, "20250801000001_create_users.sql",
		"-- create \"users\" table\nCREATE TABLE `users` (\n  `id` integer NOT NULL PRIMARY KEY AUTOINCREMENT,\n  `name` text NOT NULL\n);\n")
	writeAtlasApplyProjectMigration(c, dir, "20250801000002_add_email.sql",
		"-- add \"email\" column to \"users\"\nALTER TABLE `users` ADD COLUMN `email` text NULL;\n")
	writeAtlasApplyProjectMigration(c, dir, "20260801100335_checkpoint.sql",
		"-- atlas:checkpoint\n\n-- Create \"users\" table\nCREATE TABLE `users` (\n  `id` integer NOT NULL PRIMARY KEY AUTOINCREMENT,\n  `name` text NOT NULL,\n  `email` text NULL\n);\n")
	writeAtlasApplyProjectSum(c, dir)
}

// writeCompatPreCheckpointDir writes only the pre-checkpoint half of the
// fixture, hashed, for seeding a database that migrated before the checkpoint
// was cut.
func writeCompatPreCheckpointDir(c *qt.C, dir string) {
	c.Helper()
	writeAtlasApplyProjectMigration(c, dir, "20250801000001_create_users.sql",
		"-- create \"users\" table\nCREATE TABLE `users` (\n  `id` integer NOT NULL PRIMARY KEY AUTOINCREMENT,\n  `name` text NOT NULL\n);\n")
	writeAtlasApplyProjectMigration(c, dir, "20250801000002_add_email.sql",
		"-- add \"email\" column to \"users\"\nALTER TABLE `users` ADD COLUMN `email` text NULL;\n")
	writeAtlasApplyProjectSum(c, dir)
}

func runCompat(args ...string) (stdout, stderr string, err error) {
	cmd := atlas.NewCompatCommand("atlas")
	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs(args)
	err = cmd.Execute()
	return out.String(), errOut.String(), err
}

func compatApply(dir, dbPath string) (stdout, stderr string, err error) {
	return runCompat("migrate", "apply", "--url", "sqlite://"+dbPath, "--dir", "file://"+dir)
}

type compatRevisionRow struct {
	Version     string
	Description string
	Type        int64
	Applied     int64
	Total       int64
}

func compatRevisionRows(c *qt.C, dbPath string) []compatRevisionRow {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	rows, err := conn.Query("SELECT version, description, type, applied, total FROM atlas_schema_revisions ORDER BY version")
	c.Assert(err, qt.IsNil)
	defer rows.Close()
	var revisions []compatRevisionRow
	for rows.Next() {
		var row compatRevisionRow
		c.Assert(rows.Scan(&row.Version, &row.Description, &row.Type, &row.Applied, &row.Total), qt.IsNil)
		revisions = append(revisions, row)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return revisions
}

func TestCompatMigrateApply_CheckpointFreshDatabase(t *testing.T) {
	c := qt.New(t)
	dir := filepath.Join(c.TempDir(), "m")
	writeCompatCheckpointDir(c, dir)
	dbPath := filepath.Join(c.TempDir(), "fresh.db")

	stdout, _, err := compatApply(dir, dbPath)

	c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s", stdout))
	// Measured Atlas: "Migrating to version 20260801100335 (1 migrations in
	// total)" — only the checkpoint runs on a fresh database.
	c.Assert(stdout, qt.Contains, "Migrating to version "+compatCheckpointVersion+" from 1 pending migrations.")
	c.Assert(sqliteTableCount(c, dbPath, "users"), qt.Equals, 1)

	// Measured revision row: `20260801100335|checkpoint|2|1|1`, and nothing
	// recorded for the squashed pre-checkpoint files.
	c.Assert(compatRevisionRows(c, dbPath), qt.DeepEquals, []compatRevisionRow{
		{Version: compatCheckpointVersion, Description: "checkpoint", Type: 2, Applied: 1, Total: 1},
	})

	// Status agrees the database is clean.
	statusOut, _, err := runCompat("migrate", "status", "--url", "sqlite://"+dbPath, "--dir", "file://"+dir)
	c.Assert(err, qt.IsNil, qt.Commentf("status output:\n%s", statusOut))
	c.Assert(statusOut, qt.Contains, "Pending Migrations: 0")
}

func TestCompatMigrateApply_CheckpointPreCheckpointDatabaseSkips(t *testing.T) {
	c := qt.New(t)
	tempDir := c.TempDir()
	preDir := filepath.Join(tempDir, "m_pre")
	writeCompatPreCheckpointDir(c, preDir)
	fullDir := filepath.Join(tempDir, "m")
	writeCompatCheckpointDir(c, fullDir)
	dbPath := filepath.Join(tempDir, "pre.db")

	// Seed through ordinary pre-checkpoint history.
	stdout, _, err := compatApply(preDir, dbPath)
	c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s", stdout))
	seeded := compatRevisionRows(c, dbPath)
	c.Assert(seeded, qt.HasLen, 2)

	// Measured Atlas on the checkpointed directory: "No migration files to
	// execute", no new revision row, status stays OK.
	stdout, _, err = compatApply(fullDir, dbPath)
	c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s", stdout))
	c.Assert(stdout, qt.Contains, "No migration files to execute.")
	c.Assert(compatRevisionRows(c, dbPath), qt.DeepEquals, seeded)

	statusOut, _, err := runCompat("migrate", "status", "--url", "sqlite://"+dbPath, "--dir", "file://"+fullDir)
	c.Assert(err, qt.IsNil, qt.Commentf("status output:\n%s", statusOut))
	c.Assert(statusOut, qt.Contains, "Pending Migrations: 0")
	c.Assert(statusOut, qt.Contains, "Database is up to date")
}

func tamperCompatCheckpointFile(c *qt.C, dir string) {
	c.Helper()
	path := filepath.Join(dir, "20260801100335_checkpoint.sql")
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	c.Assert(err, qt.IsNil)
	_, err = file.WriteString("\n-- tampered comment, sum not rehashed\n")
	c.Assert(err, qt.IsNil)
	c.Assert(file.Close(), qt.IsNil)
}

func TestCompatMigrateApply_TamperedDirRefusesBeforeExecution(t *testing.T) {
	c := qt.New(t)
	tempDir := c.TempDir()
	dir := filepath.Join(tempDir, "m_tampered")
	writeCompatCheckpointDir(c, dir)
	tamperCompatCheckpointFile(c, dir)
	dbPath := filepath.Join(tempDir, "tamper.db")

	stdout, stderr, err := compatApply(dir, dbPath)

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Equals, "checksum mismatch")
	// Measured Atlas output shape (stokaro/ptah#955): the guidance block with
	// the L<line> pointer on stdout and the error line on stderr.
	c.Assert(stdout, qt.Equals, "You have a checksum error in your migration directory.\n"+
		"\n\tL4: 20260801100335_checkpoint.sql was edited\n\n"+
		"Please check your migration files and run 'atlas migrate hash' to re-hash the contents\n\n")
	c.Assert(stderr, qt.Equals, "Error: checksum mismatch\n")

	// The gate ran before anything touched the database: the SQLite file was
	// never even created, so no tables and no revision rows exist.
	_, statErr := os.Stat(dbPath)
	c.Assert(os.IsNotExist(statErr), qt.IsTrue)
}

func TestCompatMigrateApply_TamperedDirMatchesValidateOutput(t *testing.T) {
	c := qt.New(t)
	tempDir := c.TempDir()
	dir := filepath.Join(tempDir, "m_tampered")
	writeCompatCheckpointDir(c, dir)
	tamperCompatCheckpointFile(c, dir)

	applyOut, applyErrOut, applyErr := compatApply(dir, filepath.Join(tempDir, "tamper.db"))
	validateOut, validateErrOut, validateErr := runCompat("migrate", "validate", "--dir", "file://"+dir)

	// Apply refuses with output byte-identical to validate on the same
	// directory: same stdout guidance, same stderr error, same error value.
	c.Assert(applyErr, qt.IsNotNil)
	c.Assert(validateErr, qt.IsNotNil)
	c.Assert(applyOut, qt.Equals, validateOut)
	c.Assert(applyErrOut, qt.Equals, validateErrOut)
	c.Assert(applyErr.Error(), qt.Equals, validateErr.Error())
}

func TestCompatMigrateApply_UnhashedDirStaysUngated(t *testing.T) {
	c := qt.New(t)
	tempDir := c.TempDir()
	dir := filepath.Join(tempDir, "m_unhashed")
	// No atlas.sum: the directory was never hashed, so the integrity gate does
	// not apply and the directory applies exactly as before.
	writeAtlasApplyProjectMigration(c, dir, "1_create_widgets.sql",
		"CREATE TABLE widgets (id INTEGER PRIMARY KEY);\n")
	dbPath := filepath.Join(tempDir, "unhashed.db")

	stdout, _, err := compatApply(dir, dbPath)

	c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s", stdout))
	c.Assert(sqliteTableCount(c, dbPath, "widgets"), qt.Equals, 1)
}

func TestCompatMigrateApply_ValidHashedDirApplies(t *testing.T) {
	c := qt.New(t)
	tempDir := c.TempDir()
	dir := filepath.Join(tempDir, "m_valid")
	writeCompatCheckpointDir(c, dir)
	dbPath := filepath.Join(tempDir, "valid.db")

	stdout, _, err := compatApply(dir, dbPath)

	c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s", stdout))
	c.Assert(sqliteTableCount(c, dbPath, "users"), qt.Equals, 1)
}
