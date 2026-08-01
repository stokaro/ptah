package migrateup_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/cmd/migratestatus"
	"github.com/stokaro/ptah/cmd/migratevalidate"
	"github.com/stokaro/ptah/internal/migratesum"
	"github.com/stokaro/ptah/migration/migrator"
)

// These tests pin the apply-time integrity gate from stokaro/ptah#955 and the
// Atlas checkpoint semantics from stokaro/ptah#954 on the native
// `ptah migrations up` surface: a hashed directory (ptah.sum or atlas.sum)
// always verifies before anything executes, an unhashed directory keeps its
// ungated behavior, and Atlas-format checkpoint directories bootstrap or skip
// exactly like measured Atlas.

// nativeAtlasPreCheckpointFiles is the pre-checkpoint half of the measured
// Atlas fixture layout from stokaro/ptah#954.
func nativeAtlasPreCheckpointFiles() map[string]string {
	return map[string]string{
		"20250801000001_create_users.sql": "-- create \"users\" table\nCREATE TABLE `users` (\n  `id` integer NOT NULL PRIMARY KEY AUTOINCREMENT,\n  `name` text NOT NULL\n);\n",
		"20250801000002_add_email.sql":    "-- add \"email\" column to \"users\"\nALTER TABLE `users` ADD COLUMN `email` text NULL;\n",
	}
}

func writeHashedAtlasDirFiles(c *qt.C, files map[string]string) string {
	c.Helper()
	dir := c.TempDir()
	for name, content := range files {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600), qt.IsNil)
	}
	_, err := migratesum.WriteWithFormat(dir, migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)
	return dir
}

// writeNativeAtlasCheckpointDir writes the measured Atlas fixture layout from
// stokaro/ptah#954 and hashes it with atlas.sum.
func writeNativeAtlasCheckpointDir(c *qt.C) string {
	c.Helper()
	files := nativeAtlasPreCheckpointFiles()
	files["20260801100335_checkpoint.sql"] = "-- atlas:checkpoint\n\n-- Create \"users\" table\nCREATE TABLE `users` (\n  `id` integer NOT NULL PRIMARY KEY AUTOINCREMENT,\n  `name` text NOT NULL,\n  `email` text NULL\n);\n"
	return writeHashedAtlasDirFiles(c, files)
}

func writeHashedPtahDir(c *qt.C) string {
	c.Helper()
	dir := c.TempDir()
	files := map[string]string{
		"0000000001_users.up.sql":   "CREATE TABLE users (id INTEGER PRIMARY KEY);\n",
		"0000000001_users.down.sql": "DROP TABLE users;\n",
	}
	for name, content := range files {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600), qt.IsNil)
	}
	_, err := migratesum.WriteWithFormat(dir, migrator.MigrationDirFormatPtah)
	c.Assert(err, qt.IsNil)
	return dir
}

func tamperFile(c *qt.C, path string) {
	c.Helper()
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	c.Assert(err, qt.IsNil)
	_, err = file.WriteString("\n-- tampered comment, sum not rehashed\n")
	c.Assert(err, qt.IsNil)
	c.Assert(file.Close(), qt.IsNil)
}

func runNativeValidate(dir string) (combined string, err error) {
	cmd := migratevalidate.NewMigrateValidateCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"--dir", dir})
	err = cmd.Execute()
	return out.String(), err
}

func runNativeStatus(dbPath, dir string) (combined string, err error) {
	cmd := migratestatus.NewMigrateStatusCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"--db-url", "sqlite://" + dbPath, "--migrations-dir", dir})
	err = cmd.Execute()
	return out.String(), err
}

func TestMigrateUp_TamperedHashedPtahDirRefusesBeforeExecution(t *testing.T) {
	c := qt.New(t)
	dir := writeHashedPtahDir(c)
	tamperFile(c, filepath.Join(dir, "0000000001_users.up.sql"))
	dbPath := filepath.Join(c.TempDir(), "tamper.db")

	out, err := runUp("--db-url", "sqlite://"+dbPath, "--migrations-dir", dir)

	c.Assert(err, qt.IsNotNil, qt.Commentf("%s", out))
	c.Assert(err.Error(), qt.Contains, "migration sum verification failed")
	c.Assert(err.Error(), qt.Contains, "changed: 0000000001_users.up.sql")
	// The gate ran before the database connection opened: the SQLite file was
	// never created, so no tables and no revision rows exist.
	_, statErr := os.Stat(dbPath)
	c.Assert(os.IsNotExist(statErr), qt.IsTrue)
}

func TestMigrateUp_TamperedHashedAtlasDirRefusesBeforeExecution(t *testing.T) {
	c := qt.New(t)
	dir := writeNativeAtlasCheckpointDir(c)
	// The tampered file is the checkpoint itself: the gate must run before
	// checkpoint selection reads anything, so the tampered checkpoint can
	// never execute.
	tamperFile(c, filepath.Join(dir, "20260801100335_checkpoint.sql"))
	dbPath := filepath.Join(c.TempDir(), "tamper.db")

	out, err := runUp("--db-url", "sqlite://"+dbPath, "--migrations-dir", dir)

	c.Assert(err, qt.IsNotNil, qt.Commentf("%s", out))
	c.Assert(err.Error(), qt.Contains, "migration sum verification failed")
	c.Assert(err.Error(), qt.Contains, "changed: 20260801100335_checkpoint.sql")
	_, statErr := os.Stat(dbPath)
	c.Assert(os.IsNotExist(statErr), qt.IsTrue)
}

func TestMigrateUp_TamperedDirErrorMatchesValidateReport(t *testing.T) {
	c := qt.New(t)
	dir := writeHashedPtahDir(c)
	tamperFile(c, filepath.Join(dir, "0000000001_users.up.sql"))

	validateOut, validateErr := runNativeValidate(dir)
	_, upErr := runUp("--db-url", "sqlite://"+filepath.Join(c.TempDir(), "parity.db"), "--migrations-dir", dir)

	// Apply-time refusal reports the same drift description `ptah migrations
	// validate` prints for the same directory.
	c.Assert(validateErr, qt.IsNotNil)
	c.Assert(upErr, qt.IsNotNil)
	c.Assert(validateOut, qt.Contains, "migration directory does not match ptah.sum:")
	c.Assert(validateOut, qt.Contains, "changed: 0000000001_users.up.sql")
	c.Assert(upErr.Error(), qt.Contains, "migration directory does not match ptah.sum:")
	c.Assert(upErr.Error(), qt.Contains, "changed: 0000000001_users.up.sql")
}

func TestMigrateUp_UnhashedDirStaysUngated(t *testing.T) {
	c := qt.New(t)
	// No sum file at all: the directory was never hashed, so the gate does not
	// demand one and the run applies exactly as before.
	migrationsDir := writeUpMigrations(t)
	dbPath := filepath.Join(c.TempDir(), "unhashed.db")

	out, err := runUp("--db-url", "sqlite://"+dbPath, "--migrations-dir", migrationsDir)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(queryCurrentVersion(c, dbPath), qt.Equals, int64(2))
}

func TestMigrateUp_ValidHashedPtahDirApplies(t *testing.T) {
	c := qt.New(t)
	dir := writeHashedPtahDir(c)
	dbPath := filepath.Join(c.TempDir(), "valid.db")

	out, err := runUp("--db-url", "sqlite://"+dbPath, "--migrations-dir", dir)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(queryCurrentVersion(c, dbPath), qt.Equals, int64(1))
}

func TestMigrateUp_AtlasCheckpointFreshDatabase(t *testing.T) {
	c := qt.New(t)
	dir := writeNativeAtlasCheckpointDir(c)
	dbPath := filepath.Join(c.TempDir(), "fresh.db")

	out, err := runUp("--db-url", "sqlite://"+dbPath, "--migrations-dir", dir)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	// Only the checkpoint runs on a fresh database; the squashed
	// pre-checkpoint files are not replayed (measured Atlas semantics,
	// stokaro/ptah#954).
	c.Assert(out, qt.Contains, "Pending migrations: 1")
	c.Assert(out, qt.Contains, "Database is now at version: 20260801100335")
	c.Assert(queryCurrentVersion(c, dbPath), qt.Equals, int64(20260801100335))

	statusOut, err := runNativeStatus(dbPath, dir)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", statusOut))
	c.Assert(statusOut, qt.Contains, "Pending Migrations: 0")
	c.Assert(statusOut, qt.Contains, "Database is up to date")
}

func TestMigrateUp_AtlasCheckpointPreCheckpointDatabaseSkips(t *testing.T) {
	c := qt.New(t)
	fullDir := writeNativeAtlasCheckpointDir(c)

	// Seed through ordinary pre-checkpoint history from a directory that does
	// not contain the checkpoint yet.
	preDir := writeHashedAtlasDirFiles(c, nativeAtlasPreCheckpointFiles())
	dbPath := filepath.Join(c.TempDir(), "pre.db")
	out, err := runUp("--db-url", "sqlite://"+dbPath, "--migrations-dir", preDir)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(queryCurrentVersion(c, dbPath), qt.Equals, int64(20250801000002))

	// The checkpointed directory is silently skipped: no pending work, no new
	// bookkeeping row, clean status (measured Atlas prints "No migration files
	// to execute" here).
	out, err = runUp("--db-url", "sqlite://"+dbPath, "--migrations-dir", fullDir)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "Pending migrations: 0")
	c.Assert(out, qt.Contains, "Database is already up to date!")
	c.Assert(queryCurrentVersion(c, dbPath), qt.Equals, int64(20250801000002))

	statusOut, err := runNativeStatus(dbPath, fullDir)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", statusOut))
	c.Assert(statusOut, qt.Contains, "Pending Migrations: 0")
	c.Assert(statusOut, qt.Contains, "Database is up to date")
}
