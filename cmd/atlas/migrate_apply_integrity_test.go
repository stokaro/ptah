package atlas_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/atlas"
	"go.5x5.cz/ptah/dbschema"
)

// These tests pin the measured Atlas semantics from stokaro/ptah#954,
// stokaro/ptah#955, and stokaro/ptah#970 on the ptah-compat surface: the
// `-- atlas:checkpoint` directive (fresh database bootstraps from the
// checkpoint, pre-checkpoint database skips it silently) and the apply-time
// atlas.sum integrity gate (a tampered hashed directory and a directory with
// no atlas.sum both refuse before executing anything, with output
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
	c.Assert(statusOut, qt.Contains, "-- Pending Files:   0")
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
	c.Assert(stdout, qt.Contains, "No migration files to execute")
	c.Assert(compatRevisionRows(c, dbPath), qt.DeepEquals, seeded)

	statusOut, _, err := runCompat("migrate", "status", "--url", "sqlite://"+dbPath, "--dir", "file://"+fullDir)
	c.Assert(err, qt.IsNil, qt.Commentf("status output:\n%s", statusOut))
	c.Assert(statusOut, qt.Contains, "-- Pending Files:   0")
	c.Assert(statusOut, qt.Contains, "Migration Status: OK")
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

// writeCompatUnhashedDir writes a directory that was never hashed: one Atlas
// migration and no atlas.sum.
func writeCompatUnhashedDir(c *qt.C, dir string) {
	c.Helper()
	writeAtlasApplyProjectMigration(c, dir, "1_create_widgets.sql",
		"CREATE TABLE widgets (id INTEGER PRIMARY KEY);\n")
}

func TestCompatMigrateApply_UnhashedDirRefusesBeforeExecution(t *testing.T) {
	c := qt.New(t)
	tempDir := c.TempDir()
	dir := filepath.Join(tempDir, "m_unhashed")
	writeCompatUnhashedDir(c, dir)
	dbPath := filepath.Join(tempDir, "unhashed.db")

	stdout, stderr, err := compatApply(dir, dbPath)

	// Measured Atlas CE v1.2.0 on a directory with no atlas.sum
	// (stokaro/ptah#970): exit 1, the checksum guidance block without an
	// L<line> pointer, and "checksum file not found" on stderr.
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Equals, "checksum file not found")
	c.Assert(stdout, qt.Equals, "You have a checksum error in your migration directory.\n"+
		"Please check your migration files and run 'atlas migrate hash' to re-hash the contents\n\n")
	c.Assert(stderr, qt.Equals, "Error: checksum file not found\n")

	// The gate ran before the target was touched: Atlas never creates the
	// database, so neither does Ptah.
	_, statErr := os.Stat(dbPath)
	c.Assert(os.IsNotExist(statErr), qt.IsTrue)
}

func TestCompatMigrateApply_UnhashedDirRefusesDryRun(t *testing.T) {
	c := qt.New(t)
	tempDir := c.TempDir()
	dir := filepath.Join(tempDir, "m_unhashed")
	writeCompatUnhashedDir(c, dir)
	dbPath := filepath.Join(tempDir, "unhashed-dry-run.db")

	// The gate precedes the dry-run branch, so a dry run is refused too and
	// still never opens the target.
	_, stderr, err := runCompat(
		"migrate", "apply",
		"--url", "sqlite://"+dbPath,
		"--dir", "file://"+dir,
		"--dry-run",
	)

	c.Assert(err, qt.IsNotNil)
	c.Assert(stderr, qt.Equals, "Error: checksum file not found\n")
	_, statErr := os.Stat(dbPath)
	c.Assert(os.IsNotExist(statErr), qt.IsTrue)
}

func TestCompatMigrateApply_UnhashedDirMatchesValidateOutput(t *testing.T) {
	c := qt.New(t)
	tempDir := c.TempDir()
	dir := filepath.Join(tempDir, "m_unhashed")
	writeCompatUnhashedDir(c, dir)

	applyOut, applyErrOut, applyErr := compatApply(dir, filepath.Join(tempDir, "unhashed.db"))
	validateOut, validateErrOut, validateErr := runCompat("migrate", "validate", "--dir", "file://"+dir)

	// The missing-sum refusal shares one code path with validate, so apply is
	// byte-identical to `migrate validate` on the same directory — the same
	// property #962 established for the mismatch case.
	c.Assert(applyErr, qt.IsNotNil)
	c.Assert(validateErr, qt.IsNotNil)
	c.Assert(applyOut, qt.Equals, validateOut)
	c.Assert(applyErrOut, qt.Equals, validateErrOut)
	c.Assert(applyErr.Error(), qt.Equals, validateErr.Error())
}

// The converted-directory half of this gate — a directory read through
// `?format=` in a foreign tool's layout — lives in
// migrate_apply_converted_integrity_test.go. It replaced
// TestCompatMigrateApply_ConvertedDirStaysUngated_KnownDivergence, which pinned
// the gap rather than the parity (stokaro/ptah#973).

// TestCompatMigrateApply_DirWithoutSQLFilesApplies pins the measured CE
// predicate for the missing-atlas.sum refusal: it fires on the presence of any
// *.sql file, not on parseable versioned migrations. Atlas CE v1.2.0 reports
// "No migration files to execute" and exits 0 on an empty directory and on one
// holding only non-SQL files, so a CI bootstrap that creates an empty
// migrations directory keeps working (stokaro/ptah#970).
func TestCompatMigrateApply_DirWithoutSQLFilesApplies(t *testing.T) {
	tests := []struct {
		name  string
		files map[string]string
	}{
		{name: "empty directory", files: make(map[string]string)},
		{
			name:  "no SQL files",
			files: map[string]string{".gitkeep": "", "README.md": "migrations live here\n"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			tempDir := c.TempDir()
			dir := filepath.Join(tempDir, "m_no_sql")
			c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
			for name, content := range tt.files {
				c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600), qt.IsNil)
			}
			dbPath := filepath.Join(tempDir, "no-sql.db")

			stdout, stderr, err := compatApply(dir, dbPath)

			c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
			c.Assert(stdout, qt.Contains, "No migration files to execute")
		})
	}
}

// TestCompatMigrateApply_UnhashedDirWithNonVersionedSQLRefuses is the other
// half of that predicate: CE gates an unhashed directory holding `foo.sql`,
// which is not a parseable versioned migration, so the gate must key on the
// file extension rather than on the planner's view of the directory.
func TestCompatMigrateApply_UnhashedDirWithNonVersionedSQLRefuses(t *testing.T) {
	c := qt.New(t)
	tempDir := c.TempDir()
	dir := filepath.Join(tempDir, "m_foo")
	writeAtlasApplyProjectMigration(c, dir, "foo.sql", "CREATE TABLE foo (id INTEGER PRIMARY KEY);\n")
	dbPath := filepath.Join(tempDir, "foo.db")

	_, stderr, err := compatApply(dir, dbPath)

	c.Assert(err, qt.IsNotNil)
	c.Assert(stderr, qt.Equals, "Error: checksum file not found\n")
	_, statErr := os.Stat(dbPath)
	c.Assert(os.IsNotExist(statErr), qt.IsTrue)
}

// TestCompatMigrateApply_UnhashedDirWithNestedSQLIsNothingToExecute re-expresses
// what used to be TestCompatMigrateApply_UnhashedDirWithNestedSQLRefuses.
//
// That test pinned a compensator, not a behavior worth keeping: the exemption
// scan recursed because the registrar recursed, so refusing was the only way to
// stop an unhashed nested migration running unverified. Since #976 the
// registrar selects exactly the set atlas.sum covers, so a nested file is not a
// migration on either tool — the directory has nothing to execute and the
// community binary's exit 0 is now reachable without giving anything up.
//
// The post-condition is strictly stronger than the old one. Refusing only
// proved the file did not run in THIS invocation; asserting the table is absent
// after a successful apply proves it is not a migration at all. The stderr
// notice is asserted with it, because "did not run" and "nobody was told" is
// the failure mode this fix exists to avoid, not a lesser version of it.
func TestCompatMigrateApply_UnhashedDirWithNestedSQLIsNothingToExecute(t *testing.T) {
	tests := []struct {
		name  string
		files map[string]string
	}{
		{
			name:  "migration in a subdirectory",
			files: map[string]string{"sub/20260801100335_init.sql": "CREATE TABLE nested (id INTEGER PRIMARY KEY);\n"},
		},
		{
			name: "subdirectory migration beside a top-level non-SQL file",
			files: map[string]string{
				"README.md":                   "migrations live here\n",
				"sub/20260801100335_init.sql": "CREATE TABLE nested (id INTEGER PRIMARY KEY);\n",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			tempDir := c.TempDir()
			dir := filepath.Join(tempDir, "m_nested")
			for name, content := range tt.files {
				path := filepath.Join(dir, filepath.FromSlash(name))
				c.Assert(os.MkdirAll(filepath.Dir(path), 0o755), qt.IsNil)
				c.Assert(os.WriteFile(path, []byte(content), 0o600), qt.IsNil)
			}
			dbPath := filepath.Join(tempDir, "nested.db")

			stdout, stderr, err := compatApply(dir, dbPath)

			c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
			c.Assert(stdout, qt.Contains, "No migration files to execute")
			c.Assert(stderr, qt.Equals,
				"warning: sub/20260801100335_init.sql is not covered by atlas.sum and will not run; "+
					"Atlas migrations are top-level files named *.sql\n")
			c.Assert(sqliteTableCount(c, dbPath, "nested"), qt.Equals, 0)
		})
	}
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
