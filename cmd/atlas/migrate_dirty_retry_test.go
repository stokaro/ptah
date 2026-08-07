package atlas_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/migratesum"
	"go.5x5.cz/ptah/migration/migrator"
)

// The compat surface half of stokaro/ptah#966: `migrate apply` registers
// --allow-dirty (and no --skip-checks, matching Atlas), but after a migration
// body failed the flag could not be used — the retry re-inserted the same
// version and died on the revision table's UNIQUE constraint — and `migrate
// status` reported the half-applied version as Current Version with no sign
// that anything was wrong.

const dirtyRetryVersionOne = "20240301000001"
const dirtyRetryVersionTwo = "20240301000002"

// writeDirtyRetryFixture writes a two-migration Atlas directory whose second
// migration's body fails on its second statement, hashed so the apply integrity
// gate lets it through.
func writeDirtyRetryFixture(c *qt.C, secondBody string) (migrationsDir, dbPath string) {
	c.Helper()
	root := c.TempDir()
	migrationsDir = filepath.Join(root, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0o755), qt.IsNil)
	writeDirtyRetrySecond(c, migrationsDir, secondBody)
	c.Assert(os.WriteFile(
		filepath.Join(migrationsDir, dirtyRetryVersionOne+"_one.sql"),
		[]byte("CREATE TABLE dr_one (id INTEGER PRIMARY KEY);\n"),
		0o600,
	), qt.IsNil)
	_, err := migratesum.WriteWithFormat(migrationsDir, migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)
	return migrationsDir, filepath.Join(root, "dirty.db")
}

// writeDirtyRetrySecond rewrites the second migration and re-hashes the
// directory, modeling the operator fixing the migration between runs.
func writeDirtyRetrySecond(c *qt.C, migrationsDir, body string) {
	c.Helper()
	c.Assert(os.WriteFile(
		filepath.Join(migrationsDir, dirtyRetryVersionTwo+"_two.sql"),
		[]byte(body),
		0o600,
	), qt.IsNil)
	_, err := migratesum.WriteWithFormat(migrationsDir, migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)
}

const dirtyRetryFailingBody = `CREATE TABLE dr_two (id INTEGER PRIMARY KEY);
THIS IS A FAILING STATEMENT;
`

const dirtyRetryFixedBody = `CREATE TABLE dr_two (id INTEGER PRIMARY KEY);
CREATE TABLE dr_three (id INTEGER PRIMARY KEY);
`

// TestCompatCommand_MigrateApplyAllowDirtyRecoversAfterBodyFailure is the
// compat-surface reproduction from #966, end to end through the CLI.
//
// Reverted, the final apply fails with "failed to record pending migration
// 20240301000002: ... UNIQUE constraint failed: atlas_schema_revisions.version"
// and dr_three is never created.
func TestCompatCommand_MigrateApplyAllowDirtyRecoversAfterBodyFailure(t *testing.T) {
	c := qt.New(t)
	migrationsDir, dbPath := writeDirtyRetryFixture(c, dirtyRetryFailingBody)

	_, _, err := runCompatStreams(c,
		"migrate", "apply",
		"--url", "sqlite://"+dbPath,
		"--dir", "file://"+migrationsDir,
	)
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "failed to apply migration "+dirtyRetryVersionTwo)
	c.Assert(compatTableNames(c, dbPath), qt.Not(qt.Contains), "dr_two")

	// Without the flag the dirty guard still refuses, so --allow-dirty is doing
	// the work rather than the guard having quietly disappeared.
	writeDirtyRetrySecond(c, migrationsDir, dirtyRetryFixedBody)
	_, _, guardErr := runCompatStreams(c,
		"migrate", "apply",
		"--url", "sqlite://"+dbPath,
		"--dir", "file://"+migrationsDir,
	)
	c.Assert(guardErr, qt.IsNotNil)
	c.Assert(guardErr.Error(), qt.Contains, "is dirty")

	stdout, _, retryErr := runCompatStreams(c,
		"migrate", "apply",
		"--url", "sqlite://"+dbPath,
		"--dir", "file://"+migrationsDir,
		"--allow-dirty",
	)
	c.Assert(retryErr, qt.IsNil)
	c.Assert(stdout, qt.Contains, "Migration complete. Current version: "+dirtyRetryVersionTwo)
	c.Assert(compatTableNames(c, dbPath), qt.Contains, "dr_two")
	c.Assert(compatTableNames(c, dbPath), qt.Contains, "dr_three")
	// One row per version: the retry reused the dirty row instead of adding one.
	c.Assert(
		sqliteAtlasRevisionVersions(c, dbPath),
		qt.DeepEquals,
		[]string{dirtyRetryVersionOne, dirtyRetryVersionTwo},
	)

	statusOut, _, statusErr := runCompatStreams(c,
		"migrate", "status",
		"--url", "sqlite://"+dbPath,
		"--dir", "file://"+migrationsDir,
	)
	c.Assert(statusErr, qt.IsNil)
	c.Assert(statusOut, qt.Contains, "Migration Status: OK")
	c.Assert(statusOut, qt.Not(qt.Contains), "Last migration attempt had errors:")
	c.Assert(statusOut, qt.Not(qt.Contains), "partially")
}

// TestCompatCommand_DirtyGuardRefusalLeavesTheDatabaseWritable isolates the
// connection leak the recovery path used to walk into: the dirty guard's own
// query left an unscanned *sql.Row on the Atlas revision layout, pinning its
// connection and its SQLite read lock for the rest of the process.
//
// Reverted, `migrate set` after the refusal fails with "failed to commit Atlas
// revision set transaction: database is locked (5) (SQLITE_BUSY)" — and so does
// every other write, including the --allow-dirty retry this issue is about.
func TestCompatCommand_DirtyGuardRefusalLeavesTheDatabaseWritable(t *testing.T) {
	c := qt.New(t)
	migrationsDir, dbPath := writeDirtyRetryFixture(c, dirtyRetryFailingBody)

	_, _, err := runCompatStreams(c,
		"migrate", "apply",
		"--url", "sqlite://"+dbPath,
		"--dir", "file://"+migrationsDir,
	)
	c.Assert(err, qt.IsNotNil)

	// The refusal itself: this is the run that leaked.
	_, _, guardErr := runCompatStreams(c,
		"migrate", "apply",
		"--url", "sqlite://"+dbPath,
		"--dir", "file://"+migrationsDir,
	)
	c.Assert(guardErr, qt.IsNotNil)
	c.Assert(guardErr.Error(), qt.Contains, "is dirty")

	// Any write afterwards, in the same process, must still go through.
	_, _, setErr := runCompatStreams(c,
		"migrate", "set", dirtyRetryVersionOne,
		"--url", "sqlite://"+dbPath,
		"--dir", "file://"+migrationsDir,
	)
	c.Assert(setErr, qt.IsNil)
	c.Assert(sqliteAtlasRevisionVersions(c, dbPath), qt.DeepEquals, []string{dirtyRetryVersionOne})
}

// TestCompatCommand_MigrateStatusReportsTheDirtyMigration covers the reporting
// half: while a version is half-applied, status has to say so.
//
// Reverted to the pre-#1102 block, every assertion below goes red: the output
// is `=== MIGRATION STATUS ===` / `Current Version: 20240301000002` /
// `Dirty Migration: version=20240301000002 applied=0/2` / `Error Statement:` /
// `Error:` / `Status: Pending migrations available`. It carries the same facts
// under names no Atlas-shaped parser reads. Reverted further, to before #966,
// the facts are gone too: the failed version is named as Current Version and
// nothing else says the attempt failed.
func TestCompatCommand_MigrateStatusReportsTheDirtyMigration(t *testing.T) {
	c := qt.New(t)
	migrationsDir, dbPath := writeDirtyRetryFixture(c, dirtyRetryFailingBody)

	_, _, err := runCompatStreams(c,
		"migrate", "apply",
		"--url", "sqlite://"+dbPath,
		"--dir", "file://"+migrationsDir,
	)
	c.Assert(err, qt.IsNotNil)

	statusOut, _, statusErr := runCompatStreams(c,
		"migrate", "status",
		"--url", "sqlite://"+dbPath,
		"--dir", "file://"+migrationsDir,
	)

	c.Assert(statusErr, qt.IsNil)
	c.Assert(statusOut, qt.Contains, "Migration Status: PENDING\n")
	c.Assert(statusOut, qt.Contains, "  -- Current Version: "+dirtyRetryVersionTwo+" (0 statements applied)\n")
	c.Assert(statusOut, qt.Contains, "  -- Next Version:    "+dirtyRetryVersionTwo+" (2 statements left)\n")
	c.Assert(statusOut, qt.Contains, "  -- Executed Files:  2 (last one partially)\n")
	c.Assert(statusOut, qt.Contains, "  -- Pending Files:   1\n")
	c.Assert(statusOut, qt.Contains, "\nLast migration attempt had errors:\n")
	// Both lines are the shape the pinned community binary v1.3.0 prints: the
	// statement with its terminator, and the database's own message with no
	// Ptah wrapping in front of it (stokaro/ptah#1196). The negative assertion
	// is what keeps the wrapping from creeping back.
	c.Assert(statusOut, qt.Contains, "  -- SQL:   THIS IS A FAILING STATEMENT;")
	c.Assert(statusOut, qt.Not(qt.Contains), "failed to execute migration SQL")
	c.Assert(statusOut, qt.Contains, "  -- ERROR: ")
}
