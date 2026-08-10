package atlas_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/migratesum"
	"go.5x5.cz/ptah/migration/migrator"
)

// The three cells of stokaro/ptah#1241 that are retained divergences rather
// than cells to close, plus the one that closed itself and had nothing holding
// it.
//
// Every expectation here was measured against the pinned community binary
// v1.3.0 on 2026-08-09, each exit status read on its own line rather than
// through a pipe. SQLite covers every row below. The edited-file and tx-mode
// rows were re-run on PostgreSQL 17.10; trailing positional arguments and the
// out-of-order insert remain SQLite-only. The binary's side of each row is
// stated in the comment above it, because a divergence nobody re-measured is a
// claim rather than a decision.
//
//   - item 6, an already-applied file whose bytes changed: binary 0
//     ("No migration files to execute"), ptah 1. Retained; see
//     "An edited already-applied migration file" in the Atlas comparison page.
//   - item 5, a migration inserted below the applied high-water mark: binary 0,
//     ptah 1. Retained as a refusal, but the diagnostic it prints is recorded
//     as an open product-behavior gap, so this file pins the refusal and not
//     the wording.
//   - item 1 under the default --tx-mode: binary 0 on the retry, and ptah now
//     agrees. Closed incidentally by #1342, which never referenced #1241 and
//     left no test tied to it. The first test below is that missing guard.
//   - item 1 under --tx-mode none: binary 0 on the retry, ptah 1. Retained,
//     because "applied=0" is not proof that nothing happened when no
//     transaction wrapped the body.

const retainedVersionEarly = "20240101000000"
const retainedVersionLate = "20240102000000"

// writeRetainedDir writes an Atlas migration directory holding exactly the
// given files and hashes it, so the apply integrity gate is not what answers.
//
// The per-file entry an integrity file records is chained over the entries
// before it, so the same bytes hash differently depending on what precedes
// them in the directory. That is the mechanism behind item 5 and the reason
// these fixtures are built as whole directories rather than by editing one.
func writeRetainedDir(c *qt.C, dir string, files map[string]string) string {
	c.Helper()
	c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
	for name, body := range files {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600), qt.IsNil)
	}
	_, err := migratesum.WriteWithFormat(dir, migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)
	return dir
}

const retainedEarlyBody = "CREATE TABLE rt_early (id INTEGER PRIMARY KEY);\n"
const retainedLateBody = "CREATE TABLE rt_late (id INTEGER PRIMARY KEY);\n"

// retainedEditedBody is retainedEarlyBody with one column added: a real content
// change to a file that already ran, which is what separates item 6 from item 5.
const retainedEditedBody = "CREATE TABLE rt_early (id INTEGER PRIMARY KEY, note TEXT);\n"

// TestCompatCommand_AnEditedAppliedFileIsRefused pins item 6.
//
// The pinned community binary v1.3.0 records the same checksum and never
// compares it, so applying the edited directory is exit 0 and prints
// "No migration files to execute". Ptah compares it and refuses.
//
// Mutated so the stored and current checksums are compared by length instead of
// by value, this test fails and the run is accepted as a no-op.
func TestCompatCommand_AnEditedAppliedFileIsRefused(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	dbPath := filepath.Join(root, "edited.db")
	applied := writeRetainedDir(c, filepath.Join(root, "applied"), map[string]string{
		retainedVersionEarly + "_early.sql": retainedEarlyBody,
	})
	edited := writeRetainedDir(c, filepath.Join(root, "edited"), map[string]string{
		retainedVersionEarly + "_early.sql": retainedEditedBody,
	})

	_, _, err := runCompatStreams(c,
		"migrate", "apply",
		"--url", "sqlite://"+dbPath,
		"--dir", "file://"+applied,
	)
	c.Assert(err, qt.IsNil)
	c.Assert(compatTableNames(c, dbPath), qt.Contains, "rt_early")

	_, _, editedErr := runCompatStreams(c,
		"migrate", "apply",
		"--url", "sqlite://"+dbPath,
		"--dir", "file://"+edited,
	)
	c.Assert(editedErr, qt.IsNotNil)
	c.Assert(editedErr.Error(), qt.Contains, "checksum mismatch")
	// The refusal names the version whose bytes actually changed. That is the
	// half of this gate whose diagnostic is true; see the item 5 test below for
	// the half whose diagnostic is not.
	c.Assert(editedErr.Error(), qt.Contains, retainedVersionEarly)
}

// TestCompatCommand_AnOutOfOrderInsertIsRefused pins item 5.
//
// The late migration is byte-identical in both directories — only its position
// differs — and the pinned community binary v1.3.0 exits 0 with
// "No migration files to execute", leaving the early migration unapplied. Ptah
// refuses.
//
// Two independent gates refuse this, and which one answers is the open gap.
// Measured by mutating the checksum comparison to compare lengths instead of
// values: the refusal does not disappear, it changes to
//
//	out-of-order pending migrations below current version 20240102000000:
//	[20240101000000] (use --exec-order=non-linear to apply or
//	--exec-order=linear-skip to ignore)
//
// which is true about what happened and names the flag that resolves it. The
// checksum gate runs first — verifyAppliedMigrationChecksums is called before
// migrationsToApply in migrateUpLocked — so what an operator actually sees is a
// checksum mismatch against the LATE migration, whose bytes did not change.
//
// The assertion below therefore records today's message on purpose, as a
// characterization row rather than as a contract. When the gap recorded in
// "A checksum refusal that names an unchanged file" is closed, this row should
// flip to the out-of-order message above; it must not be deleted, because the
// refusal itself is retained either way.
//
// Mutated so the checksums are compared by length instead of by value, the
// message assertion below fails while the refusal assertions hold.
func TestCompatCommand_AnOutOfOrderInsertIsRefused(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	dbPath := filepath.Join(root, "order.db")
	lateOnly := writeRetainedDir(c, filepath.Join(root, "late"), map[string]string{
		retainedVersionLate + "_late.sql": retainedLateBody,
	})
	bothFiles := writeRetainedDir(c, filepath.Join(root, "both"), map[string]string{
		retainedVersionEarly + "_early.sql": retainedEarlyBody,
		retainedVersionLate + "_late.sql":   retainedLateBody,
	})

	_, _, err := runCompatStreams(c,
		"migrate", "apply",
		"--url", "sqlite://"+dbPath,
		"--dir", "file://"+lateOnly,
	)
	c.Assert(err, qt.IsNil)
	c.Assert(compatTableNames(c, dbPath), qt.Contains, "rt_late")

	_, _, insertErr := runCompatStreams(c,
		"migrate", "apply",
		"--url", "sqlite://"+dbPath,
		"--dir", "file://"+bothFiles,
	)
	c.Assert(insertErr, qt.IsNotNil)
	c.Assert(insertErr.Error(), qt.Contains, "checksum mismatch")
	c.Assert(insertErr.Error(), qt.Contains, retainedVersionLate)
	c.Assert(compatTableNames(c, dbPath), qt.Not(qt.Contains), "rt_early")
	c.Assert(sqliteAtlasRevisionVersions(c, dbPath), qt.DeepEquals, []string{retainedVersionLate})
}

// TestCompatCommand_LintRefusesATrailingPositional pins the fourth verb of
// #1241 item 13, and the one the existing table in compat_overstrict_test.go
// does not reach.
//
// That table pins `migrate status`, `migrate validate` and `schema inspect`.
// Measured 2026-08-09 against the pinned community binary v1.3.0 with a hashed
// ./migrations present, `migrate lint --dir file://migrations --dev-url …
// --latest 1 trailingarg` exits 0 and prints the report, discarding the
// positional. Ptah refuses and names the flag the value belongs on.
//
// `migrate hash` refuses on the same shared helper. Its oracle cell is not
// measured here: the sandbox this was run in refuses any command containing
// that bare word, so no reading of that binary was taken for it.
//
// Mutated so NoPositionalArgsHint refuses only more than one positional, this
// test fails, and so do the three rows in the other table.
func TestCompatCommand_LintRefusesATrailingPositional(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	migrationsDir := writeRetainedDir(c, filepath.Join(root, "migrations"), map[string]string{
		retainedVersionEarly + "_early.sql": retainedEarlyBody,
	})

	// The dev database is named under the temporary root rather than relatively:
	// this test does not chdir, so a relative sqlite URL would leave the file in
	// the package directory and the next run would find it there.
	_, _, err := runCompatStreams(c,
		"migrate", "lint",
		"--dir", "file://"+migrationsDir,
		"--dev-url", "sqlite://"+filepath.Join(root, "dev.db"),
		"--latest", "1",
		"stray",
	)
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, `unexpected positional arguments ["stray"]`)
	c.Assert(err.Error(), qt.Contains, "name the migration directory with --dir")
}

// retainedZeroProgressBody fails on its FIRST statement, so the attempt commits
// nothing and the revision row it leaves reads applied=0/1. A body that fails
// on a later statement is a different case and keeps the gate; see
// dirtyRetryFailingBody, whose second statement is the failing one.
const retainedZeroProgressBody = "THIS IS A FAILING STATEMENT;\n"

// writeZeroProgressFixture writes a two-migration directory whose second
// migration fails on its first statement.
func writeZeroProgressFixture(c *qt.C) (migrationsDir, dbPath string) {
	c.Helper()
	root := c.TempDir()
	migrationsDir = writeRetainedDir(c, filepath.Join(root, "migrations"), map[string]string{
		retainedVersionEarly + "_early.sql": retainedEarlyBody,
		retainedVersionLate + "_late.sql":   retainedZeroProgressBody,
	})
	return migrationsDir, filepath.Join(root, "zero.db")
}

// TestCompatCommand_AZeroProgressFailureLeavesNoDirtyRowUnderTheDefaultTxMode
// is the guard #1342 never got.
//
// It closed item 1's headline case — the fix-and-rerun flow the register called
// permanently wedged — by discarding the revision row of an up attempt whose
// transaction rolled back with nothing committed. Nothing tied that to #1241,
// so this pins it: after a zero-progress failure under the default transaction
// mode, no dirty row survives and the next apply is not refused for dirtiness.
//
// Mutated so discardRolledBackFailure returns before deleting the row, this
// test fails on the second apply with "is dirty".
func TestCompatCommand_AZeroProgressFailureLeavesNoDirtyRowUnderTheDefaultTxMode(t *testing.T) {
	c := qt.New(t)
	migrationsDir, dbPath := writeZeroProgressFixture(c)

	_, _, err := runCompatStreams(c,
		"migrate", "apply",
		"--url", "sqlite://"+dbPath,
		"--dir", "file://"+migrationsDir,
	)
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, retainedVersionLate)
	// The early migration committed; only the failing one left nothing.
	c.Assert(compatTableNames(c, dbPath), qt.Contains, "rt_early")
	c.Assert(sqliteAtlasRevisionVersions(c, dbPath), qt.DeepEquals, []string{retainedVersionEarly})

	// The next run reaches the migration again rather than being turned away by
	// the dirty guard. It still fails, because the body is still wrong — the
	// point is which error it is.
	_, _, retryErr := runCompatStreams(c,
		"migrate", "apply",
		"--url", "sqlite://"+dbPath,
		"--dir", "file://"+migrationsDir,
	)
	c.Assert(retryErr, qt.IsNotNil)
	c.Assert(retryErr.Error(), qt.Not(qt.Contains), "is dirty")
}

// TestCompatCommand_AZeroProgressFailureUnderTxModeNoneStillRefuses pins the
// half of item 1 that is retained.
//
// The pinned community binary v1.3.0 keeps its row in this mode too and resumes
// from it, so the repaired retry is exit 0 there and exit 1 here. The row is
// kept because applied=0 is not proof that nothing happened when no transaction
// wrapped the body: --tx-mode none exists so that statements which cannot run
// inside a transaction can run, and on PostgreSQL a failed CREATE INDEX
// CONCURRENTLY leaves an invalid index behind while the statement counts as not
// applied.
//
// Mutated so discardRolledBackFailure discards whenever the revision is a
// zero-progress up failure, without requiring a confirmed rollback, this test
// fails: the second apply no longer says "is dirty".
func TestCompatCommand_AZeroProgressFailureUnderTxModeNoneStillRefuses(t *testing.T) {
	c := qt.New(t)
	migrationsDir, dbPath := writeZeroProgressFixture(c)

	_, _, err := runCompatStreams(c,
		"migrate", "apply",
		"--url", "sqlite://"+dbPath,
		"--dir", "file://"+migrationsDir,
		"--tx-mode", "none",
	)
	c.Assert(err, qt.IsNotNil)
	c.Assert(sqliteAtlasRevisionVersions(c, dbPath), qt.DeepEquals, []string{
		retainedVersionEarly,
		retainedVersionLate,
	})

	_, _, retryErr := runCompatStreams(c,
		"migrate", "apply",
		"--url", "sqlite://"+dbPath,
		"--dir", "file://"+migrationsDir,
		"--tx-mode", "none",
	)
	c.Assert(retryErr, qt.IsNotNil)
	c.Assert(retryErr.Error(), qt.Contains, "is dirty")
	c.Assert(retryErr.Error(), qt.Contains, "applied=0/1")

	// --allow-dirty is the documented way through, and it still is.
	_, _, allowErr := runCompatStreams(c,
		"migrate", "apply",
		"--url", "sqlite://"+dbPath,
		"--dir", "file://"+migrationsDir,
		"--tx-mode", "none",
		"--allow-dirty",
	)
	c.Assert(allowErr, qt.IsNotNil)
	c.Assert(allowErr.Error(), qt.Not(qt.Contains), "is dirty")
}
