package atlas_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/migratesum"
	"go.5x5.cz/ptah/migration/migrator"
)

// The retained divergences from stokaro/ptah#1241, plus the checksum-chain cell
// closed here and the tx-mode cell that closed itself without a focused guard.
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
//   - item 5, a prefix migration inserted below every applied version: the
//     binary's default silently leaves it pending. Ptah refuses by default,
//     reproduces that outcome with linear-skip, and applies it with non-linear.
//     An insertion between two applied versions is a different oracle cell:
//     both binaries refuse it by default.
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

// TestCompatCommand_APrefixInsertionRequiresNonLinear pins Ptah's two explicit
// answers for the prefix-insertion shape of item 5.
//
// The late migration is byte-identical in both directories; only a migration
// before it was inserted. The default linear order still refuses with
//
//	out-of-order pending migrations below current version 20240102000000:
//	[20240101000000] (use --exec-order=non-linear to apply or
//	--exec-order=linear-skip to ignore)
//
// With --exec-order non-linear, Ptah proves the late row against the applied
// directory projection, applies the insertion, and reconciles both clean rows
// to the current Atlas chain. A second apply proves the reconciled rows are
// stable.
func TestCompatCommand_APrefixInsertionRequiresNonLinear(t *testing.T) {
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
	c.Assert(insertErr.Error(), qt.Contains, "out-of-order pending migrations")
	c.Assert(insertErr.Error(), qt.Contains, retainedVersionEarly)
	c.Assert(compatTableNames(c, dbPath), qt.Not(qt.Contains), "rt_early")
	c.Assert(sqliteAtlasRevisionVersions(c, dbPath), qt.DeepEquals, []string{retainedVersionLate})

	_, _, nonLinearErr := runCompatStreams(c,
		"migrate", "apply",
		"--url", "sqlite://"+dbPath,
		"--dir", "file://"+bothFiles,
		"--exec-order", "non-linear",
	)
	c.Assert(nonLinearErr, qt.IsNil)
	c.Assert(compatTableNames(c, dbPath), qt.Contains, "rt_early")
	c.Assert(sqliteAtlasRevisionVersions(c, dbPath), qt.DeepEquals, []string{retainedVersionEarly, retainedVersionLate})

	_, _, secondErr := runCompatStreams(c,
		"migrate", "apply",
		"--url", "sqlite://"+dbPath,
		"--dir", "file://"+bothFiles,
		"--exec-order", "non-linear",
	)
	c.Assert(secondErr, qt.IsNil)
}

// TestCompatCommand_LinearSkipReproducesThePinnedPrefixInsertion pins the half
// of item 5 that makes the retained refusal legitimate: the outcome the pinned
// community binary v1.3.0 produces for a prefix insertion is still reachable
// here, so nothing is removed by refusing it as the default.
//
// Measured 2026-08-12 on the item 5 prefix fixture, both directories authored
// and hashed by that binary through `migrate import`, exit status read on its
// own line rather than through a pipe. At its DEFAULT --exec-order (linear, per
// its own --help) that binary exits 0 on the inserted directory, prints "No
// migration files to execute", and leaves the prefix migration unapplied: the
// table it would have created is absent from the catalog and only the late
// revision is recorded. It reports nothing about the file it passed over.
//
// This is not the interval fixture recorded in docs/conformance.md. There,
// applied revisions exist on both sides of the insertion, and both binaries
// refuse at the default order. The lower applied floor is the observable state
// that distinguishes the two oracle results.
//
// That silent discard is the argument for Ptah's default refusal, and this test
// is the reason the refusal costs nothing: --exec-order=linear-skip reproduces
// that outcome exactly, on request and in writing.
//
// The sibling test above covers the other two orders. This one is separate
// because it is the only row whose claim is an equivalence with the oracle
// rather than a divergence from it.
//
// Mutated so the skip branch tests ExecOrderNonLinear instead of
// ExecOrderLinearSkip in migrator.go, this test fails: linear-skip applies the
// inserted migration and rt_early appears.
func TestCompatCommand_LinearSkipReproducesThePinnedPrefixInsertion(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	dbPath := filepath.Join(root, "skip.db")
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

	_, _, skipErr := runCompatStreams(c,
		"migrate", "apply",
		"--url", "sqlite://"+dbPath,
		"--dir", "file://"+bothFiles,
		"--exec-order", "linear-skip",
	)
	c.Assert(skipErr, qt.IsNil)
	// The inserted migration is passed over rather than applied, which is what
	// that binary's default order does with it.
	c.Assert(compatTableNames(c, dbPath), qt.Not(qt.Contains), "rt_early")
	c.Assert(sqliteAtlasRevisionVersions(c, dbPath), qt.DeepEquals, []string{retainedVersionLate})

	// Repeating it is stable: skipping is not a one-shot that later promotes the
	// migration into the applied set.
	_, _, secondErr := runCompatStreams(c,
		"migrate", "apply",
		"--url", "sqlite://"+dbPath,
		"--dir", "file://"+bothFiles,
		"--exec-order", "linear-skip",
	)
	c.Assert(secondErr, qt.IsNil)
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
// `migrate hash` refuses on the same shared helper, and its oracle cell is
// measured now -- see the test below, which was unwritable while the sandbox
// this ran in refused any command containing that bare word.
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

// TestCompatCommand_HashRefusesATrailingPositional pins the fifth verb of #1241
// item 13, whose oracle reading was the one cell of that issue nobody ever took
// (stokaro/ptah#1623).
//
// Read 2026-08-17 against the pinned community binary v1.3.0 on a hashed
// directory, exit status from an unpiped invocation: `migrate hash --dir
// file://mig extra` exits 0 and prints ZERO bytes, and it hashes the directory
// anyway -- a migration added before the run lands in atlas.sum exactly as if
// the word had not been typed. So the operator who meant `--dir extra` and
// dropped the flag rewrites the checksum file of a directory they did not name,
// silently. Ptah refuses and names the flag the value belongs on.
//
// The companion cell, item 12's `--var`, needs no test here because it is
// parity: `migrate hash --dir file://mig --var x=1` exits 0 on both binaries
// with byte-identical output, which is none.
func TestCompatCommand_HashRefusesATrailingPositional(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	migrationsDir := writeRetainedDir(c, filepath.Join(root, "migrations"), map[string]string{
		retainedVersionEarly + "_early.sql": retainedEarlyBody,
	})

	_, _, err := runCompatStreams(c, "migrate", "hash", "--dir", "file://"+migrationsDir, "stray")

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, `unexpected positional arguments ["stray"]`)
	c.Assert(err.Error(), qt.Contains, "name the migration directory with --dir")
}

// TestCompatCommand_HashAcceptsAVar is the parity half of the pair above: item
// 12 of #1241, also unread until stokaro/ptah#1623.
//
// Both binaries exit 0 on `migrate hash --dir file://mig --var x=1` and print
// nothing. The row exists so the acceptance is held: a later change that made
// Ptah refuse an unused --var here would be a divergence introduced against a
// measured cell rather than an unknown.
func TestCompatCommand_HashAcceptsAVar(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	migrationsDir := writeRetainedDir(c, filepath.Join(root, "migrations"), map[string]string{
		retainedVersionEarly + "_early.sql": retainedEarlyBody,
	})

	stdout, stderr, err := runCompatStreams(c, "migrate", "hash", "--dir", "file://"+migrationsDir, "--var", "x=1")

	c.Assert(err, qt.IsNil)
	c.Assert(stdout, qt.Equals, "")
	c.Assert(stderr, qt.Equals, "")
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
