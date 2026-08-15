package atlas_test

import (
	"bytes"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/atlas"
)

// A dry run intercepts writes, never reads, and a pre-migration check is a
// read. Before #1005 that meant every checked migration's guard was evaluated
// for real against a database the dry run had refused to change, so migration
// N's precondition was asked about state that only exists once migrations
// 1..N-1 apply — state the preview had, by construction, declined to produce.
// `migrate apply --dry-run` therefore failed with "no such table: users" on a
// directory whose real apply succeeds.
//
// The rule that replaced it: a dry run evaluates a migration's assertions only
// where the state it observes is the state a real apply would evaluate them
// against — that is, for the FIRST migration executed in the run. Every check
// is still parsed and statically validated wherever it sits; only the database
// evaluation of later migrations is deferred, and the run says how many it
// deferred.
//
// The oracle for each row below is the REAL apply of the same directory: a
// preview is right when it predicts what applying would do. The pinned
// community binary is no oracle here, because it implements no check semantics
// at all — it flattens the archive and runs checks.sql as an ordinary migration
// statement — so it exits 0 on every row in this file. Matching it on the rows
// where the guard genuinely fails would make our preview lie about a failure we
// can see coming, which is why E, F, H and I stay strict.

const (
	dryRunChecksCreateUsers = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);\n"
	dryRunChecksAddEmail    = "ALTER TABLE users ADD COLUMN email TEXT;\n"

	// A guard that needs a table an earlier pending migration creates.
	dryRunChecksTxtarNeedsPrior = `-- atlas:txtar

-- checks.sql --
SELECT NOT EXISTS (SELECT * FROM users);

-- migration.sql --
ALTER TABLE users ADD COLUMN email TEXT;
`
	// A guard that is false whatever the state.
	dryRunChecksTxtarAlwaysFalse = `-- atlas:txtar

-- checks.sql --
SELECT 0;

-- migration.sql --
ALTER TABLE users ADD COLUMN email TEXT;
`
	// A first migration carrying a guard that is false whatever the state.
	dryRunChecksTxtarFalseFirst = `-- atlas:txtar

-- checks.sql --
SELECT 0;

-- migration.sql --
CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
`
	// A first migration carrying a guard satisfied on a fresh database.
	dryRunChecksTxtarSatisfiedFirst = `-- atlas:txtar

-- checks.sql --
SELECT 1;

-- migration.sql --
CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
`
	// The same shape as dryRunChecksTxtarNeedsPrior, spelled as a directive.
	dryRunChecksDirectiveNeedsPrior = "-- +ptah check name=\"users_empty\" assert=\"SELECT count(*) = 0 FROM users\" on_fail=abort\n" +
		"ALTER TABLE users ADD COLUMN email TEXT;\n"
	// An authoring error decidable from the text alone: no assert predicate.
	dryRunChecksDirectiveMalformed = "-- +ptah check name=\"users_empty\" on_fail=abort\n" +
		"ALTER TABLE users ADD COLUMN email TEXT;\n"
	// An authoring error decidable from the text alone: not a read-only SELECT.
	dryRunChecksTxtarWriteAssertion = `-- atlas:txtar

-- checks.sql --
UPDATE users SET name = 'x';

-- migration.sql --
ALTER TABLE users ADD COLUMN email TEXT;
`
)

const dryRunChecksDeferredNote = "Deferred pre-migration checks"

// writeDryRunChecksDir writes a hashed two-migration directory.
func writeDryRunChecksDir(c *qt.C, dir, first, second string) string {
	c.Helper()
	migrationsDir := filepath.Join(dir, "migrations")
	writeAtlasApplyProjectMigration(c, migrationsDir, "20260101000001_one.sql", first)
	writeAtlasApplyProjectMigration(c, migrationsDir, "20260101000002_two.sql", second)
	writeAtlasApplyProjectSum(c, migrationsDir)
	return migrationsDir
}

// runDryRunChecksApply runs `migrate apply` with stdout and stderr kept apart,
// which is what lets a row assert that the deferred-checks note never lands on
// the machine-readable stream.
func runDryRunChecksApply(dir string, args ...string) (stdout, stderr string, err error) {
	cmd := atlas.NewCompatCommand("atlas")
	var outBuf, errBuf bytes.Buffer
	cmd.SetOut(&outBuf)
	cmd.SetErr(&errBuf)
	cmd.SetArgs(append([]string{"migrate", "apply", "--dir", "file://" + dir}, args...))
	err = cmd.Execute()
	return outBuf.String(), errBuf.String(), err
}

// seedFirstMigration really applies migration 1, which promotes migration 2 to
// first place in the next run — the position that decides whether its guard is
// evaluated.
func seedFirstMigration(c *qt.C, migrationsDir, dbPath string) {
	c.Helper()
	out, err := executeAtlasProjectCommand(
		"migrate", "apply",
		"--url", "sqlite://"+dbPath,
		"--dir", "file://"+migrationsDir,
		"1",
	)
	c.Assert(err, qt.IsNil, qt.Commentf("seed output:\n%s", out))
}

// previewDryRunChecks writes the two-migration directory and previews it
// against an EMPTY database, so both migrations are pending and migration 1
// leads the run. The tests whose subject is the other arrangement — migration 2
// leading, because migration 1 was really applied — seed it themselves.
func previewDryRunChecks(c *qt.C, first, second string, extraArgs ...string) (stdout, stderr string, err error) {
	c.Helper()
	dir := c.TempDir()
	migrationsDir := writeDryRunChecksDir(c, dir, first, second)
	return runDryRunChecksApply(
		migrationsDir,
		append([]string{"--url", "sqlite://" + filepath.Join(dir, "apply.db"), "--dry-run"}, extraArgs...)...,
	)
}

// TestMigrateApplyDryRunChecksEvaluateWhatTheRunCanObserve is the pair of
// controls the deferral rule is measured against: nothing is deferred when
// there is nothing to defer, and nothing is deferred when the guard sits where
// a real apply would evaluate it anyway.
func TestMigrateApplyDryRunChecksEvaluateWhatTheRunCanObserve(t *testing.T) {
	tests := []struct {
		name   string
		first  string
		second string
	}{
		{
			// Control: nothing declares a check, so nothing can be deferred.
			name:   "A no checks anywhere",
			first:  dryRunChecksCreateUsers,
			second: dryRunChecksAddEmail,
		},
		{
			// Control: the first migration's guard holds on a fresh database,
			// so it is evaluated, passes, and nothing is deferred.
			name:   "B check on the first migration passes",
			first:  dryRunChecksTxtarSatisfiedFirst,
			second: dryRunChecksAddEmail,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			unsetSkipChecksEnv(t)

			stdout, stderr, err := previewDryRunChecks(c, test.first, test.second)

			c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
			c.Assert(stderr, qt.Not(qt.Contains), dryRunChecksDeferredNote)
		})
	}
}

// TestMigrateApplyDryRunChecksDeferChecksOnLaterMigrations is the rule itself:
// a guard on a migration that does not lead the run is evaluated against state
// the preview declined to produce, so it is deferred — and SAID, by version, so
// a deferral is never a silent drop.
func TestMigrateApplyDryRunChecksDeferChecksOnLaterMigrations(t *testing.T) {
	tests := []struct {
		name      string
		first     string
		second    string
		extraArgs []string
		wantNote  string
	}{
		{
			// THE ISSUE. Migration 2's guard reads a table migration 1 creates.
			// The real apply of this directory succeeds, so the old failure was
			// an artifact of the preview and nothing else.
			name:     "C check needs state a prior pending migration creates",
			first:    dryRunChecksCreateUsers,
			second:   dryRunChecksTxtarNeedsPrior,
			wantNote: "Deferred pre-migration checks for 1 migration (20260101000002)",
		},
		{
			// D's guard is false regardless of state, but proving that needs a
			// query, and the position rule cannot know it without one. It is
			// deferred and reported rather than silently dropped.
			name:     "D always-false check on a later migration is deferred and reported",
			first:    dryRunChecksCreateUsers,
			second:   dryRunChecksTxtarAlwaysFalse,
			wantNote: "Deferred pre-migration checks for 1 migration (20260101000002)",
		},
		{
			// G proves the rule covers both spellings, not just txtar.
			name:     "G +ptah check directive is deferred like a txtar check",
			first:    dryRunChecksCreateUsers,
			second:   dryRunChecksDirectiveNeedsPrior,
			wantNote: "Deferred pre-migration checks for 1 migration (20260101000002)",
		},
		{
			// --tx-mode none shares the per-file loop, so it shares the rule.
			name:      "C under --tx-mode none",
			first:     dryRunChecksCreateUsers,
			second:    dryRunChecksTxtarNeedsPrior,
			extraArgs: []string{"--tx-mode", "none"},
			wantNote:  "Deferred pre-migration checks for 1 migration (20260101000002)",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			unsetSkipChecksEnv(t)

			stdout, stderr, err := previewDryRunChecks(c, test.first, test.second, test.extraArgs...)

			c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
			c.Assert(stdout, qt.Contains, "Would have applied 2 migrations.")
			c.Assert(stderr, qt.Contains, test.wantNote)
			// The note is a human-facing aside; stdout stays clean.
			c.Assert(stdout, qt.Not(qt.Contains), dryRunChecksDeferredNote)
		})
	}
}

// TestMigrateApplyDryRunChecksRefuseWhatThePreviewCanDecide holds the rows
// where the preview must still fail, and each is a way the naive "skip every
// check in a dry run" fix went wrong.
//
// The oracle is the same as everywhere else in this file: the REAL apply of the
// same directory. Each row's directory cannot apply, so its preview must not
// report that it could.
func TestMigrateApplyDryRunChecksRefuseWhatThePreviewCanDecide(t *testing.T) {
	tests := []struct {
		name      string
		first     string
		second    string
		extraArgs []string
		wantErr   string
	}{
		{
			// E kills "skip every check in a dry run": the guard sits on the
			// first migration executed, so the preview observes exactly the
			// state a real apply would, and the real apply fails too.
			name:    "E false check on the first migration still fails",
			first:   dryRunChecksTxtarFalseFirst,
			second:  dryRunChecksAddEmail,
			wantErr: "pre-migration check",
		},
		{
			// H is the regression that killed the naive fix. Whether a
			// directive is malformed is decided by its text, so deferring
			// evaluation must not stop reporting it.
			name:    "H malformed directive is still reported",
			first:   dryRunChecksCreateUsers,
			second:  dryRunChecksDirectiveMalformed,
			wantErr: "invalid pre-migration check directives",
		},
		{
			// I is the other half of that regression: a write-shaped assertion
			// needs no database to condemn.
			name:    "I write-shaped assertion is still reported",
			first:   dryRunChecksCreateUsers,
			second:  dryRunChecksTxtarWriteAssertion,
			wantErr: "check assertion must be a read-only SELECT statement",
		},
		{
			// --tx-mode all refuses a checked directory whether or not the run
			// is a preview. It refuses, so the preview refuses. Answering 0
			// here would be a preview reporting success for a run that cannot
			// succeed.
			name:      "C under --tx-mode all",
			first:     dryRunChecksCreateUsers,
			second:    dryRunChecksTxtarNeedsPrior,
			extraArgs: []string{"--tx-mode", "all"},
			wantErr:   "cannot run with tx-mode all",
		},
		{
			// A batch preview still evaluates the leading migration's guard.
			name:      "E under --tx-mode all still fails",
			first:     dryRunChecksTxtarFalseFirst,
			second:    dryRunChecksAddEmail,
			extraArgs: []string{"--tx-mode", "all"},
			wantErr:   "pre-migration check",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			unsetSkipChecksEnv(t)

			stdout, stderr, err := previewDryRunChecks(c, test.first, test.second, test.extraArgs...)

			c.Assert(err, qt.IsNotNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
			c.Assert(err.Error(), qt.Contains, test.wantErr)
		})
	}
}

// TestMigrateApplyDryRunChecksKeepTheNoteOffMachineReadableStdout covers the
// --format branch, which returns before the plain-text summary and so needs a
// run of its own: stdout must stay parseable and the note must still reach
// stderr.
func TestMigrateApplyDryRunChecksKeepTheNoteOffMachineReadableStdout(t *testing.T) {
	c := qt.New(t)
	unsetSkipChecksEnv(t)

	stdout, stderr, err := previewDryRunChecks(c,
		dryRunChecksCreateUsers, dryRunChecksTxtarNeedsPrior,
		"--format", "{{ json . }}")

	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(stdout, qt.Contains, `"Pending"`)
	c.Assert(stdout, qt.Not(qt.Contains), dryRunChecksDeferredNote)
	c.Assert(stderr, qt.Contains, dryRunChecksDeferredNote)
}

// TestMigrateApplyDryRunChecksEvaluateTheCheckedMigrationOnceItLeadsTheRun is C
// with migration 1 really applied: the checked migration now leads the run, so
// its guard IS evaluated — and passes, because migration 1 left the table
// empty. Nothing is deferred, which is what makes the deferral in C a
// consequence of the position rather than of the check.
func TestMigrateApplyDryRunChecksEvaluateTheCheckedMigrationOnceItLeadsTheRun(t *testing.T) {
	c := qt.New(t)
	unsetSkipChecksEnv(t)
	dir := c.TempDir()
	migrationsDir := writeDryRunChecksDir(c, dir, dryRunChecksCreateUsers, dryRunChecksTxtarNeedsPrior)
	dbPath := filepath.Join(dir, "apply.db")
	seedFirstMigration(c, migrationsDir, dbPath)

	stdout, stderr, err := runDryRunChecksApply(migrationsDir, "--url", "sqlite://"+dbPath, "--dry-run")

	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(stderr, qt.Not(qt.Contains), dryRunChecksDeferredNote)
}

// TestMigrateApplyDryRunChecksFailWhenTheLeadingMigrationsGuardFails is F, and
// it kills "skip whenever the migration is not first in the DIRECTORY".
// Migration 2 is second in the directory but first in the RUN once migration 1
// is applied, and its guard genuinely fails because migration 1 seeded the row
// it forbids.
func TestMigrateApplyDryRunChecksFailWhenTheLeadingMigrationsGuardFails(t *testing.T) {
	c := qt.New(t)
	unsetSkipChecksEnv(t)
	dir := c.TempDir()
	migrationsDir := writeDryRunChecksDir(c, dir,
		dryRunChecksCreateUsers+"INSERT INTO users (id, name) VALUES (1, 'alice');\n",
		dryRunChecksTxtarNeedsPrior)
	dbPath := filepath.Join(dir, "apply.db")
	seedFirstMigration(c, migrationsDir, dbPath)

	_, _, err := runDryRunChecksApply(migrationsDir, "--url", "sqlite://"+dbPath, "--dry-run")

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "pre-migration check")
}

func TestMigrateApplyDryRunChecksNameConvertedFlywayIdentity(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	writeAtlasApplyProjectMigration(c, migrationsDir, "V1__one.sql", dryRunChecksCreateUsers)
	writeAtlasApplyProjectMigration(c, migrationsDir, "V1.5__two.sql", dryRunChecksDirectiveNeedsPrior)
	hashConvertedApplyDir(c, migrationsDir, "flyway")
	dbPath := filepath.Join(dir, "flyway-checks.db")

	stdout, stderr, err := runDryRunChecksApply(
		migrationsDir+"?format=flyway",
		"--url", "sqlite://"+dbPath,
		"--dry-run",
	)

	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(stderr, qt.Contains, "Deferred pre-migration checks for 1 migration (1.5)")
	c.Assert(stderr, qt.Not(qt.Contains), "461168")
}
