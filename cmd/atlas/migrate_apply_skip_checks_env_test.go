package atlas_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/dbschema"
)

// unsetSkipChecksEnv removes PTAH_SKIP_CHECKS for the duration of a test.
// t.Setenv records the caller's original value and restores it on cleanup, so
// registering it first makes the following Unsetenv safe to leave in place.
func unsetSkipChecksEnv(t *testing.T) {
	t.Helper()
	t.Setenv("PTAH_SKIP_CHECKS", "")
	if err := os.Unsetenv("PTAH_SKIP_CHECKS"); err != nil {
		t.Fatalf("unset PTAH_SKIP_CHECKS: %v", err)
	}
}

// `ptah-compat migrate apply` exposes the pre-migration check bypass through the
// PTAH_SKIP_CHECKS environment variable rather than a flag (stokaro/ptah#951).
//
// The flag is not an option, and that is measured rather than assumed. Atlas CE
// v1.2.0 answers `migrate apply --skip-checks` with `unknown flag:
// --skip-checks`, the same text it gives a nonsense sibling (`--skip-chxxxx`),
// so the flag is unregistered rather than registered-and-community-gated; and
// the licensed v1.2.4 help surface registers `--skip-checks` only on `migrate
// down`, never on `migrate apply`. Adding it here would put a non-Atlas flag on
// the compat surface, which the conformance cli-surface tier forbids.
// TestMigrateApplyHasNoSkipChecksFlag pins that half; these tests pin the
// capability the environment variable makes reachable.

// migrationWithPtahCheckDirective guards its body with a `-- +ptah check`
// directive. That construct is Ptah's own — Atlas has no counterpart and
// therefore no behavior to reproduce here — which is why the bypass is a Ptah
// environment variable rather than an Atlas flag.
const migrationWithPtahCheckDirective = `-- +ptah check name="users_empty" assert="SELECT count(*) = 0 FROM users" on_fail=abort
ALTER TABLE users ADD COLUMN email TEXT;
`

// writeCheckedMigrationsDir seeds a users row in migration 1 so migration 2's
// guard is unsatisfied, and hashes the directory because apply verifies
// atlas.sum before planning (stokaro/ptah#970).
func writeCheckedMigrationsDir(c *qt.C, dir, secondMigration string) string {
	c.Helper()
	migrationsDir := filepath.Join(dir, "migrations")
	writeAtlasApplyProjectMigration(c, migrationsDir, "20260801000001_create_users.sql",
		"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);\nINSERT INTO users (id, name) VALUES (1, 'alice');\n")
	writeAtlasApplyProjectMigration(c, migrationsDir, "20260801000002_add_users_email.sql", secondMigration)
	writeAtlasApplyProjectSum(c, migrationsDir)
	return migrationsDir
}

func TestMigrateApplySkipChecksEnvBypassesFailingTxtarCheck(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	migrationsDir := writeCheckedMigrationsDir(c, dir, compatTxtarCheckedAddEmail)
	dbPath := filepath.Join(dir, "apply.db")
	t.Setenv("PTAH_SKIP_CHECKS", "1")

	out, err := executeAtlasProjectCommand(
		"migrate", "apply",
		"--url", "sqlite://"+dbPath,
		"--dir", "file://"+migrationsDir,
	)

	c.Assert(err, qt.IsNil, qt.Commentf("command output:\n%s", out))
	c.Assert(sqliteUsersEmailColumnCount(c, dbPath), qt.Equals, 1)
	// The bypass is invisible in the command line, so the run has to say so.
	c.Assert(out, qt.Contains, "warning: PTAH_SKIP_CHECKS is set")
}

// The `-- +ptah check` directive is the case with no Atlas spelling at all:
// before the environment gate it was enforceable through ptah-compat with no
// escape hatch, while native `ptah migrations up --skip-checks` had one.
func TestMigrateApplySkipChecksEnvBypassesFailingPtahCheckDirective(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	migrationsDir := writeCheckedMigrationsDir(c, dir, migrationWithPtahCheckDirective)
	dbPath := filepath.Join(dir, "apply.db")
	t.Setenv("PTAH_SKIP_CHECKS", "1")

	out, err := executeAtlasProjectCommand(
		"migrate", "apply",
		"--url", "sqlite://"+dbPath,
		"--dir", "file://"+migrationsDir,
	)

	c.Assert(err, qt.IsNil, qt.Commentf("command output:\n%s", out))
	c.Assert(sqliteUsersEmailColumnCount(c, dbPath), qt.Equals, 1)
}

// TestMigrateApplySkipChecksEnvValues separates "the variable is set" from "the
// variable parses as true": an unset variable, an explicit false and a
// malformed value must all leave checks enforcing, and only the malformed one
// must fail before the database is touched.
//
// Each case builds its own directory and database. A shared fixture would let a
// case pass on another case's state instead of on its own input.
func TestMigrateApplySkipChecksEnvValues(t *testing.T) {
	for _, tc := range []struct {
		name string
		// value is the PTAH_SKIP_CHECKS value; unset selects no variable at all.
		value string
		unset bool
		// wantErr is the substring the refusal must contain.
		wantErr string
		// wantApplied reports whether migration 2's body must have run.
		wantApplied bool
	}{
		{
			name:    "unset enforces checks",
			unset:   true,
			wantErr: "pre-migration check checks.sql#1 for migration 20260801000002 was not satisfied",
		},
		{
			name:    "empty value enforces checks",
			value:   "",
			wantErr: "pre-migration check checks.sql#1 for migration 20260801000002 was not satisfied",
		},
		{
			name:    "zero enforces checks",
			value:   "0",
			wantErr: "pre-migration check checks.sql#1 for migration 20260801000002 was not satisfied",
		},
		{
			name:    "false enforces checks",
			value:   "false",
			wantErr: "pre-migration check checks.sql#1 for migration 20260801000002 was not satisfied",
		},
		{
			name:    "malformed value is refused, not treated as false",
			value:   "notabool",
			wantErr: `invalid boolean value "notabool" for PTAH_SKIP_CHECKS`,
		},
		{
			name:        "true bypasses checks",
			value:       "true",
			wantApplied: true,
		},
		{
			name:        "t bypasses checks",
			value:       "t",
			wantApplied: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			dir := t.TempDir()
			migrationsDir := writeCheckedMigrationsDir(c, dir, compatTxtarCheckedAddEmail)
			dbPath := filepath.Join(dir, "apply.db")
			if tc.unset {
				unsetSkipChecksEnv(t)
			} else {
				t.Setenv("PTAH_SKIP_CHECKS", tc.value)
			}

			out, err := executeAtlasProjectCommand(
				"migrate", "apply",
				"--url", "sqlite://"+dbPath,
				"--dir", "file://"+migrationsDir,
			)

			if tc.wantApplied {
				c.Assert(err, qt.IsNil, qt.Commentf("command output:\n%s", out))
				c.Assert(sqliteUsersEmailColumnCount(c, dbPath), qt.Equals, 1)
				return
			}
			c.Assert(err, qt.IsNotNil, qt.Commentf("command output:\n%s", out))
			c.Assert(err.Error(), qt.Contains, tc.wantErr)
			c.Assert(sqliteUsersEmailColumnCount(c, dbPath), qt.Equals, 0)
		})
	}
}

// A malformed value must be refused before anything is applied, not merely
// reported: the run aborts while migration 1 is still pending.
func TestMigrateApplySkipChecksEnvMalformedValueAppliesNothing(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	migrationsDir := writeCheckedMigrationsDir(c, dir, compatTxtarCheckedAddEmail)
	dbPath := filepath.Join(dir, "apply.db")
	t.Setenv("PTAH_SKIP_CHECKS", "notabool")

	_, err := executeAtlasProjectCommand(
		"migrate", "apply",
		"--url", "sqlite://"+dbPath,
		"--dir", "file://"+migrationsDir,
	)

	c.Assert(err, qt.IsNotNil)
	c.Assert(sqliteTableCount(c, dbPath, "users"), qt.Equals, 0)
}

// tx-mode all refuses a migration declaring pre-migration checks, because a
// check on the pool connection cannot see earlier batched migrations'
// uncommitted work. Bypassing checks removes the reason for the refusal, so the
// batch runs — this is the branch whose diagnostic used to advertise a
// --skip-checks flag the compat surface then rejected.
func TestMigrateApplySkipChecksEnvLiftsTxModeAllRefusal(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	migrationsDir := writeCheckedMigrationsDir(c, dir, compatTxtarCheckedAddEmail)

	refusedDB := filepath.Join(dir, "refused.db")
	_, err := executeAtlasProjectCommand(
		"migrate", "apply",
		"--url", "sqlite://"+refusedDB,
		"--dir", "file://"+migrationsDir,
		"--tx-mode", "all",
	)
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "declares pre-migration checks, which cannot run with tx-mode all")

	t.Setenv("PTAH_SKIP_CHECKS", "1")
	bypassedDB := filepath.Join(dir, "bypassed.db")
	out, err := executeAtlasProjectCommand(
		"migrate", "apply",
		"--url", "sqlite://"+bypassedDB,
		"--dir", "file://"+migrationsDir,
		"--tx-mode", "all",
	)

	c.Assert(err, qt.IsNil, qt.Commentf("command output:\n%s", out))
	c.Assert(sqliteUsersEmailColumnCount(c, bypassedDB), qt.Equals, 1)
}

// The environment gate must not reach past the checks it names. A directory
// with no checks behaves identically with and without the variable, and in
// particular the atlas.sum integrity gate still refuses a tampered directory —
// PTAH_SKIP_CHECKS is not a general safety switch.
func TestMigrateApplySkipChecksEnvDoesNotBypassChecksumVerification(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	migrationsDir := writeCheckedMigrationsDir(c, dir, compatTxtarCheckedAddEmail)
	// Tamper after hashing.
	writeAtlasApplyProjectMigration(c, migrationsDir, "20260801000001_create_users.sql",
		"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, tampered TEXT);\n")
	dbPath := filepath.Join(dir, "apply.db")
	t.Setenv("PTAH_SKIP_CHECKS", "1")

	out, err := executeAtlasProjectCommand(
		"migrate", "apply",
		"--url", "sqlite://"+dbPath,
		"--dir", "file://"+migrationsDir,
	)

	c.Assert(err, qt.IsNotNil, qt.Commentf("command output:\n%s", out))
	c.Assert(sqliteTableCount(c, dbPath, "users"), qt.Equals, 0)
}

// A directory with no checks is unaffected by the variable: nothing about
// selection, ordering or execution changes when there is nothing to bypass.
func TestMigrateApplySkipChecksEnvLeavesUncheckedDirectoryUnchanged(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	writeAtlasApplyProjectMigration(c, migrationsDir, "20260801000001_create_users.sql",
		"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);\n")
	writeAtlasApplyProjectSum(c, migrationsDir)
	dbPath := filepath.Join(dir, "apply.db")
	t.Setenv("PTAH_SKIP_CHECKS", "1")

	out, err := executeAtlasProjectCommand(
		"migrate", "apply",
		"--url", "sqlite://"+dbPath,
		"--dir", "file://"+migrationsDir,
	)

	c.Assert(err, qt.IsNil, qt.Commentf("command output:\n%s", out))
	c.Assert(sqliteTableCount(c, dbPath, "users"), qt.Equals, 1)
}

// sqliteMigrationRowCount counts revision rows, so a bypassed run can be shown
// to record its migrations normally rather than skipping bookkeeping too.
func sqliteMigrationRowCount(c *qt.C, dbPath string) int {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	row := conn.QueryRowContext(context.Background(), `SELECT count(*) FROM atlas_schema_revisions`)
	var count int
	c.Assert(row.Scan(&count), qt.IsNil)
	return count
}

// Bypassing checks must not disturb revision bookkeeping: both migrations are
// recorded, so a later run is a clean no-op rather than a replay.
func TestMigrateApplySkipChecksEnvStillRecordsRevisions(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	migrationsDir := writeCheckedMigrationsDir(c, dir, compatTxtarCheckedAddEmail)
	dbPath := filepath.Join(dir, "apply.db")
	t.Setenv("PTAH_SKIP_CHECKS", "1")

	_, err := executeAtlasProjectCommand(
		"migrate", "apply",
		"--url", "sqlite://"+dbPath,
		"--dir", "file://"+migrationsDir,
	)
	c.Assert(err, qt.IsNil)
	c.Assert(sqliteMigrationRowCount(c, dbPath), qt.Equals, 2)

	out, err := executeAtlasProjectCommand(
		"migrate", "apply",
		"--url", "sqlite://"+dbPath,
		"--dir", "file://"+migrationsDir,
	)
	c.Assert(err, qt.IsNil, qt.Commentf("command output:\n%s", out))
	c.Assert(out, qt.Contains, "No migration files to execute.")
}
