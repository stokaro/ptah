package atlas_test

import (
	"context"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
)

// Atlas enforces a txtar checks.sql section as a pre-migration gate: a
// failing assertion aborts the apply with exit 1 before any body statement
// runs (measured). ptah-compat
// matches that behavior, and — like Atlas — `migrate apply` registers no
// --skip-checks flag (#956). The bypass exists but is spelled as an
// environment variable, PTAH_SKIP_CHECKS, so the flag surface stays at parity;
// it is covered in migrate_apply_skip_checks_env_test.go. Every test in this
// file runs without that variable set.

const compatTxtarCheckedAddEmail = `-- atlas:txtar

-- checks.sql --
SELECT NOT EXISTS (SELECT * FROM users);

-- migration.sql --
ALTER TABLE users ADD COLUMN email TEXT;
`

func writeTxtarChecksMigrationsDir(tb testing.TB, dir string, usersSQL string) string {
	c := qt.New(tb)
	c.Helper()
	migrationsDir := filepath.Join(dir, "migrations")
	writeAtlasApplyProjectMigration(c.TB, migrationsDir, "20260801000001_create_users.sql", usersSQL)
	writeAtlasApplyProjectMigration(c.TB, migrationsDir, "20260801000002_add_users_email.sql", compatTxtarCheckedAddEmail)
	writeAtlasApplyProjectSum(c.TB, migrationsDir)
	return migrationsDir
}

func sqliteUsersEmailColumnCount(tb testing.TB, dbPath string) int {
	c := qt.New(tb)
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	row := conn.QueryRowContext(
		context.Background(),
		`SELECT count(*) FROM pragma_table_info('users') WHERE name = 'email'`,
	)
	var count int
	c.Assert(row.Scan(&count), qt.IsNil)
	return count
}

func TestMigrateApplyTxtarFailingChecksAbortBeforeBody(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	// Migration 1 seeds a row, so migration 2's checks.sql assertion fails.
	migrationsDir := writeTxtarChecksMigrationsDir(c.TB, dir,
		"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);\nINSERT INTO users (id, name) VALUES (1, 'alice');\n")
	dbPath := filepath.Join(dir, "apply.db")

	out, err := executeAtlasProjectCommand(
		"migrate", "apply",
		"--url", "sqlite://"+dbPath,
		"--dir", "file://"+migrationsDir,
	)

	c.Assert(err, qt.IsNotNil, qt.Commentf("command output:\n%s", out))
	c.Assert(err.Error(), qt.Contains, "pre-migration check checks.sql#1 for migration 20260801000002 was not satisfied")
	// Migration 1 applied; nothing from migration 2's body did.
	c.Assert(sqliteTableCount(c.TB, dbPath, "users"), qt.Equals, 1)
	c.Assert(sqliteUsersEmailColumnCount(c.TB, dbPath), qt.Equals, 0)
}

// TestMigrateApplyTxtarRetryAfterFixingDataSucceeds is the recovery half of the
// gate on the surface that needs it most: `ptah-compat migrate apply` registers
// no --skip-checks (Atlas has none either), so a check failure that left a dirty
// row would force every later apply through --allow-dirty. Since #966 that flag
// does recover, but a failed check must still leave no dirty row behind, or the
// drop-in workflow needs a flag Atlas users never had to pass.
func TestMigrateApplyTxtarRetryAfterFixingDataSucceeds(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	migrationsDir := writeTxtarChecksMigrationsDir(c.TB, dir,
		"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);\nINSERT INTO users (id, name) VALUES (1, 'alice');\n")
	dbPath := filepath.Join(dir, "apply.db")

	_, err := executeAtlasProjectCommand(
		"migrate", "apply",
		"--url", "sqlite://"+dbPath,
		"--dir", "file://"+migrationsDir,
	)
	c.Assert(err, qt.IsNotNil)

	// The operator fixes the data the check guarded.
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(context.Background(), "DELETE FROM users")
	c.Assert(err, qt.IsNil)
	dbschema.CloseAndWarn(conn)

	// The retry needs no flags and no manual revision repair.
	out, err := executeAtlasProjectCommand(
		"migrate", "apply",
		"--url", "sqlite://"+dbPath,
		"--dir", "file://"+migrationsDir,
	)

	c.Assert(err, qt.IsNil, qt.Commentf("retry output:\n%s", out))
	c.Assert(sqliteUsersEmailColumnCount(c.TB, dbPath), qt.Equals, 1)

	status, err := executeAtlasProjectCommand(
		"migrate", "status",
		"--url", "sqlite://"+dbPath,
		"--dir", "file://"+migrationsDir,
	)
	c.Assert(err, qt.IsNil, qt.Commentf("status output:\n%s", status))
	c.Assert(status, qt.Contains, "Current Version: 20260801000002")
}

func TestMigrateApplyTxtarPassingChecksApply(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	// users stays empty, so migration 2's checks.sql assertion passes.
	migrationsDir := writeTxtarChecksMigrationsDir(c.TB, dir,
		"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);\n")
	dbPath := filepath.Join(dir, "apply.db")

	out, err := executeAtlasProjectCommand(
		"migrate", "apply",
		"--url", "sqlite://"+dbPath,
		"--dir", "file://"+migrationsDir,
	)

	c.Assert(err, qt.IsNil, qt.Commentf("command output:\n%s", out))
	c.Assert(sqliteUsersEmailColumnCount(c.TB, dbPath), qt.Equals, 1)
}

func TestMigrateApplyHasNoSkipChecksFlag(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	migrationsDir := writeTxtarChecksMigrationsDir(c.TB, dir,
		"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);\n")
	dbPath := filepath.Join(dir, "apply.db")

	// Parity with Atlas: `migrate apply` registers no --skip-checks flag, so
	// the bypass is unreachable from the command line. It is reachable from
	// PTAH_SKIP_CHECKS, which is the point — the capability exists without the
	// flag surface growing a flag no Atlas build has.
	out, err := executeAtlasProjectCommand(
		"migrate", "apply",
		"--url", "sqlite://"+dbPath,
		"--dir", "file://"+migrationsDir,
		"--skip-checks",
	)

	c.Assert(err, qt.IsNotNil, qt.Commentf("command output:\n%s", out))
	c.Assert(err.Error(), qt.Contains, "unknown flag: --skip-checks")
}
