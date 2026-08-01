package atlas_test

import (
	"context"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/dbschema"
)

// Atlas Pro enforces a txtar checks.sql section as a pre-migration gate: a
// failing assertion aborts the apply with exit 1 before any body statement
// runs (measured 2026-08-01, Atlas CLI v1.2.4-e282f76-canary). ptah-compat
// matches that behavior, and — like Atlas — `migrate apply` has no
// --skip-checks escape hatch, so checks always enforce on the compat surface
// (#956).

const compatTxtarCheckedAddEmail = `-- atlas:txtar

-- checks.sql --
SELECT NOT EXISTS (SELECT * FROM users);

-- migration.sql --
ALTER TABLE users ADD COLUMN email TEXT;
`

func writeTxtarChecksMigrationsDir(c *qt.C, dir string, usersSQL string) string {
	c.Helper()
	migrationsDir := filepath.Join(dir, "migrations")
	writeAtlasApplyProjectMigration(c, migrationsDir, "20260801000001_create_users.sql", usersSQL)
	writeAtlasApplyProjectMigration(c, migrationsDir, "20260801000002_add_users_email.sql", compatTxtarCheckedAddEmail)
	writeAtlasApplyProjectSum(c, migrationsDir)
	return migrationsDir
}

func sqliteUsersEmailColumnCount(c *qt.C, dbPath string) int {
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
	migrationsDir := writeTxtarChecksMigrationsDir(c, dir,
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
	c.Assert(sqliteTableCount(c, dbPath, "users"), qt.Equals, 1)
	c.Assert(sqliteUsersEmailColumnCount(c, dbPath), qt.Equals, 0)
}

// TestMigrateApplyTxtarRetryAfterFixingDataSucceeds is the recovery half of the
// gate on the surface that needs it most: `ptah-compat migrate apply` has
// neither --skip-checks nor --allow-dirty (Atlas has neither either), so a
// failed check must leave no dirty revision row behind. Otherwise the next
// apply would abort on that row and the drop-in workflow would be wedged with
// no in-band way out.
func TestMigrateApplyTxtarRetryAfterFixingDataSucceeds(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	migrationsDir := writeTxtarChecksMigrationsDir(c, dir,
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
	c.Assert(sqliteUsersEmailColumnCount(c, dbPath), qt.Equals, 1)

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
	migrationsDir := writeTxtarChecksMigrationsDir(c, dir,
		"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);\n")
	dbPath := filepath.Join(dir, "apply.db")

	out, err := executeAtlasProjectCommand(
		"migrate", "apply",
		"--url", "sqlite://"+dbPath,
		"--dir", "file://"+migrationsDir,
	)

	c.Assert(err, qt.IsNil, qt.Commentf("command output:\n%s", out))
	c.Assert(sqliteUsersEmailColumnCount(c, dbPath), qt.Equals, 1)
}

func TestMigrateApplyHasNoSkipChecksFlag(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	migrationsDir := writeTxtarChecksMigrationsDir(c, dir,
		"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);\n")
	dbPath := filepath.Join(dir, "apply.db")

	// Parity with Atlas: `migrate apply` registers no --skip-checks, so the
	// compat surface cannot bypass pre-migration checks.
	out, err := executeAtlasProjectCommand(
		"migrate", "apply",
		"--url", "sqlite://"+dbPath,
		"--dir", "file://"+migrationsDir,
		"--skip-checks",
	)

	c.Assert(err, qt.IsNotNil, qt.Commentf("command output:\n%s", out))
	c.Assert(err.Error(), qt.Contains, "unknown flag: --skip-checks")
}
