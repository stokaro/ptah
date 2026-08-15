package migrateup

// White-box testing required: these tests reuse resetMigrateUpCommandForTest,
// which manages the command's package-global flag state and is not accessible
// from an external test package.
//
// They cover the native CLI surface for Atlas txtar checks.sql enforcement
// (#956): a failing check aborts `ptah migrations up`, and --skip-checks
// bypasses txtar checks exactly as it bypasses `-- +ptah check` directives.

import (
	"bytes"
	"context"
	"net/url"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
)

// txtarCheckedAddEmailSQL mirrors the measured Atlas fixture: checks.sql
// asserts users is empty before migration.sql adds the email column.
const txtarCheckedAddEmailSQL = `-- atlas:txtar

-- checks.sql --
SELECT NOT EXISTS (SELECT * FROM users);

-- migration.sql --
ALTER TABLE users ADD COLUMN email TEXT;
`

func writeTxtarCheckedMigrationsDir(tb testing.TB, usersSQL string) string {
	c := qt.New(tb)
	c.Helper()
	dir := c.TempDir()
	writeMigrateUpFile(c.TB, dir, "20260801000001_create_users.sql", usersSQL)
	writeMigrateUpFile(c.TB, dir, "20260801000002_add_users_email.sql", txtarCheckedAddEmailSQL)
	return dir
}

func executeTxtarMigrateUp(tb testing.TB, dir, dbURL string, extraArgs ...string) error {
	c := qt.New(tb)
	c.Helper()
	cmd := NewMigrateUpCommand()
	resetMigrateUpCommandForTest(c.TB, cmd)
	c.Cleanup(func() { resetMigrateUpCommandForTest(c.TB, cmd) })
	var out, errOut bytes.Buffer // swallow command output to keep test logs quiet
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs(append([]string{
		"--db-url", dbURL,
		"--migrations-dir", dir,
	}, extraArgs...))
	return cmd.Execute()
}

func sqliteMigrateUpUsersHasEmail(tb testing.TB, dbURL string) bool {
	c := qt.New(tb)
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	var count int
	err = conn.QueryRow(
		"SELECT COUNT(*) FROM pragma_table_info('users') WHERE name = 'email'",
	).Scan(&count)
	c.Assert(err, qt.IsNil)
	return count > 0
}

func TestMigrateUpCommand_TxtarFailingCheckAborts(t *testing.T) {
	c := qt.New(t)

	// Migration 1 seeds a row, so migration 2's checks.sql assertion fails and
	// the guarded ALTER TABLE never runs.
	dir := writeTxtarCheckedMigrationsDir(c.TB,
		"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);\nINSERT INTO users (id, name) VALUES (1, 'alice');\n")
	dbURL := (&url.URL{Scheme: "sqlite", Path: filepath.Join(t.TempDir(), "ptah.db")}).String()

	err := executeTxtarMigrateUp(c.TB, dir, dbURL)

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "pre-migration check checks.sql#1 for migration 20260801000002 was not satisfied")
	c.Assert(err.Error(), qt.Contains, "rerun with --skip-checks")
	c.Assert(sqliteMigrateUpUsersHasEmail(c.TB, dbURL), qt.IsFalse)
}

func TestMigrateUpCommand_TxtarSkipChecksBypasses(t *testing.T) {
	c := qt.New(t)

	dir := writeTxtarCheckedMigrationsDir(c.TB,
		"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);\nINSERT INTO users (id, name) VALUES (1, 'alice');\n")
	dbURL := (&url.URL{Scheme: "sqlite", Path: filepath.Join(t.TempDir(), "ptah.db")}).String()

	err := executeTxtarMigrateUp(c.TB, dir, dbURL, "--skip-checks")

	c.Assert(err, qt.IsNil)
	c.Assert(sqliteMigrateUpUsersHasEmail(c.TB, dbURL), qt.IsTrue)
}
