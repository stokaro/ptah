package migrateup

// White-box testing required: these tests reuse resetMigrateUpCommandForTest,
// which manages the command's package-global flag state and is not accessible
// from an external test package.
//
// The tests give end-to-end coverage for `-- +ptah check` directives loaded
// from real migration files (docs/pre-migration-checks.md syntax). This
// regressed once: the timeout directive scanner rejected the `check` token, so
// any file carrying the documented directive failed to load with
// `invalid +ptah directive "check"` before the check could ever run. The
// check-execution tests in migration/migrator bypass file loading via
// CreateMigrationFromSQL, so the CLI file path must be exercised here.

import (
	"bytes"
	"net/url"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/migratehash"
)

// checkDirectiveDropUsersSQL is the exact documented directive syntax from
// docs/pre-migration-checks.md guarding a destructive statement.
const checkDirectiveDropUsersSQL = `-- +ptah check name="users_empty" assert="SELECT count(*) = 0 FROM users" on_fail=abort` + "\nDROP TABLE users;\n"

// hashMigrationsDirForTest hashes dir through the real `migrations hash`
// command so --verify-sum exercises the full hash-then-load CLI flow.
func hashMigrationsDirForTest(tb testing.TB, dir string) {
	c := qt.New(tb)
	c.Helper()
	cmd := migratehash.NewMigrateHashCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"--dir", dir})
	c.Assert(cmd.Execute(), qt.IsNil, qt.Commentf("hash output:\n%s", out.String()))
}

func writeCheckDirectiveMigrationsDir(tb testing.TB, initUpSQL string) string {
	c := qt.New(tb)
	c.Helper()
	dir := c.TempDir()
	writeMigrateUpFile(c.TB, dir, "0000000001_init.up.sql", initUpSQL)
	writeMigrateUpFile(c.TB, dir, "0000000001_init.down.sql", "DROP TABLE users;\n")
	writeMigrateUpFile(c.TB, dir, "0000000002_drop_users.up.sql", checkDirectiveDropUsersSQL)
	writeMigrateUpFile(c.TB, dir, "0000000002_drop_users.down.sql", "CREATE TABLE users (id INTEGER PRIMARY KEY);\n")
	hashMigrationsDirForTest(c.TB, dir)
	return dir
}

func executeCheckDirectiveMigrateUp(tb testing.TB, dir, dbURL string) error {
	c := qt.New(tb)
	c.Helper()
	cmd := NewMigrateUpCommand()
	resetMigrateUpCommandForTest(c.TB, cmd)
	c.Cleanup(func() { resetMigrateUpCommandForTest(c.TB, cmd) })
	var out, errOut bytes.Buffer // swallow command output to keep test logs quiet
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs([]string{
		"--db-url", dbURL,
		"--migrations-dir", dir,
		"--verify-sum",
		"--allow-destructive",
	})
	return cmd.Execute()
}

func TestMigrateUpCommand_CheckDirectiveFilePassingCheckApplies(t *testing.T) {
	c := qt.New(t)

	// users is created empty, so migration 2's check passes and the guarded
	// DROP TABLE applies.
	dir := writeCheckDirectiveMigrationsDir(c.TB, "CREATE TABLE users (id INTEGER PRIMARY KEY);\n")
	dbURL := (&url.URL{Scheme: "sqlite", Path: filepath.Join(t.TempDir(), "ptah.db")}).String()

	err := executeCheckDirectiveMigrateUp(c.TB, dir, dbURL)

	c.Assert(err, qt.IsNil)
	c.Assert(sqliteMigrateUpTableExists(c.TB, dbURL, "users"), qt.IsFalse)
}

func TestMigrateUpCommand_CheckDirectiveFileFailingCheckAborts(t *testing.T) {
	c := qt.New(t)

	// users is seeded with a row, so migration 2's check must execute, fail,
	// and abort with the guarded DROP TABLE never applied.
	dir := writeCheckDirectiveMigrationsDir(c.TB,
		"CREATE TABLE users (id INTEGER PRIMARY KEY);\nINSERT INTO users (id) VALUES (1);\n")
	dbURL := (&url.URL{Scheme: "sqlite", Path: filepath.Join(t.TempDir(), "ptah.db")}).String()

	err := executeCheckDirectiveMigrateUp(c.TB, dir, dbURL)

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Not(qt.Contains), "invalid +ptah directive",
		qt.Commentf("the documented check directive must not fail file loading"))
	c.Assert(err.Error(), qt.Contains, "pre-migration check users_empty for migration 2 was not satisfied")
	c.Assert(err.Error(), qt.Contains, "rerun with --skip-checks")
	c.Assert(sqliteMigrateUpTableExists(c.TB, dbURL, "users"), qt.IsTrue)
}
