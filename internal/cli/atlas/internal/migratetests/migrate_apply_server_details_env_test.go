package migratetests_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/atlas/internal/atlastest"
)

// `ptah-compat migrate apply` shows the server details of a failed statement
// under PTAH_SERVER_DETAILS, the variable native `migrations up
// --server-details` binds, because the compatibility surface takes no flag the
// community binary does not register.

// writeFailingApplyDir hashes a directory whose second migration inserts into
// a table no migration creates.
func writeFailingApplyDir(c *qt.C, dir string) string {
	c.Helper()
	migrationsDir := filepath.Join(dir, "migrations")
	atlastest.WriteAtlasApplyProjectMigration(c, migrationsDir, "20260801000001_create_users.sql",
		"CREATE TABLE users (id INTEGER PRIMARY KEY);\n")
	atlastest.WriteAtlasApplyProjectMigration(c, migrationsDir, "20260801000002_missing.sql",
		"INSERT INTO missing (id) VALUES (1);\n")
	atlastest.WriteAtlasApplyProjectSum(c, migrationsDir)
	return migrationsDir
}

// unsetServerDetailsEnv removes PTAH_SERVER_DETAILS for the duration of a
// test, as unsetSkipChecksEnv does for its variable.
func unsetServerDetailsEnv(t *testing.T) {
	t.Helper()
	t.Setenv("PTAH_SERVER_DETAILS", "")
	if err := os.Unsetenv("PTAH_SERVER_DETAILS"); err != nil {
		t.Fatalf("unset PTAH_SERVER_DETAILS: %v", err)
	}
}

func TestMigrateApplyServerDetailsEnv_HappyPath(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	migrationsDir := writeFailingApplyDir(c, dir)
	t.Setenv("PTAH_SERVER_DETAILS", "1")

	out, err := atlastest.RunCompatOutput("migrate", "apply",
		"--url", "sqlite://"+filepath.Join(dir, "apply.db"), "--dir", "file://"+migrationsDir)

	c.Assert(err, qt.IsNotNil)
	c.Assert(out, qt.Contains, "server details (may contain application data):\n"+
		"  failed at: statement 1 of 1 of migration 20260801000002 (20260801000002_missing.sql)\n"+
		"  server: no details reported")
}

// The default surface is the community binary's: the error line alone.
func TestMigrateApplyWithoutServerDetailsEnv(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	migrationsDir := writeFailingApplyDir(c, dir)
	unsetServerDetailsEnv(t)

	out, err := atlastest.RunCompatOutput("migrate", "apply",
		"--url", "sqlite://"+filepath.Join(dir, "apply.db"), "--dir", "file://"+migrationsDir)

	c.Assert(err, qt.IsNotNil)
	c.Assert(out, qt.Not(qt.Contains), "server details")
}

// A value that is not a boolean is refused before the run opens the database,
// which for SQLite is what creates its file.
func TestMigrateApplyServerDetailsEnv_FailurePath(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	migrationsDir := writeFailingApplyDir(c, dir)
	dbPath := filepath.Join(dir, "apply.db")
	t.Setenv("PTAH_SERVER_DETAILS", "yes")

	_, err := atlastest.RunCompatOutput("migrate", "apply", "--url", "sqlite://"+dbPath, "--dir", "file://"+migrationsDir)

	c.Assert(err, qt.ErrorMatches, `invalid boolean value "yes" for PTAH_SERVER_DETAILS`)
	_, statErr := os.Stat(dbPath)
	c.Assert(statErr, qt.ErrorIs, os.ErrNotExist)
}

// A directory of another layout runs converted, and the details name the file
// as the directory holds it, with and without --format, whose template has no
// field for them.
func TestMigrateApplyServerDetailsEnvNamesAConvertedFile(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{name: "text"},
		{name: "format", args: []string{"--format", "{{ json . }}"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := filepath.Join(t.TempDir(), "migrations")
			writeApplyFiles(c, dir, map[string]string{
				"1_a.up.sql":   "CREATE TABLE a (id int);\n",
				"1_a.down.sql": "DROP TABLE a;\n",
				"2_b.up.sql":   "INSERT INTO missing (id) VALUES (1);\n",
				"2_b.down.sql": "DELETE FROM missing;\n",
			})
			hashConvertedApplyDir(c, dir, "golang-migrate")
			t.Setenv("PTAH_SERVER_DETAILS", "1")

			out, err := atlastest.RunCompatOutput(append([]string{"migrate", "apply",
				"--url", "sqlite://" + filepath.Join(t.TempDir(), "apply.db"),
				"--dir", "file://" + dir + "?format=golang-migrate"}, test.args...)...)

			c.Assert(err, qt.IsNotNil)
			c.Assert(out, qt.Contains, "  failed at: statement 1 of 1 of migration 2 (2_b.up.sql)\n")
		})
	}
}

// `ptah-compat migrate down` shows the details of a down statement that
// failed. Without --format it forwards to native `ptah migrations down`,
// whose --server-details reads the same variable; with it, it runs the
// rollback itself and reads the variable itself.
func TestMigrateDownServerDetailsEnv_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{name: "text"},
		{name: "format", args: []string{"--format", "{{ json . }}"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := t.TempDir()
			migrationsDir := filepath.Join(dir, "migrations")
			dbPath := filepath.Join(dir, "down.db")
			writeMigrateDownFixtureWithRollback(c, migrationsDir, dbPath, failingRollbackSQL)
			t.Setenv("PTAH_SERVER_DETAILS", "1")

			out, err := atlastest.RunCompatOutput(append([]string{"migrate", "down",
				"--url", "sqlite://" + dbPath, "--dir", "file://" + migrationsDir, "--to-version", "1"}, test.args...)...)

			c.Assert(err, qt.IsNotNil)
			c.Assert(out, qt.Contains, "server details (may contain application data):\n"+
				"  failed at: statement 1 of 1 of migration 2 (2_add_audit.down.sql)\n")
		})
	}
}

// A malformed value is refused under --format too, before the directory is
// read.
func TestMigrateDownServerDetailsEnv_FailurePath(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	dbPath := filepath.Join(dir, "down.db")
	writeMigrateDownFixtureWithRollback(c, migrationsDir, dbPath, failingRollbackSQL)
	t.Setenv("PTAH_SERVER_DETAILS", "yes")

	_, err := atlastest.RunCompatOutput("migrate", "down",
		"--url", "sqlite://"+dbPath, "--dir", "file://"+migrationsDir, "--to-version", "1", "--format", "{{ json . }}")

	c.Assert(err, qt.ErrorMatches, `invalid boolean value "yes" for PTAH_SERVER_DETAILS`)
	c.Assert(atlastest.SqliteAtlasRevisionVersions(c, dbPath), qt.DeepEquals, []string{"1", "2"})
}
