package atlas_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// setEngineFixture writes a one-file Atlas directory with its atlas.sum and
// returns the --dir and --url a `migrate set` reads.
func setEngineFixture(c *qt.C) (dir, url string) {
	c.Helper()
	root := c.TempDir()
	migrations := filepath.Join(root, "migrations")
	c.Assert(os.MkdirAll(migrations, 0o750), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrations, "1_a.sql"), []byte("CREATE TABLE a (id int);\n"), 0o600), qt.IsNil)
	dir = "file://" + filepath.ToSlash(migrations)
	_, stderr, code := runCompat(c, "migrate", "hash", "--dir", dir)
	c.Assert(code, qt.Equals, 0, qt.Commentf("stderr:\n%s", stderr))
	return dir, "sqlite://" + filepath.ToSlash(filepath.Join(root, "set.db"))
}

// TestAtlasMigrateSetMigrationsEngine_FailurePath runs `migrate set` with
// PTAH_MIGRATIONS_ENGINE naming an engine SQLite has no clause for. The engine
// reaches the revision table's CREATE, as it does for `migrate apply`, and
// SQLite refuses the statement; a set that dropped the variable would create a
// table with the dialect's default and record the version.
func TestAtlasMigrateSetMigrationsEngine_FailurePath(t *testing.T) {
	c := qt.New(t)
	dir, url := setEngineFixture(c)
	t.Setenv("PTAH_MIGRATIONS_ENGINE", "MergeTree")

	_, stderr, code := runCompat(c, "migrate", "set", "1", "--url", url, "--dir", dir)

	c.Assert(code, qt.Equals, 1)
	c.Assert(stderr, qt.Contains, "failed to create migrations table with ENGINE = MergeTree")
}

// TestAtlasMigrateSetMigrationsEngine_HappyPath is the control: the same set
// with no engine named records the version.
func TestAtlasMigrateSetMigrationsEngine_HappyPath(t *testing.T) {
	c := qt.New(t)
	dir, url := setEngineFixture(c)
	t.Setenv("PTAH_MIGRATIONS_ENGINE", "MergeTree")
	c.Assert(os.Unsetenv("PTAH_MIGRATIONS_ENGINE"), qt.IsNil)

	_, stderr, code := runCompat(c, "migrate", "set", "1", "--url", url, "--dir", dir)

	c.Assert(code, qt.Equals, 0, qt.Commentf("stderr:\n%s", stderr))
}
