//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// SQLite's ALTER TABLE cannot change a column's type, so `migrations generate`
// writes a rebuild: a new table, a copy of every row, a DROP TABLE of the old
// one and a rename of the copy. `migrations lint` reads the file it wrote as a
// rebuild rather than as a lost table: the copy keeps the rows under the old
// name. Without a dev database the run says that the copy was not checked
// against the old table's columns.
func TestMigrationsLintReadsAGeneratedSQLiteRebuildFS(t *testing.T) {
	c := qt.New(t)
	url := sqliteScratchURL(c)
	dir := c.TempDir()
	migrations := filepath.Join(dir, "migrations")
	first := filepath.Join(dir, "first.sql")
	second := filepath.Join(dir, "second.sql")
	c.Assert(os.WriteFile(first, []byte("CREATE TABLE items (id INTEGER PRIMARY KEY, n INTEGER, label TEXT);\n"), 0o600),
		qt.IsNil)
	c.Assert(os.WriteFile(second, []byte("CREATE TABLE items (id INTEGER PRIMARY KEY, n TEXT NOT NULL, label TEXT);\n"),
		0o600), qt.IsNil)
	out, err := runPtahNativeWithError("migrations", "generate", "--db-url", url, "--schema-file", first,
		"--migrations-dir", migrations, "--name", "first")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	out, err = runPtahNativeWithError("migrations", "up", "--db-url", url, "--migrations-dir", migrations)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	out, err = runPtahNativeWithError("migrations", "generate", "--db-url", url, "--schema-file", second,
		"--migrations-dir", migrations, "--name", "second")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	ups, err := filepath.Glob(filepath.Join(migrations, "*_second.up.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(ups, qt.HasLen, 1)
	body, err := os.ReadFile(ups[0])
	c.Assert(err, qt.IsNil)
	c.Assert(string(body), qt.Contains, "SQLite table rebuild")

	linted, lintErr := runPtahNativeWithError("migrations", "lint", "--dir", migrations, "--dialect", "sqlite")

	c.Assert(lintErr, qt.IsNil, qt.Commentf("%s", linted))
	c.Assert(linted, qt.Contains, "No lint findings.")
	c.Assert(linted, qt.Contains, "warning: DS101 ran without the baseline schema")
}
