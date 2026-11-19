//go:build integration

package integration_test

import (
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// These tests drive the shipped binary over SQLite and an Atlas directory
// whose R__r.sql, the version R, drops the table 1_a.sql creates. Over the
// Atlas revision table the migrator runs R__r.sql last, under the key above
// every numbered file, and the destructive-statement gate of up judges it
// under that key.

// gatedAtlasRepeatable is the directory both tests apply.
func gatedAtlasRepeatable(c *qt.C) string {
	c.Helper()
	return readsWholeDirectory(c, map[string]string{
		"1_a.sql":  "CREATE TABLE ta (id INTEGER);\n",
		"R__r.sql": "DROP TABLE ta;\n",
	})
}

// The gate refuses the plan before anything runs, naming the statement, so
// the revision table records nothing and ta is never created. Without the
// gate selecting R__r.sql by the key the migrator runs it under, up exits 0
// and drops ta without --allow-destructive.
func TestMigrationGate_AnAtlasRepeatableOverTheAtlasTable_FailurePath(t *testing.T) {
	c := qt.New(t)
	dir := gatedAtlasRepeatable(c)

	up := readsWholePtah(c, dir, "up", "--revision-format", "atlas")

	c.Assert(up.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", up.Stdout, up.Stderr))
	c.Assert(up.Stderr, qt.Contains, "migrations/R__r.sql:1 DS101 error: DROP TABLE permanently deletes table ta")
	c.Assert(revisionTableVersions(c, filepath.Join(dir, "app.db")), qt.HasLen, 0)
	c.Assert(sqliteTables(c, filepath.Join(dir, "app.db")), qt.Not(qt.Contains), "ta")
}

// With --allow-destructive the same plan applies both files, R__r.sql last,
// and the revision table records 1 and R.
func TestMigrationGate_AnAtlasRepeatableOverTheAtlasTable_HappyPath(t *testing.T) {
	c := qt.New(t)
	dir := gatedAtlasRepeatable(c)

	up := readsWholePtah(c, dir, "up", "--revision-format", "atlas", "--allow-destructive")

	c.Assert(up.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", up.Stdout, up.Stderr))
	c.Assert(revisionTableVersions(c, filepath.Join(dir, "app.db")), qt.DeepEquals, []string{"1", "R"})
	c.Assert(sqliteTables(c, filepath.Join(dir, "app.db")), qt.Not(qt.Contains), "ta")
}

// A gate that blocks on the migration-form family judges R__r.sql for the
// Atlas revision table the run records versions in, which keeps the version
// R, so it reports no refusal and up applies both files. Judged for Ptah's
// table, the gate would refuse the plan under MF105P for a file the run
// applies.
func TestMigrationGate_AnAtlasRepeatableUnderAMigrationFormGate_HappyPath(t *testing.T) {
	c := qt.New(t)
	dir := readsWholeDirectory(c, map[string]string{
		".ptah-lint.yaml": "gate:\n  families: [DS, MF]\n",
		"1_a.sql":         "CREATE TABLE ta (id INTEGER);\n",
		"R__r.sql":        "CREATE TABLE tr (id INTEGER);\n",
	})

	up := readsWholePtah(c, dir, "up", "--revision-format", "atlas")

	c.Assert(up.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", up.Stdout, up.Stderr))
	c.Assert(revisionTableVersions(c, filepath.Join(dir, "app.db")), qt.DeepEquals, []string{"1", "R"})
}
