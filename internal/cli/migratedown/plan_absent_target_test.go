package migratedown_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/migrateup"
)

// `migrations down --target` accepts a version no migration file names and
// rolls back every migration above it. A rollback derived with --plan lands
// where the one through the down files does: on the newest migration below
// the target, which is the newest one the derived statements leave in place.

// gapDir is a directory of versions 10 and 20, each with a down file, applied
// to a fresh SQLite database. No migration is numbered 15.
func gapDir(c *qt.C) (dir, dbPath string) {
	c.Helper()
	root := c.TempDir()
	dir = filepath.Join(root, "migrations")
	c.Assert(os.MkdirAll(dir, 0o750), qt.IsNil)
	for name, body := range map[string]string{
		"10_ten.up.sql":      "CREATE TABLE ten (id INTEGER PRIMARY KEY);\n",
		"10_ten.down.sql":    "DROP TABLE ten;\n",
		"20_twenty.up.sql":   "CREATE TABLE twenty (id INTEGER PRIMARY KEY);\n",
		"20_twenty.down.sql": "DROP TABLE twenty;\n",
	} {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600), qt.IsNil)
	}
	dbPath = filepath.Join(root, "gap.db")
	up := migrateup.NewMigrateUpCommand()
	var out bytes.Buffer
	up.SetOut(&out)
	up.SetErr(&out)
	up.SetArgs([]string{"--db-url", "sqlite://" + dbPath, "--migrations-dir", dir})
	c.Assert(up.Execute(), qt.IsNil, qt.Commentf("%s", out.String()))
	return dir, dbPath
}

// TestMigrateDownPlanToATargetNoFileNames_HappyPath rolls back to version 15
// with a derived plan. It drops the table of version 20 and leaves version 10
// applied: no statement runs that the revision table then fails to record.
func TestMigrateDownPlanToATargetNoFileNames_HappyPath(t *testing.T) {
	c := qt.New(t)
	dir, dbPath := gapDir(c)
	f := documentRollback{dir: dir, dbURL: "sqlite://" + dbPath}

	stdout, stderr, err := f.down("--target", "15", "--confirm", "--plan", "--shadow-db", devURL(c))

	assertRolledBackToTen(c, dbPath, stdout, stderr, err)
	c.Assert(stdout, qt.Matches, `(?s).*Derived rollback plan: 1 statements\n  DROP TABLE "twenty"[^\n]*\n.*`)
}

// TestMigrateDownToATargetNoFileNames_HappyPath is the control: the rollback
// through the down files lands on the same version.
func TestMigrateDownToATargetNoFileNames_HappyPath(t *testing.T) {
	c := qt.New(t)
	dir, dbPath := gapDir(c)
	f := documentRollback{dir: dir, dbURL: "sqlite://" + dbPath}

	stdout, stderr, err := f.down("--target", "15", "--confirm")

	assertRolledBackToTen(c, dbPath, stdout, stderr, err)
}

// assertRolledBackToTen asserts that a rollback of gapDir to version 15
// succeeded and left version 10 applied and its table alone in place.
func assertRolledBackToTen(c *qt.C, dbPath, stdout, stderr string, err error) {
	c.Helper()
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(stdout, qt.Contains, "Database is now at version: 10")
	c.Assert(tableCensus(c, dbPath), qt.DeepEquals, []string{"schema_migrations", "schema_migrations_log", "ten"})
	c.Assert(appliedVersions(c, dbPath), qt.DeepEquals, []int64{10})
}
