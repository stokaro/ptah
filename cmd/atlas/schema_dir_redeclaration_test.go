package atlas_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// redeclaringDir is the fixture both verbs are measured on: two files, the
// second declaring a table the first already declared. The pinned Atlas
// community binary v1.3.0 exits 1 on it, on `schema diff` and on `schema apply`
// alike, because it executes the files in filename order against the dev
// database and the engine refuses the second CREATE TABLE.
func redeclaringDir(tb testing.TB) string {
	c := qt.New(tb)
	c.Helper()
	return writeSchemaSourceDir(c.TB, map[string]string{
		"1_a.sql": "CREATE TABLE redeclared_users (id INTEGER PRIMARY KEY);\n",
		"2_b.sql": "CREATE TABLE redeclared_users (id INTEGER PRIMARY KEY, extra TEXT);\n",
	})
}

// TestSchemaDiffRefusesDirectoryRedeclaration is the regression test for the
// compatibility defect the first cut of the directory source shipped: the files
// were merged as declarations, so this directory rendered a `redeclared_users`
// carrying `extra` -- a table that appears in neither file -- and exited 0 where
// the pinned binary exits 1.
func TestSchemaDiffRefusesDirectoryRedeclaration(t *testing.T) {
	c := qt.New(t)
	dir := redeclaringDir(c.TB)
	emptyHCL := filepath.Join(c.TempDir(), "empty.hcl")
	c.Assert(os.WriteFile(emptyHCL, []byte("schema \"main\" {}\n"), 0o600), qt.IsNil)

	out, err := runAtlasArgs(
		"schema", "diff",
		"--from", "file://"+emptyHCL,
		"--to", "file://"+dir,
		"--dev-url", "sqlite://"+filepath.Join(c.TempDir(), "dev.db"),
	)

	c.Assert(err, qt.ErrorMatches, `.*read state from "2_b\.sql": table "redeclared_users" already exists`)
	c.Assert(out, qt.Not(qt.Contains), "extra")
}

// TestSchemaApplyRefusesDirectoryRedeclaration covers the verb that writes. The
// catalog assertion is the point: before the fix this exited 0 AND created the
// merged table, so a run that should have stopped had already changed the
// target database.
func TestSchemaApplyRefusesDirectoryRedeclaration(t *testing.T) {
	c := qt.New(t)
	workdir := c.TempDir()
	dir := redeclaringDir(c.TB)
	dbPath := filepath.Join(workdir, "target.db")

	out, err := runSchemaApply(c.TB,
		"--url", "sqlite://"+dbPath,
		"--to", "file://"+dir,
		"--dev-url", "sqlite://"+filepath.Join(workdir, "dev.db"),
		"--auto-approve",
	)

	c.Assert(err, qt.ErrorMatches, `.*read state from "2_b\.sql": table "redeclared_users" already exists`)
	c.Assert(out, qt.Not(qt.Contains), "Schema apply completed successfully.")
	c.Assert(sqliteTableCount(c.TB, dbPath, "redeclared_users"), qt.Equals, 0)
}

// TestSchemaApplyAcceptsDirectoryWithDistinctObjects is the control that keeps
// the two refusals honest: the gate separates a redeclaration from an ordinary
// multi-file directory rather than refusing every directory.
func TestSchemaApplyAcceptsDirectoryWithDistinctObjects(t *testing.T) {
	c := qt.New(t)
	workdir := c.TempDir()
	dir := writeSchemaSourceDir(c.TB, map[string]string{
		"1_a.sql": "CREATE TABLE distinct_users (id INTEGER PRIMARY KEY);\n",
		"2_b.sql": "CREATE TABLE distinct_posts (id INTEGER PRIMARY KEY);\n",
	})
	dbPath := filepath.Join(workdir, "target.db")

	out, err := runSchemaApply(c.TB,
		"--url", "sqlite://"+dbPath,
		"--to", "file://"+dir,
		"--dev-url", "sqlite://"+filepath.Join(workdir, "dev.db"),
		"--auto-approve",
	)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(sqliteTableCount(c.TB, dbPath, "distinct_users"), qt.Equals, 1)
	c.Assert(sqliteTableCount(c.TB, dbPath, "distinct_posts"), qt.Equals, 1)
}
