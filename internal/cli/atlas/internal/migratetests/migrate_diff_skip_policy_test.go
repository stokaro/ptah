package migratetests_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/atlas/internal/atlastest"
)

// skipPolicyMigrateProject is a migration directory that creates t(id, a, b)
// with an index on a, a desired schema of t(id, a, c), and an atlas.hcl env
// carrying policy. On SQLite, dropping b is a table rebuild, which loses t_a
// as well, so an unskipped diff writes a rebuild.
func skipPolicyMigrateProject(c *qt.C, policy string) string {
	c.Helper()
	dir := c.TempDir()
	c.Chdir(dir)
	// The directory sits in the project, which is the root migrate diff
	// confines it to.
	migrations := filepath.Join(dir, "migrations")
	c.Assert(os.MkdirAll(migrations, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrations, "20260101000000_base.sql"),
		[]byte("CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, b TEXT);\nCREATE INDEX t_a ON t(a);\n"), 0o600), qt.IsNil)
	hashMigrationDir(c, migrations)
	c.Assert(os.WriteFile("schema.sql", []byte("CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, c TEXT);\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile("atlas.hcl", []byte(`env "local" {
  dev = "sqlite://`+filepath.ToSlash(filepath.Join(dir, "dev.db"))+`"
  src = "file://schema.sql"
  migration {
    dir = "file://`+filepath.ToSlash(migrations)+`"
  }
`+policy+`}
`), 0o600), qt.IsNil)
	return migrations
}

// newMigration reads the one file a run added to migrations beside the base.
func newMigration(c *qt.C, migrations string) string {
	c.Helper()
	matches, err := filepath.Glob(filepath.Join(migrations, "*_next.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(matches, qt.HasLen, 1)
	data, err := os.ReadFile(matches[0])
	c.Assert(err, qt.IsNil)
	return string(data)
}

// TestMigrateDiffUsesAtlasProjectDiffSkipDropColumnAndIndex pins the file the
// community binary v1.3.0 writes under this policy, measured on SQLite: the
// column added, nothing dropped, and no word about the drops it withheld,
// since the file is what atlas.sum hashes. The omissions are reported on the
// error stream instead.
func TestMigrateDiffUsesAtlasProjectDiffSkipDropColumnAndIndex(t *testing.T) {
	c := qt.New(t)
	migrations := skipPolicyMigrateProject(c, "  diff {\n    skip {\n      drop_index = true\n      drop_column = true\n    }\n  }\n")

	_, stderr, err := atlastest.RunCompatStreams(c, "migrate", "diff", "--env", "local", "next")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", stderr))
	c.Assert(newMigration(c, migrations), qt.Equals, "-- Add column \"c\" to table: \"t\"\nALTER TABLE \"t\" ADD COLUMN \"c\" text NULL;\n")
	c.Assert(stderr, qt.Contains, "SKIP: DROP COLUMN of t.b omitted by diff policy (skip: drop_column)\n"+
		"SKIP: DROP INDEX of t.t_a omitted by diff policy (skip: drop_index)\n")
}

// TestMigrateDiffWithoutAtlasProjectDiffSkipRebuildsTheTable is the control:
// without the policy the same project writes the rebuild that drops b.
func TestMigrateDiffWithoutAtlasProjectDiffSkipRebuildsTheTable(t *testing.T) {
	c := qt.New(t)
	migrations := skipPolicyMigrateProject(c, "")

	_, stderr, err := atlastest.RunCompatStreams(c, "migrate", "diff", "--env", "local", "next")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", stderr))
	c.Assert(newMigration(c, migrations), qt.Contains, `DROP TABLE "t"`)
	c.Assert(stderr, qt.Not(qt.Contains), "SKIP:")
}
