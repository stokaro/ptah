package migratetests_test

import (
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/atlas/internal/atlastest"
)

// TestCompatMigrateApply_OneVersionSpelledTwoWaysRefuses pins a retained
// divergence. The pinned community binary v1.3.0 applies 1_a.sql beside
// 001_b.sql as two revisions, 001 and then 1, because it keys revisions on the
// spelling and orders files by name. Ptah runs migrations in numeric order, so
// the pair ties, and it refuses the directory before any migration runs rather
// than choose an order the directory does not state.
func TestCompatMigrateApply_OneVersionSpelledTwoWaysRefuses(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	dir := writeConvertedApplyDir(c, filepath.Join(root, "migrations"), map[string]string{
		"1_a.sql":   "CREATE TABLE spelled_a (id INTEGER PRIMARY KEY);\n",
		"001_b.sql": "CREATE TABLE spelled_b (id INTEGER PRIMARY KEY);\n",
	})
	hashOut, hashErrOut, hashErr := atlastest.RunCompat("migrate", "hash", "--dir", "file://"+dir)
	c.Assert(hashErr, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", hashOut, hashErrOut))
	dbPath := filepath.Join(root, "apply.db")

	stdout, stderr, err := compatApply(dir, dbPath)

	c.Assert(err, qt.ErrorMatches, `.*Atlas migration files 001_b\.sql and 1_a\.sql spell version 1 two ways, "001" and "1": `+
		`the revision table records the spelling and migrations run in numeric order, so one version needs one spelling`)
	c.Assert(stdout, qt.Equals, "")
	c.Assert(stderr, qt.Equals, "Error: "+err.Error()+"\n")
	c.Assert(atlastest.CompatTableNames(c, dbPath), qt.HasLen, 0)
}
