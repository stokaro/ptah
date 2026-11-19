//go:build integration

package integration_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// TestMigrateLintDropOfATableAViewReadsE2E lints, on a MySQL-family dev
// database, a version that drops a table an earlier version's view reads.
// The replay leaves a view the server can no longer open, and the cleanup
// after the replay has to drop it: both surfaces report the drop, exit 1 for
// the finding, and leave the dev database empty. The pinned community binary
// reports `-- L1: Dropping table "t"` (DS102) and exits 1 there on MySQL 8.4.
// On MariaDB 12.3 it reports no diagnostic for this drop and exits 0, with
// the view and without it, where ptah-compat reports the drop on both engines.
func TestMigrateLintDropOfATableAViewReadsE2E(t *testing.T) {
	c := qt.New(t)
	root := e2eRepoRoot(t)
	binaries := c.TempDir()
	ptahBinary := filepath.Join(binaries, "ptah")
	compatBinary := filepath.Join(binaries, "ptah-compat")
	buildPtah(c, t.Context(), root, ptahBinary)
	buildPtahCompat(c, t.Context(), root, compatBinary)
	engines := []struct {
		name   string
		engine dbtarget.Engine
	}{
		{name: "mysql", engine: dbtarget.MySQLAdmin},
		{name: "mariadb", engine: dbtarget.MariaDBAdmin},
	}
	for _, test := range engines {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
			defer cancel()
			scratch := newMySQLFamilyScratch(c, test.engine)
			devName, dev := scratch.database(c, "lint_view_drop")
			dir := c.TempDir()
			writeLintE2EFile(c, dir, "20240101000000_init.sql",
				"CREATE TABLE t (id int PRIMARY KEY);\nCREATE VIEW v AS SELECT id FROM t;\n")
			writeLintE2EFile(c, dir, "20240102000000_drop.sql", "DROP TABLE t;\n")
			_, err := runCompatVerb("migrate", "hash", "--dir", "file://"+dir)
			c.Assert(err, qt.IsNil)

			stdout, stderr, err := runLintE2EBinary(ctx, compatBinary,
				"migrate", "lint", "--dir", "file://"+dir, "--dev-url", dev, "--latest", "1")

			c.Assert(exitStatusOf(c, err), qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
			c.Assert(stdout, qt.Contains, "      -- L1: Dropping table \"t\" https://atlasgo.io/lint/analyzers#DS102\n")
			c.Assert(stdout, qt.Contains, "  -- 1 schema change\n  -- 1 diagnostic\n")
			c.Assert(stderr, qt.Equals, "")
			c.Assert(mysqlDatabaseObjects(c, scratch, devName), qt.Equals, 0)

			stdout, stderr, err = runLintE2EBinary(ctx, ptahBinary,
				"migrations", "lint", "--dir", dir, "--dev-url", dev, "--latest", "1")

			c.Assert(exitStatusOf(c, err), qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
			c.Assert(stdout, qt.Contains, "20240102000000_drop.sql:1 [error] DS101: DROP TABLE permanently deletes table t")
			c.Assert(mysqlDatabaseObjects(c, scratch, devName), qt.Equals, 0)
		})
	}
}

// mysqlDatabaseObjects counts the tables and views of one database.
func mysqlDatabaseObjects(c *qt.C, scratch mysqlScratch, database string) int {
	c.Helper()
	var objects int
	err := scratch.admin.QueryRowContext(c.Context(),
		"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ?", database,
	).Scan(&objects)
	c.Assert(err, qt.IsNil)
	return objects
}
