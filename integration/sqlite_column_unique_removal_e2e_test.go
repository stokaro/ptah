//go:build integration

package integration_test

import (
	"database/sql"
	"os"
	"path/filepath"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasurl"
	"ptah.run/internal/clirun"
)

// A UNIQUE a SQLite column declares is an index SQLite builds for itself,
// sqlite_autoindex_parent_1, which DROP INDEX refuses. Removing it means
// rebuilding the table. The community binary v1.3.0 plans the removal from
// sqliteUniqueRemovalKeyed to sqliteUniqueRemovalSound as DROP INDEX
// `parent_code` and a rebuild of child; on a database created from
// sqliteUniqueRemovalKeyed that plan fails at its second statement with `no
// such index: parent_code`, and `atlas schema apply` exits 1 there. Ptah
// rebuilds parent too. The test applies Ptah's plan to such a database, reads
// the rows and the keys back, and diffs the database against the document.

// sqliteUniqueRemovalKeyed declares parent.code UNIQUE on the column and child
// referencing it.
const sqliteUniqueRemovalKeyed = "CREATE TABLE parent (id INTEGER PRIMARY KEY, code TEXT NOT NULL UNIQUE);\n" +
	"CREATE TABLE child (id INTEGER PRIMARY KEY, code TEXT REFERENCES parent(code));\n"

// sqliteUniqueRemovalSound is sqliteUniqueRemovalKeyed without the UNIQUE
// and without the foreign key.
const sqliteUniqueRemovalSound = "CREATE TABLE parent (id INTEGER PRIMARY KEY, code TEXT NOT NULL);\n" +
	"CREATE TABLE child (id INTEGER PRIMARY KEY, code TEXT);\n"

// TestSchemaDiffRemovesAColumnUniqueByRebuild_HappyPathE2E runs the plan of
// every surface of [documentDiffRuns] on a database holding a row in each
// table.
func TestSchemaDiffRemovesAColumnUniqueByRebuild_HappyPathE2E(t *testing.T) {
	for _, run := range documentDiffRuns {
		t.Run(run.name, func(t *testing.T) {
			c := qt.New(t)
			root := c.TempDir()
			path := filepath.Join(root, "app.db")
			db, err := sql.Open("sqlite", path)
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
			db.SetMaxOpenConns(1)
			_, err = db.ExecContext(c.Context(), "PRAGMA foreign_keys = ON;\n"+sqliteUniqueRemovalKeyed+
				"INSERT INTO parent VALUES (1, 'a'); INSERT INTO child VALUES (1, 'a');")
			c.Assert(err, qt.IsNil)

			plan := runSQLiteDocumentDiff(c, run.target, sqliteUniqueRemovalKeyed, sqliteUniqueRemovalSound, run.env)

			c.Assert(plan.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", plan.Stderr))
			c.Assert(plan.Stdout, qt.Not(qt.Contains), "DROP INDEX")
			_, err = db.ExecContext(c.Context(), plan.Stdout)
			c.Assert(err, qt.IsNil, qt.Commentf("plan:\n%s", plan.Stdout))
			c.Assert(readSQLiteCount(c, db, "SELECT count(*) FROM parent WHERE id = 1 AND code = 'a'"), qt.Equals, 1)
			c.Assert(readSQLiteCount(c, db, "SELECT count(*) FROM child WHERE id = 1 AND code = 'a'"), qt.Equals, 1)
			c.Assert(readSQLiteCount(c, db, "SELECT count(*) FROM pragma_index_list('parent')"), qt.Equals, 0)
			c.Assert(readSQLiteCount(c, db, "SELECT count(*) FROM pragma_foreign_key_list('child')"), qt.Equals, 0)
			c.Assert(os.WriteFile(filepath.Join(root, "sound.sql"), []byte(sqliteUniqueRemovalSound), 0o600), qt.IsNil)
			again := clirun.Run(c, run.target, clirun.Options{Dir: root, Env: slices.Concat(environmentWithoutPtahVariables(), run.env)},
				"schema", "diff", "--from", atlasurl.SQLiteURLFromPath(path), "--to", "file://sound.sql", "--dev-url", "sqlite://dev?mode=memory")
			c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", again.Stderr))
			c.Assert(again.Stdout, qt.Equals, "Schemas are synced, no changes to be made.\n")
		})
	}
}
