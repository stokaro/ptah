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

// A SQLite index key can carry a COLLATE of its own, `code COLLATE NOCASE`,
// and a read of the database keeps it as the key's text, the shape a document
// gives the same key. Read as the column alone, the key compares equal to the
// column: a database holding it diffs as synced against a document without
// it, a document holding it is planned again on every run, and a --to database
// whose foreign key references it is planned as a foreign key over an index
// with the column's collation. The community binary v1.3.0 does the first and
// the third. SQLite needs no server, so the rows run on every platform.

// sqliteCollationParent declares parent with code in the default BINARY
// collation.
const sqliteCollationParent = "CREATE TABLE parent (id INTEGER PRIMARY KEY, code TEXT NOT NULL);\n"

// sqliteCollatedIndex keys parent.code under NOCASE.
const sqliteCollatedIndex = "CREATE UNIQUE INDEX pc ON parent (code COLLATE NOCASE);\n"

// sqliteCollationDatabase creates a SQLite database file in root from
// statements, on one connection with foreign keys enforced, and returns it
// open with its URL.
func sqliteCollationDatabase(c *qt.C, root, statements string) (*sql.DB, string) {
	c.Helper()
	path := filepath.Join(root, "app.db")
	db, err := sql.Open("sqlite", path)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	db.SetMaxOpenConns(1)
	_, err = db.ExecContext(c.Context(), "PRAGMA foreign_keys = ON;\n"+statements)
	c.Assert(err, qt.IsNil)
	return db, atlasurl.SQLiteURLFromPath(path)
}

// runSQLiteCollationDiff runs `schema diff --from from --to to` in root on an
// in-memory SQLite dev database.
func runSQLiteCollationDiff(c *qt.C, target clirun.Target, root string, env []string, from, to string) clirun.Result {
	c.Helper()
	return clirun.Run(c, target, clirun.Options{Dir: root, Env: slices.Concat(environmentWithoutPtahVariables(), env)},
		"schema", "diff", "--from", from, "--to", to, "--dev-url", "sqlite://dev?mode=memory")
}

// TestSchemaDiffSeesAnIndexKeyCollation_HappyPathE2E diffs a database holding
// the collated key against a document that keys the bare column. The plan
// rebuilds the index without the collation; the community binary reports the
// two as synced.
func TestSchemaDiffSeesAnIndexKeyCollation_HappyPathE2E(t *testing.T) {
	for _, run := range documentDiffRuns {
		t.Run(run.name, func(t *testing.T) {
			c := qt.New(t)
			root := c.TempDir()
			_, url := sqliteCollationDatabase(c, root, sqliteCollationParent+sqliteCollatedIndex)
			c.Assert(os.WriteFile(filepath.Join(root, "plain.sql"),
				[]byte(sqliteCollationParent+"CREATE UNIQUE INDEX pc ON parent (code);\n"), 0o600), qt.IsNil)

			result := runSQLiteCollationDiff(c, run.target, root, run.env, url, "file://plain.sql")

			c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", result.Stderr))
			c.Assert(result.Stdout, qt.Contains, `CREATE UNIQUE INDEX "pc" ON "parent" ("code");`)
			c.Assert(result.Stdout, qt.Not(qt.Contains), "Schemas are synced")
		})
	}
}

// TestSchemaDiffConvergesOnACollatedIndexKey_HappyPathE2E plans a database
// with a bare key toward the document that collates it, runs the plan, reads
// the collation back from SQLite, and diffs again: the second run plans
// nothing.
func TestSchemaDiffConvergesOnACollatedIndexKey_HappyPathE2E(t *testing.T) {
	for _, run := range documentDiffRuns {
		t.Run(run.name, func(t *testing.T) {
			c := qt.New(t)
			root := c.TempDir()
			db, url := sqliteCollationDatabase(c, root, sqliteCollationParent+"CREATE UNIQUE INDEX pc ON parent (code);\n")
			c.Assert(os.WriteFile(filepath.Join(root, "collated.sql"), []byte(sqliteCollationParent+sqliteCollatedIndex), 0o600), qt.IsNil)

			plan := runSQLiteCollationDiff(c, run.target, root, run.env, url, "file://collated.sql")

			c.Assert(plan.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", plan.Stderr))
			_, err := db.ExecContext(c.Context(), plan.Stdout)
			c.Assert(err, qt.IsNil, qt.Commentf("plan:\n%s", plan.Stdout))
			var collation string
			c.Assert(db.QueryRowContext(c.Context(), "SELECT coll FROM pragma_index_xinfo('pc') WHERE key = 1").Scan(&collation), qt.IsNil)
			c.Assert(collation, qt.Equals, "NOCASE")
			again := runSQLiteCollationDiff(c, run.target, root, run.env, url, "file://collated.sql")
			c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", again.Stderr))
			c.Assert(again.Stdout, qt.Equals, "Schemas are synced, no changes to be made.\n")
		})
	}
}

// TestSchemaDiffRefusesAForeignKeyOverACollatedKey_FailurePathE2E plans toward
// a database whose child references code through the NOCASE index over a
// BINARY column. SQLite holds that state and refuses every child row with
// `foreign key mismatch`, which the control reads from the same database, so
// the diff refuses it as it refuses the same --to document. The community
// binary plans the index as `(code)`, without its collation.
func TestSchemaDiffRefusesAForeignKeyOverACollatedKey_FailurePathE2E(t *testing.T) {
	for _, run := range documentDiffRuns {
		t.Run(run.name, func(t *testing.T) {
			c := qt.New(t)
			root := c.TempDir()
			db, url := sqliteCollationDatabase(c, root, sqliteCollationParent+sqliteCollatedIndex+
				"CREATE TABLE child (id INTEGER PRIMARY KEY, code TEXT REFERENCES parent(code));\nINSERT INTO parent VALUES (1, 'a');")
			c.Assert(os.WriteFile(filepath.Join(root, "parent.sql"), []byte(sqliteCollationParent), 0o600), qt.IsNil)

			result := runSQLiteCollationDiff(c, run.target, root, run.env, "file://parent.sql", url)

			c.Assert(result.ExitCode, qt.Equals, run.wantExit, qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
			c.Assert(result.Stderr, qt.Equals, run.prefix+sqliteDiffReferenceFaultLine+"\n")
			c.Assert(result.Stdout, qt.Equals, "")
			_, err := db.ExecContext(c.Context(), "INSERT INTO child VALUES (1, 'a')")
			c.Assert(err, qt.ErrorMatches, `.*foreign key mismatch - "child" referencing "parent".*`)
		})
	}
}
