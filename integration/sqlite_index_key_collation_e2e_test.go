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
			c.Assert(result.Stderr, qt.Equals,
				run.prefix+sqliteDiffReferenceFaultLine+`; unique index "pc" keys code COLLATE NOCASE rather than a column`+"\n")
			c.Assert(result.Stdout, qt.Equals, "")
			_, err := db.ExecContext(c.Context(), "INSERT INTO child VALUES (1, 'a')")
			c.Assert(err, qt.ErrorMatches, `.*foreign key mismatch - "child" referencing "parent".*`)
		})
	}
}

// TestSchemaDiffReadsACollatedKeyInAnySpelling_HappyPathE2E diffs a database
// whose index spells the collated key one way against a document that spells
// it another. sqlite3 3.51.0 reports column code and collation NOCASE in
// pragma_index_xinfo for every row, and the community binary v1.3.0 reports
// each pair synced; so does the first run here.
func TestSchemaDiffReadsACollatedKeyInAnySpelling_HappyPathE2E(t *testing.T) {
	const parent = `CREATE TABLE parent (id INTEGER PRIMARY KEY, code TEXT NOT NULL, "my code" TEXT);` + "\n"
	tests := []struct {
		name     string
		database string
		document string
	}{
		{name: "a quoted column", database: `("code" COLLATE NOCASE)`, document: "(code COLLATE NOCASE)"},
		{name: "an ASC", database: "(code COLLATE NOCASE ASC)", document: "(code COLLATE NOCASE)"},
		{name: "a bracketed column", database: "([code] COLLATE NOCASE)", document: "(code COLLATE NOCASE)"},
		{name: "a comment", database: "(code COLLATE /*x*/ NOCASE)", document: "(code COLLATE NOCASE)"},
		{name: "a quoted column that descends", database: `("code" COLLATE NOCASE DESC)`, document: "(code COLLATE NOCASE DESC)"},
		{name: "a bracketed name with a space", database: "([my code] COLLATE NOCASE)", document: `("my code" COLLATE NOCASE)`},
		{name: "a document that quotes and comments", database: "(code COLLATE NOCASE)", document: `("code"  COLLATE /*y*/ NOCASE ASC)`},
		{name: "a document that quotes the collation", database: "(code COLLATE NOCASE)", document: `(code COLLATE "NOCASE")`},
		{name: "a database that brackets the collation", database: "(code COLLATE [NOCASE])", document: "(code COLLATE NOCASE)"},
	}
	for _, tt := range tests {
		for _, run := range documentDiffRuns {
			t.Run(tt.name+"/"+run.name, func(t *testing.T) {
				c := qt.New(t)
				root := c.TempDir()
				_, url := sqliteCollationDatabase(c, root, parent+"CREATE UNIQUE INDEX pc ON parent "+tt.database+";\n")
				c.Assert(os.WriteFile(filepath.Join(root, "schema.sql"),
					[]byte(parent+"CREATE UNIQUE INDEX pc ON parent "+tt.document+";\n"), 0o600), qt.IsNil)

				result := runSQLiteCollationDiff(c, run.target, root, run.env, url, "file://schema.sql")

				c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", result.Stderr))
				c.Assert(result.Stdout, qt.Equals, "Schemas are synced, no changes to be made.\n")
			})
		}
	}
}
