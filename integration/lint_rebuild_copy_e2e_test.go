//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// rebuildCopyBase is the table every copy below replaces. Its column order is
// what the dev database reports, and a copy that names no column is checked
// against that order.
const rebuildCopyBase = "CREATE TABLE items (id INTEGER PRIMARY KEY, n INTEGER, label TEXT);\n"

// lintRebuildCopy writes the base migration and a SQLite rebuild of items whose
// replacement declares createColumns and is filled by copyStatement, runs
// `ptah migrations lint` against an in-memory dev database, and returns the run.
func lintRebuildCopy(c *qt.C, createColumns, copyStatement string) clirun.Result {
	c.Helper()
	dir := c.TempDir()
	migrations := filepath.Join(dir, "migrations")
	c.Assert(os.Mkdir(migrations, 0o750), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrations, "1.sql"), []byte(rebuildCopyBase), 0o600), qt.IsNil)
	rebuild := "CREATE TABLE new_items (" + createColumns + ");\n" +
		copyStatement + ";\n" +
		"DROP TABLE items;\n" +
		"ALTER TABLE new_items RENAME TO items;\n"
	c.Assert(os.WriteFile(filepath.Join(migrations, "2.sql"), []byte(rebuild), 0o600), qt.IsNil)
	return clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir, Env: environmentWithoutPtahVariables()},
		"migrations", "lint", "--dir", "migrations", "--dev-url", "sqlite://dev?mode=memory", "--latest", "1")
}

// TestMigrationsLintReportsACopyThatChangesValuesE2E lints rebuilds whose copy
// keeps every row and still changes values. The INSERT pairs by position, so a
// SELECT * into a replacement that declares the columns in another order moves
// n into label and label into n, and an INSERT with no column list fills the
// replacement's columns in CREATE TABLE order. A word after a column is an
// alias only when it is a name: SQLite reads `label ISNULL` as
// `label IS NULL`. Each drop loses values, so DS101 reports it.
func TestMigrationsLintReportsACopyThatChangesValuesE2E(t *testing.T) {
	tests := []struct {
		name          string
		createColumns string
		copyStatement string
	}{
		{
			name:          "SELECT * into the columns in another order",
			createColumns: "id INTEGER PRIMARY KEY, label TEXT, n INTEGER",
			copyStatement: "INSERT INTO new_items SELECT * FROM items",
		},
		{
			name:          "no column list, NULL in a column's place",
			createColumns: "id INTEGER PRIMARY KEY, n INTEGER, label TEXT",
			copyStatement: "INSERT INTO new_items SELECT id, n, NULL FROM items",
		},
		{
			name:          "a postfix ISNULL after a column",
			createColumns: "id INTEGER PRIMARY KEY, n INTEGER, label TEXT",
			copyStatement: "INSERT INTO new_items (id, n, label) SELECT id, n, label ISNULL FROM items",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := lintRebuildCopy(c, test.createColumns, test.copyStatement)

			c.Assert(got.ExitCode, qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stdout, qt.Contains, "migrations/2.sql:3 [error] DS101: DROP TABLE permanently deletes table items")
		})
	}
}

// TestMigrationsLintReadsAFaithfulPositionalCopyE2E is the control for the test
// above: a copy that names no column and puts every old column in the place the
// replacement declares it keeps the table, so the run reports nothing. A
// matcher that refused every copy without a column list would fail here.
func TestMigrationsLintReadsAFaithfulPositionalCopyE2E(t *testing.T) {
	tests := []struct {
		name          string
		copyStatement string
	}{
		{name: "SELECT *", copyStatement: "INSERT INTO new_items SELECT * FROM items"},
		{name: "each column in its place", copyStatement: "INSERT INTO new_items SELECT id, n, label FROM items"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := lintRebuildCopy(c, "id INTEGER PRIMARY KEY, n INTEGER, label TEXT", test.copyStatement)

			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stdout, qt.Contains, "No lint findings.")
		})
	}
}
