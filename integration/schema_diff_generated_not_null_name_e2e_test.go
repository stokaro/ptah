//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// `ptah schema diff --from <file> --to <database>` with no --dev-url reads the
// database as the desired side. PostgreSQL 18 names every NOT NULL, the ones
// nobody named included, while a file that leaves a NOT NULL unnamed carries
// no name. Each test reads the name the server gave back from the catalog.

// TestSchemaDiffTakesTheGeneratedNotNullNameForNoneE2E diffs a file that
// leaves t1.id's NOT NULL unnamed to a database the same statement built. The
// server named the constraint t1_id_not_null, the name it gives one the file
// leaves unnamed, so the diff says nothing about it.
func TestSchemaDiffTakesTheGeneratedNotNullNameForNoneE2E(t *testing.T) {
	c := qt.New(t)
	database := newEmptyReadScopeDatabase(c, "not_null_name")
	execPostgresFamilySQL(c, c.Context(), database, "CREATE TABLE t1 (id integer PRIMARY KEY)")
	file := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(file, []byte("CREATE TABLE t1 (id integer PRIMARY KEY);\n"), 0o600), qt.IsNil)

	diff := runPtahIn(c, "schema", "diff", "--from", "file://"+file, "--to", database)

	c.Assert(diff.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", diff.Stdout, diff.Stderr))
	c.Assert(notNullConstraintNames(c, database), qt.Equals, "t1_id_not_null")
	c.Assert(diff.Stdout, qt.Not(qt.Contains), "t1_id_not_null")
	c.Assert(diff.Stdout, qt.Not(qt.Contains), "NOT NULL constraint name")
}

// TestSchemaDiffComparesAWrittenNotNullNameE2E is the control: the file names
// the constraint t1_id_nn, and the database holds the generated name, so the
// diff still speaks about the name.
func TestSchemaDiffComparesAWrittenNotNullNameE2E(t *testing.T) {
	c := qt.New(t)
	database := newEmptyReadScopeDatabase(c, "not_null_name")
	execPostgresFamilySQL(c, c.Context(), database, "CREATE TABLE t1 (id integer PRIMARY KEY)")
	file := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(file, []byte("CREATE TABLE t1 (id integer CONSTRAINT t1_id_nn NOT NULL PRIMARY KEY);\n"), 0o600), qt.IsNil)

	diff := runPtahIn(c, "schema", "diff", "--from", "file://"+file, "--to", database)

	c.Assert(diff.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", diff.Stdout, diff.Stderr))
	c.Assert(notNullConstraintNames(c, database), qt.Equals, "t1_id_not_null")
	c.Assert(diff.Stdout, qt.Contains, `NOT NULL constraint name "t1_id_not_null"`)
}

// notNullConstraintNames lists the names of the NOT NULL constraints of table
// t1 in the public schema, comma-separated in name order.
func notNullConstraintNames(c *qt.C, database string) string {
	c.Helper()
	var names string
	err := openReadScopeDatabase(c, database).QueryRowContext(c.Context(), `
		SELECT COALESCE(string_agg(conname, ',' ORDER BY conname), '')
		FROM pg_constraint
		WHERE contype = 'n' AND conrelid = 'public.t1'::regclass`).Scan(&names)
	c.Assert(err, qt.IsNil)
	return names
}
