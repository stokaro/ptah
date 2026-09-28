//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// These run on the PostgreSQL server CI starts, PostgreSQL 18, which holds
// what the dialect default, PostgreSQL 17, cannot: a NOT ENFORCED CHECK and a
// named NOT NULL. schema diff planned for the default, so it refused the first
// on the server that held it and reported naming the second as unavailable on
// the target; it now plans for the server it compares on (stokaro/ptah#3910,
// stokaro/ptah#3936).

// writeSchemaFile writes sql as a schema file and answers its file:// URL.
func writeSchemaFile(c *qt.C, sql string) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(sql), 0o600), qt.IsNil)
	return "file://" + path
}

// TestSchemaDiffPlansForTheDatabaseItReadsE2E diffs a database with a file
// that declares a NOT ENFORCED CHECK, natively: the plan creates it.
func TestSchemaDiffPlansForTheDatabaseItReadsE2E(t *testing.T) {
	c := qt.New(t)
	file := writeSchemaFile(c, `CREATE TABLE t (a int CHECK (a > 0) NOT ENFORCED);`)

	plan := runPtahNative(c, "schema", "diff", "--from", postgresUniqueEngine.emptyDB(c), "--to", file)

	c.Assert(plan, qt.Contains, `CHECK (a > 0) NOT ENFORCED`)
}

// TestCompatSchemaDiffPlansForTheDevServerE2E compares the same file with
// itself through ptah-compat on a dev database: synced.
func TestCompatSchemaDiffPlansForTheDevServerE2E(t *testing.T) {
	c := qt.New(t)
	file := writeSchemaFile(c, `CREATE TABLE t (a int CHECK (a > 0) NOT ENFORCED);`)

	out, err := runCompatVerb("schema", "diff", "--from", file, "--to", file,
		"--dev-url", postgresUniqueEngine.emptyDB(c))

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "Schemas are synced")
}

// TestCompatSchemaDiffRenamesANotNullE2E diffs a database whose NOT NULL is
// named nn with a file naming it nn2, through ptah-compat: the plan renames
// it, as native schema apply does.
func TestCompatSchemaDiffRenamesANotNullE2E(t *testing.T) {
	c := qt.New(t)
	database := postgresUniqueEngine.builtDB(c, `CREATE TABLE c (id int PRIMARY KEY, d int CONSTRAINT nn NOT NULL);`)
	file := writeSchemaFile(c, `CREATE TABLE c (id int PRIMARY KEY, d int CONSTRAINT nn2 NOT NULL);`)

	out, err := runCompatVerb("schema", "diff", "--from", database, "--to", file,
		"--dev-url", postgresUniqueEngine.emptyDB(c))

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, `ALTER TABLE "c" RENAME CONSTRAINT "nn" TO "nn2";`)
}
