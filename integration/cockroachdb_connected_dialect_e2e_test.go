//go:build integration

package integration_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
)

// A postgres:// URL reaches CockroachDB too, and only the server's banner says
// which one answered. These verbs read the schema file in the dialect the
// server reports: read as PostgreSQL, `CREATE INDEX k_b ON ic (b) NOT VISIBLE`
// is refused as an unsupported statement, and `WHERE a > 0 NOT VISIBLE` keeps
// the clause as part of its condition (stokaro/ptah#3952).

// cockroachHiddenIndexes is the table the rows declare, with k_a a hidden
// partial index and k_b a hidden index.
var cockroachHiddenIndexes = cockroachVisibilitySchema(" NOT VISIBLE", " NOT VISIBLE")

// cockroachBuiltFromFile builds a database from cockroachHiddenIndexes and
// writes the same SQL as the schema file, so the two describe one schema.
func cockroachBuiltFromFile(c *qt.C) (dbURL, schema string) {
	c.Helper()
	ctx, cancel := context.WithTimeout(c.Context(), 5*time.Minute)
	c.Cleanup(cancel)
	dbURL, _ = cockroachVisibilityDatabase(c, ctx, cockroachHiddenIndexes)
	return dbURL, writeCockroachVisibilitySchema(c, cockroachHiddenIndexes)
}

// TestSchemaDriftReadsACockroachDBFileInItsDialectE2E finds no drift between a
// database and the file that built it.
func TestSchemaDriftReadsACockroachDBFileInItsDialectE2E(t *testing.T) {
	c := qt.New(t)
	dbURL, schema := cockroachBuiltFromFile(c)

	out := runPtahNative(c, "schema", "drift", "--db-url", dbURL, "--schema-file", schema)

	c.Assert(out, qt.Contains, "No schema drift detected.")
}

// TestMigrationsPlanReadsACockroachDBFileInItsDialectE2E plans nothing for a
// database and the file that built it.
func TestMigrationsPlanReadsACockroachDBFileInItsDialectE2E(t *testing.T) {
	c := qt.New(t)
	dbURL, schema := cockroachBuiltFromFile(c)

	out := runPtahNative(c, "migrations", "plan", "--db-url", dbURL, "--schema-file", schema)

	c.Assert(out, qt.Contains, "no executable migration statements")
}

// TestMigrationsGenerateReadsACockroachDBFileInItsDialectE2E writes no
// migration for a database and the file that built it.
func TestMigrationsGenerateReadsACockroachDBFileInItsDialectE2E(t *testing.T) {
	c := qt.New(t)
	dbURL, schema := cockroachBuiltFromFile(c)

	out := runPtahNative(c, "migrations", "generate", "--db-url", dbURL, "--schema-file", schema,
		"--migrations-dir", c.TempDir(), "--name", "none")

	c.Assert(out, qt.Contains, "Schema is synced with")
}

// TestMigrationsGenerateReplayReadsACockroachDBFileInItsDialectE2E replays an
// empty migration directory on a CockroachDB dev database and writes the first
// migration: it keeps each index hidden, after the partial index's condition.
func TestMigrationsGenerateReplayReadsACockroachDBFileInItsDialectE2E(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(c.Context(), 5*time.Minute)
	defer cancel()
	devURL, _ := cockroachVisibilityDatabase(c, ctx, "")
	schema := writeCockroachVisibilitySchema(c, cockroachHiddenIndexes)
	dir := c.TempDir()

	runPtahNative(c, "migrations", "generate", "--replay", "--dev-url", devURL, "--schema-file", schema,
		"--migrations-dir", dir, "--name", "init")

	ups, err := filepath.Glob(filepath.Join(dir, "*_init.up.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(ups, qt.HasLen, 1)
	up, err := os.ReadFile(ups[0])
	c.Assert(err, qt.IsNil)
	c.Assert(string(up), qt.Contains, `("a") WHERE a > 0 NOT VISIBLE;`)
	c.Assert(string(up), qt.Contains, `("b") NOT VISIBLE;`)
}
