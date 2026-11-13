//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestPostgresSetweightGeneratedColumnConvergesLive asks PostgreSQL to store
// the desired expression and compares it again. PostgreSQL inserts its
// internal ::"char" cast around the weight literal on readback (#5982).
func TestPostgresSetweightGeneratedColumnConvergesLive(t *testing.T) {
	c := qt.New(t)
	target, _ := scratchReplayDatabase(c)
	path := filepath.Join(c.TempDir(), "schema.sql")
	source := `CREATE TABLE notes (
  title text NOT NULL,
  search_vector tsvector GENERATED ALWAYS AS
    (setweight(to_tsvector('simple', coalesce(title, '')), 'B')) STORED
);`
	c.Assert(os.WriteFile(path, []byte(source), 0o600), qt.IsNil)
	runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", path, "--auto-approve")
	c.Assert(runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", path, "--dry-run"),
		qt.Contains, "Schema is synced")
}
