//go:build integration

package integration_test

// The index verb driven the way an operator runs it, and the control that says
// what building the index is FOR.
//
// The package-level tests beside this one assert that an index is created and
// valid. That is not the same as it mattering: a verb could build a perfectly
// good index that nothing downstream consults. This runs the lifecycle with an
// index-declaring specification and requires verification to refuse before the
// build and pass after it (stokaro/ptah#2415).

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the pgx driver for database/sql

	"go.5x5.cz/ptah/internal/dbtarget"
)

// TestInferenceIndexE2E is prepare, backfill, index — and verify on both sides
// of the build.
func TestInferenceIndexE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.TimescaleDB)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	adminDB, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()

	name := fmt.Sprintf("ptah_index_%d", time.Now().UnixNano())
	createE2EDatabase(c, ctx, adminDB, name)
	defer dropE2EDatabase(c, context.Background(), adminDB, name)

	dbName := replaceDatabaseName(c, dbURL, name)
	db, err := sql.Open("pgx", dbName)
	c.Assert(err, qt.IsNil)
	defer db.Close()
	seedCLIArticles(c, ctx, db)

	endpoint := httptest.NewServer(http.HandlerFunc(embeddingsHandler(c)))
	defer endpoint.Close()
	specPath := writeIndexedCLISpec(c, endpoint.URL)

	runInference(c, ctx, "prepare", "--spec", specPath, "--db-url", dbName, "--run-id", cliRunID)
	runInference(c, ctx, "backfill",
		"--spec", specPath, "--db-url", dbName, "--run-id", cliRunID, "--batch-rows", "10")
	runInference(c, ctx, "catchup",
		"--spec", specPath, "--db-url", dbName, "--run-id", cliRunID, "--batch-rows", "10")

	// Before the build. A specification that declares an index and has none is
	// a generation whose every query is a sequential scan, and verification is
	// where that is refused.
	before, err := runInferenceExpectingFailure(c, ctx, "verify",
		"--spec", specPath, "--db-url", dbName, "--run-id", cliRunID)
	c.Assert(err, qt.IsNotNil)
	c.Assert(before, qt.Contains, "index")

	built := runInference(c, ctx, "index",
		"--spec", specPath, "--db-url", dbName, "--run-id", cliRunID)
	c.Assert(built, qt.Contains, "has a valid index")

	// After. The same command, the same run, the same data -- the index is the
	// only thing that changed.
	after := runInference(c, ctx, "verify",
		"--spec", specPath, "--db-url", dbName, "--run-id", cliRunID)
	c.Assert(after, qt.Contains, "every deterministic layer passed")

	// And running it again is the finished state rather than an error.
	again := runInference(c, ctx, "index",
		"--spec", specPath, "--db-url", dbName, "--run-id", cliRunID)
	c.Assert(again, qt.Contains, "already has a valid index")
}

// writeIndexedCLISpec writes the CLI fixture's specification with an index
// method, which the shared one deliberately does not declare.
func writeIndexedCLISpec(c *qt.C, endpoint string) string {
	c.Helper()
	document := fmt.Sprintf(`
version: 1
name: cli articles
source:
  schema: public
  table: articles
  key_fields: [id]
  input_fields: [title, body]
  version_strategy: updated_at
  version_field: updated_at
  mutable: true
preprocessing:
  separator: "\n"
  null_policy: empty
  empty_policy: skip
  unicode_normalization: none
  truncate: refuse
model:
  provider: openai-compatible
  endpoint_class: local
  endpoint: %s/v1
  identifier: test-embed
  revision: "1"
  reported_dimension: 4
  normalization: none
target:
  schema: public
  table: articles
  column: embedding
  representation: vector
  metric: cosine
  index_method: hnsw
  index_options:
    m: "16"
    ef_construction: "64"
consistency:
  mode: outbox
policy:
  require_exact_approval: true
  require_consistency_mode: true
`, endpoint)
	path := filepath.Join(c.TempDir(), "spec.yaml")
	c.Assert(os.WriteFile(path, []byte(document), 0o600), qt.IsNil)
	return path
}
