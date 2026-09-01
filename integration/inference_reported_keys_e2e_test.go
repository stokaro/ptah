//go:build integration

package integration_test

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

// TestInferenceAReportedCompositeKeyIsReadableE2E is stokaro/ptah#2649 finding
// 2, on the line an operator acts from.
//
// A key's components are joined with U+001F so that no column value can forge a
// boundary. Printed as stored, a terminal swallows it: `(acme, 2)` and
// `(globex, 1)` came out as `acme2` and `globex1` -- not copy-pasteable into a
// predicate, and ambiguous, since tenant `a` with id `11` and tenant `a1` with
// id `1` both render as `a11`. That line is the only thing telling an operator
// which rows to remove.
//
// It runs through the CLI rather than against the renderer, because the
// renderer having the right answer is not the property that was missing: what
// was missing was the printing path calling it, and a unit test on the function
// stays green while `joinKeys` prints the raw string.
func TestInferenceAReportedCompositeKeyIsReadableE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.TimescaleDB)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	db, dbName := freshReportedKeysDatabase(c, ctx, dbURL, "ptah_reported_keys")
	endpoint := httptest.NewServer(http.HandlerFunc(embeddingsHandler(c)))
	defer endpoint.Close()

	for _, statement := range []string{
		`CREATE EXTENSION IF NOT EXISTS vector`,
		`CREATE TABLE documents (
			tenant TEXT NOT NULL, id BIGINT NOT NULL, body TEXT NOT NULL,
			published BOOLEAN NOT NULL DEFAULT true, updated_at TEXT NOT NULL,
			PRIMARY KEY (tenant, id))`,
		`INSERT INTO documents (tenant, id, body, published, updated_at) VALUES
			('acme',   1, 'about pricing', true,  '7'),
			('acme',   2, 'about support', true,  '7'),
			('globex', 1, 'about billing', true,  '7')`,
	} {
		_, err := db.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}

	specPath := writeCompositeKeyCLISpec(c, endpoint.URL)
	runInference(c, ctx, "prepare", "--spec", specPath, "--db-url", dbName, "--run-id", cliRunID)
	runInference(c, ctx, "backfill",
		"--spec", specPath, "--db-url", dbName, "--run-id", cliRunID, "--batch-rows", "10")
	runInference(c, ctx, "catchup",
		"--spec", specPath, "--db-url", dbName, "--run-id", cliRunID, "--batch-rows", "10")

	// Two rows leave scope without the outbox seeing it, so they keep this
	// generation's vectors and become rows verification has to name.
	_, err := db.ExecContext(ctx,
		`SET session_replication_role = replica;
		 UPDATE documents SET published = false WHERE (tenant, id) IN (('acme', 2), ('globex', 1));
		 SET session_replication_role = origin`)
	c.Assert(err, qt.IsNil)

	output, err := runInferenceExpectingFailure(c, ctx, "verify",
		"--spec", specPath, "--db-url", dbName, "--run-id", cliRunID)

	c.Assert(err, qt.IsNotNil, qt.Commentf("%s", output))
	c.Assert(output, qt.Contains, "are outside the generation's source scope")
	c.Assert(output, qt.Contains, "(acme, 2)")
	c.Assert(output, qt.Contains, "(globex, 1)")
	// And the shape the terminal swallowed is gone.
	c.Assert(output, qt.Not(qt.Contains), "acme\x1f2")
	c.Assert(output, qt.Not(qt.Contains), "acme2")
}

// writeCompositeKeyCLISpec writes a specification whose key has two components.
//
// The shared template's key is a single column, and a one-component key renders
// as its own value however it is joined -- so the fixture that would show this
// defect could not be built from it.
func writeCompositeKeyCLISpec(c *qt.C, endpoint string) string {
	c.Helper()
	document := fmt.Sprintf(`
version: 1
name: tenant documents
source:
  schema: public
  table: documents
  key_fields: [tenant, id]
  input_fields: [body]
  filter: "published = true"
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
  table: documents
  column: embedding
  representation: vector
  metric: cosine
consistency:
  mode: outbox
policy:
  require_exact_approval: true
  require_consistency_mode: true
`, endpoint)
	path := filepath.Join(c.TempDir(), "composite-spec.yaml")
	c.Assert(os.WriteFile(path, []byte(document), 0o600), qt.IsNil)
	return path
}

// freshReportedKeysDatabase makes a database of its own and hands back a
// connection and the URL that reaches it.
func freshReportedKeysDatabase(
	c *qt.C, ctx context.Context, dbURL, prefix string,
) (*sql.DB, string) {
	c.Helper()
	adminDB, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = adminDB.Close() })

	name := fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
	createE2EDatabase(c, ctx, adminDB, name)
	c.Cleanup(func() { dropE2EDatabase(c, context.Background(), adminDB, name) })

	dbName := replaceDatabaseName(c, dbURL, name)
	db, err := sql.Open("pgx", dbName)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = db.Close() })
	return db, dbName
}
