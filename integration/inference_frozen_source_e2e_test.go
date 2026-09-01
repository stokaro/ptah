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

// TestInferenceFrozenSourceE2E is stokaro/ptah#2632.
//
// `backfill` recorded `backfilling` and `catchup` was the only verb that
// reached `caught_up`. Catch-up is refused by design for a mode that records no
// changes, and `nextPhases` admits no other edge out of `backfilling` -- so a
// run over a frozen source could never be indexed, verified or cut over. It sat
// at `backfilling` with every row embedded, and verification reported "the
// backfill has not reached the end of its snapshot" about a backfill that had
// finished.
//
// The sequence below is the one `migrate-a-paused-source.md` publishes, and the
// page says in as many words that `catchup` is not in it.
//
// The control is the last assertion: the same lifecycle under `outbox` still
// requires the catch-up. Without it, a fix that reached `caught_up` from every
// backfill would look identical here while quietly retiring the barrier that
// makes a live-source cutover mean anything.
func TestInferenceFrozenSourceE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.TimescaleDB)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	adminDB, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()

	name := fmt.Sprintf("ptah_frozen_%d", time.Now().UnixNano())
	createE2EDatabase(c, ctx, adminDB, name)
	defer dropE2EDatabase(c, context.Background(), adminDB, name)

	dbName := replaceDatabaseName(c, dbURL, name)
	db, err := sql.Open("pgx", dbName)
	c.Assert(err, qt.IsNil)
	defer db.Close()
	seedCLIArticles(c, ctx, db)

	endpoint := httptest.NewServer(http.HandlerFunc(embeddingsHandler(c)))
	defer endpoint.Close()

	assertAFrozenSourceReachesCutover(c, ctx, db, endpoint.URL, dbName)
	assertALiveSourceStillNeedsItsCatchUp(c, ctx, endpoint.URL, dbName)
}

// assertAFrozenSourceReachesCutover walks the published sequence.
//
// The phase is read from the run record rather than from what `status` prints,
// because the defect was precisely a phase that did not move while every verb
// above it reported success.
func assertAFrozenSourceReachesCutover(
	c *qt.C, ctx context.Context, db *sql.DB, endpointURL, dbURL string,
) {
	c.Helper()
	specPath := writeFrozenSpec(c, endpointURL, "embedding_frozen")
	const runID = "frozen-1"

	runInference(c, ctx, "prepare", "--spec", specPath, "--db-url", dbURL, "--run-id", runID)
	runInference(c, ctx, "backfill",
		"--spec", specPath, "--db-url", dbURL, "--run-id", runID, "--batch-rows", "10")

	// The backfill is what reaches the barrier here: there is nothing for a
	// catch-up to process, so the completed snapshot IS the run being current.
	c.Assert(recordedPhaseOf(c, ctx, db, runID), qt.Equals, "caught_up")

	runInference(c, ctx, "index", "--spec", specPath, "--db-url", dbURL, "--run-id", runID)
	c.Assert(recordedPhaseOf(c, ctx, db, runID), qt.Equals, "indexed")

	runInference(c, ctx, "verify", "--spec", specPath, "--db-url", dbURL, "--run-id", runID)
	c.Assert(recordedPhaseOf(c, ctx, db, runID), qt.Equals, "verified")

	refused, err := runInferenceExpectingFailure(c, ctx, "cutover",
		"--spec", specPath, "--db-url", dbURL, "--run-id", runID)
	c.Assert(err, qt.IsNotNil)
	runInference(c, ctx, "cutover",
		"--spec", specPath, "--db-url", dbURL, "--run-id", runID,
		"--approve", planDigestFrom(c, refused), "--approver", "an operator")
	c.Assert(recordedPhaseOf(c, ctx, db, runID), qt.Equals, "cut_over")
}

// assertALiveSourceStillNeedsItsCatchUp is the control.
//
// A mode that DOES record changes must still be held to processing them: the
// backfill covers the source as of the boundary and says nothing about the
// writes since. A run reaching `caught_up` off its backfill alone would be a
// cutover resting on a barrier nobody crossed.
func assertALiveSourceStillNeedsItsCatchUp(
	c *qt.C, ctx context.Context, endpointURL, dbURL string,
) {
	c.Helper()
	specPath := writeCLISpecWithMetric(c, endpointURL, "cosine", "embedding_live")
	const runID = "frozen-control"

	runInference(c, ctx, "prepare", "--spec", specPath, "--db-url", dbURL, "--run-id", runID)
	runInference(c, ctx, "backfill",
		"--spec", specPath, "--db-url", dbURL, "--run-id", runID, "--batch-rows", "10")

	output, err := runInferenceExpectingFailure(c, ctx, "index",
		"--spec", specPath, "--db-url", dbURL, "--run-id", runID)

	c.Assert(err, qt.IsNotNil)
	c.Assert(output+err.Error(), qt.Contains, "backfilling cannot move to indexed")
}

// recordedPhaseOf reads the phase off the run record.
//
// The row rather than `status`, and for an arbitrary run id: the defect was a
// phase that did not move while every verb above it reported success, so the
// assertion has to reach past the verbs that report it.
func recordedPhaseOf(c *qt.C, ctx context.Context, db *sql.DB, runID string) string {
	c.Helper()
	var phase string
	c.Assert(db.QueryRowContext(ctx,
		`SELECT phase FROM ptah_embedding_run WHERE id = $1`, runID).Scan(&phase), qt.IsNil)
	return phase
}

// writeFrozenSpec writes the specification the paused-source guide publishes.
func writeFrozenSpec(c *qt.C, endpoint, column string) string {
	c.Helper()
	document := fmt.Sprintf(`
version: 1
name: frozen articles
source:
  schema: public
  table: articles
  key_fields: [id]
  input_fields: [title, body]
  version_strategy: updated_at
  version_field: updated_at
  mutable: false
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
  column: %s
  representation: vector
  metric: cosine
  index_method: hnsw
consistency:
  mode: immutable
policy:
  require_exact_approval: true
`, endpoint, column)
	path := filepath.Join(c.TempDir(), "spec-frozen.yaml")
	c.Assert(os.WriteFile(path, []byte(document), 0o600), qt.IsNil)
	return path
}
