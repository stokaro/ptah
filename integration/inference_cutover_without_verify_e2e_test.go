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
	_ "github.com/jackc/pgx/v5/stdlib"

	"go.5x5.cz/ptah/internal/dbtarget"
)

// TestInferenceCutoverWithoutVerifyE2E is stokaro/ptah#2631.
//
// `cutover` verifies internally and refuses on a failing report, so nothing in
// the product asked an operator to run the `verify` VERB first. But `cut_over`
// is reachable only from `verified`, and only that verb reached it -- and the
// phase was recorded AFTER the pointer had moved and the window had opened. So
// a lifecycle of prepare, backfill, catchup, cutover:
//
//   - moved the pointer and printed "queries now read generation ...";
//   - then exited 2 with "caught_up cannot move to cut_over";
//   - published no evidence at all, for a cutover that had happened;
//   - left the run at `caught_up` forever.
//
// `status --require-ready` reported the state as ready beforehand, so a
// Kubernetes rollout gate opened onto a cutover job that reported failure.
//
// The assertions are on the CATALOG rather than on the exit code, because the
// state this defect produced is one where the command's report and the database
// disagree -- and an assertion on either alone accepts the disagreement.
func TestInferenceCutoverWithoutVerifyE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.TimescaleDB)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	adminDB, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()

	name := fmt.Sprintf("ptah_cutover_direct_%d", time.Now().UnixNano())
	createE2EDatabase(c, ctx, adminDB, name)
	defer dropE2EDatabase(c, context.Background(), adminDB, name)

	dbName := replaceDatabaseName(c, dbURL, name)
	db, err := sql.Open("pgx", dbName)
	c.Assert(err, qt.IsNil)
	defer db.Close()
	seedCLIArticles(c, ctx, db)

	endpoint := httptest.NewServer(http.HandlerFunc(embeddingsHandler(c)))
	defer endpoint.Close()
	specPath := writeDirectCutoverSpec(c, endpoint.URL)
	const runID = "direct-1"
	evidencePath := filepath.Join(c.TempDir(), "cutover.json")

	// No `verify` verb anywhere in this sequence. That is the point.
	runInference(c, ctx, "prepare", "--spec", specPath, "--db-url", dbName, "--run-id", runID)
	runInference(c, ctx, "backfill",
		"--spec", specPath, "--db-url", dbName, "--run-id", runID, "--batch-rows", "10")
	runInference(c, ctx, "catchup",
		"--spec", specPath, "--db-url", dbName, "--run-id", runID, "--batch-rows", "10")

	refused, err := runInferenceExpectingFailure(c, ctx, "cutover",
		"--spec", specPath, "--db-url", dbName, "--run-id", runID)
	c.Assert(err, qt.IsNotNil)
	runInference(c, ctx, "cutover",
		"--spec", specPath, "--db-url", dbName, "--run-id", runID,
		"--approve", planDigestFrom(c, refused), "--approver", "an operator",
		"--evidence-file", evidencePath)

	// The pointer moved AND the run says so. The defect produced the first
	// without the second, so asserting only the pointer would pass against it.
	var active, phase string
	c.Assert(db.QueryRowContext(ctx,
		`SELECT active_generation FROM ptah_embedding_pointer`).Scan(&active), qt.IsNil)
	c.Assert(db.QueryRowContext(ctx,
		`SELECT phase FROM ptah_embedding_run WHERE id = $1`, runID).Scan(&phase), qt.IsNil)
	c.Assert(phase, qt.Equals, "cut_over")
	c.Assert(active, qt.Not(qt.Equals), "")

	// And the evidence exists. It was written after the phase, so the failure
	// took it with it -- for a cutover an operator can see in their database.
	body, err := os.ReadFile(evidencePath)
	c.Assert(err, qt.IsNil)
	c.Assert(string(body), qt.Contains, active)
}

// writeDirectCutoverSpec writes the specification this lifecycle uses.
func writeDirectCutoverSpec(c *qt.C, endpoint string) string {
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
  column: embedding_direct
  representation: vector
  metric: cosine
consistency:
  mode: outbox
policy:
  require_exact_approval: true
  require_consistency_mode: true
`, endpoint)
	path := filepath.Join(c.TempDir(), "spec-direct.yaml")
	c.Assert(os.WriteFile(path, []byte(document), 0o600), qt.IsNil)
	return path
}
