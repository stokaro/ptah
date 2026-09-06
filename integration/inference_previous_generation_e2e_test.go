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
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the pgx driver for database/sql

	"ptah.run/internal/dbtarget"
)

// TestInferencePreviousGenerationE2E is stokaro/ptah#2630.
//
// A rollback asks one question about ONE generation: is the way back still
// fresh enough to take. Freshness is each row's stored input hash against a
// hash recomputed from the source, and that recomputation needs the retired
// generation's own source fields, preprocessing and identity.
//
// Both verbs took it from whichever `--spec` the operator passed. The
// documented commands pass the CURRENT specification, and a second generation
// exists in order to change something -- the headline case changes the model --
// so the expected hashes were computed under an identity belonging to no
// generation at all. Every row mismatched, and the two verbs failed in opposite
// directions from the same wrong measurement:
//
//   - `rollback` was refused, always, with "N rows are stale" -- while `verify`
//     on that same generation at that same instant passed every layer;
//   - `retire` was ALLOWED, because "this generation can still be rolled back
//     to" is exactly the refusal that a stale reading deletes. It destroyed the
//     live rollback target of an active generation, inside its stabilization
//     window, and printed no warning.
//
// The two directions are why this test asserts both. A fix measured only
// through `rollback` leaves the destructive half in place, and it is the half
// that cannot be undone.
func TestInferencePreviousGenerationE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.TimescaleDB)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	adminDB, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()

	name := fmt.Sprintf("ptah_prevgen_%d", time.Now().UnixNano())
	createE2EDatabase(c, ctx, adminDB, name)
	defer dropE2EDatabase(c, context.Background(), adminDB, name)

	dbName := replaceDatabaseName(c, dbURL, name)
	db, err := sql.Open("pgx", dbName)
	c.Assert(err, qt.IsNil)
	defer db.Close()
	seedCLIArticles(c, ctx, db)

	endpoint := httptest.NewServer(http.HandlerFunc(embeddingsHandler(c)))
	defer endpoint.Close()

	// Two generations differing the way the migration guide tells an operator
	// to differ: a new model revision into a new column. A column-only
	// difference does NOT reproduce this -- the old code repaired the column
	// from the registry -- so a fixture that changed only the column would pass
	// against the defect and record its own blindness as coverage.
	first := writeRevisionSpec(c, endpoint.URL, "1", "embedding_v1")
	second := writeRevisionSpec(c, endpoint.URL, "2", "embedding_v2")

	older := buildGeneration(c, ctx, first, dbName, "prevgen-1")
	newer := buildGeneration(c, ctx, second, dbName, "prevgen-2")
	c.Assert(older, qt.Not(qt.Equals), newer)

	// The first generation has to be the one queries read before the second
	// takes over, or the pointer records no previous generation and there is no
	// way back for either verb to be wrong about.
	runInference(c, ctx, "cutover",
		"--spec", first, "--db-url", dbName, "--run-id", "prevgen-1",
		"--approve", cutoverDigestFor(c, ctx, first, dbName, "prevgen-1"),
		"--approver", "an operator")
	runInference(c, ctx, "cutover",
		"--spec", second, "--db-url", dbName, "--run-id", "prevgen-2",
		"--approve", cutoverDigestFor(c, ctx, second, dbName, "prevgen-2"),
		"--approver", "an operator", "--stabilize-for", "24h")
	runInference(c, ctx, "catchup",
		"--spec", first, "--db-url", dbName, "--run-id", "prevgen-1",
		"--batch-rows", "10", "--maintain-for", "24h")

	assertRetireRefusesToDestroyTheWayBack(c, ctx, second, dbName, older)
	assertRollbackWorksWithTheCurrentSpec(c, ctx, db, second, dbName, older)
}

// assertRetireRefusesToDestroyTheWayBack is the half that cannot be undone.
//
// The command is the one `rollback-and-retire.md` publishes: the current
// specification, the previous generation named by `--generation`. The refusal
// has to fire, and it has to fire for the right reason -- so the message is
// asserted rather than the exit code alone, which a refusal for any other cause
// would also produce.
func assertRetireRefusesToDestroyTheWayBack(
	c *qt.C, ctx context.Context, specPath, dbURL, older string,
) {
	c.Helper()
	// Approved, because the policy's approval check fires first and would
	// refuse this run for a reason that has nothing to do with the rollback
	// target -- an assertion on the exit code alone would have been satisfied
	// by that, and satisfied identically against the defect.
	digest := retirementDigestOf(c, ctx, specPath, dbURL, older)
	output, err := runInferenceExpectingFailure(c, ctx, "retire",
		"--spec", specPath, "--db-url", dbURL, "--generation", older,
		"--approve", digest, "--approver", "an operator", "--drop-column=false")

	c.Assert(err, qt.IsNotNil)
	c.Assert(output, qt.Contains, "can still be rolled back to")
	c.Assert(output, qt.Not(qt.Contains), "is gone")
}

// assertRollbackWorksWithTheCurrentSpec is the other direction of the same
// measurement.
//
// The pointer is read back from the catalog rather than from the command's own
// output: a rollback that printed success and moved nothing would satisfy an
// assertion on stdout, and that is the failure this whole area keeps producing.
func assertRollbackWorksWithTheCurrentSpec(
	c *qt.C, ctx context.Context, db *sql.DB, specPath, dbURL, older string,
) {
	c.Helper()
	output := runInference(c, ctx, "rollback",
		"--spec", specPath, "--db-url", dbURL, "--to", older, "--window", "24h")
	c.Assert(output, qt.Contains, older)

	var active string
	c.Assert(db.QueryRowContext(ctx,
		`SELECT active_generation FROM ptah_embedding_pointer`).Scan(&active), qt.IsNil)
	c.Assert(active, qt.Equals, older)
}

// buildGeneration walks one specification through to a verified generation.
func buildGeneration(c *qt.C, ctx context.Context, specPath, dbURL, runID string) string {
	c.Helper()
	runInference(c, ctx, "prepare", "--spec", specPath, "--db-url", dbURL, "--run-id", runID)
	runInference(c, ctx, "backfill",
		"--spec", specPath, "--db-url", dbURL, "--run-id", runID, "--batch-rows", "10")
	runInference(c, ctx, "catchup",
		"--spec", specPath, "--db-url", dbURL, "--run-id", runID, "--batch-rows", "10")
	runInference(c, ctx, "verify", "--spec", specPath, "--db-url", dbURL, "--run-id", runID)

	return activeGenerationFromRun(c, ctx, specPath, dbURL, runID)
}

// activeGenerationFromRun reads the generation identity a run is building.
func activeGenerationFromRun(c *qt.C, ctx context.Context, specPath, dbURL, runID string) string {
	c.Helper()
	status := runInference(c, ctx, "status", "--spec", specPath, "--db-url", dbURL, "--run-id", runID)
	for line := range strings.SplitSeq(status, "\n") {
		if after, found := strings.CutPrefix(strings.TrimSpace(line), "- generation: "); found {
			return strings.TrimSpace(after)
		}
	}
	c.Fatalf("no generation in:\n%s", status)
	return ""
}

// cutoverDigestFor is the plan digest a refused cutover publishes.
//
// The digest an approval binds to is only ever printed by a cutover that was
// refused for want of one, which is what makes this two invocations rather than
// a value the test could compute.
func cutoverDigestFor(c *qt.C, ctx context.Context, specPath, dbURL, runID string) string {
	c.Helper()
	refused, err := runInferenceExpectingFailure(c, ctx, "cutover",
		"--spec", specPath, "--db-url", dbURL, "--run-id", runID)
	c.Assert(err, qt.IsNotNil)
	return planDigestFrom(c, refused)
}

// writeRevisionSpec writes a specification at one model revision and column.
func writeRevisionSpec(c *qt.C, endpoint, revision, column string) string {
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
  revision: %q
  reported_dimension: 4
  normalization: none
target:
  schema: public
  table: articles
  column: %s
  representation: vector
  metric: cosine
consistency:
  mode: outbox
policy:
  require_exact_approval: true
  require_consistency_mode: true
`, endpoint, revision, column)
	path := filepath.Join(c.TempDir(), "spec-"+revision+".yaml")
	c.Assert(os.WriteFile(path, []byte(document), 0o600), qt.IsNil)
	return path
}
