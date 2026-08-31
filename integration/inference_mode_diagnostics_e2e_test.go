//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the pgx driver for database/sql

	"go.5x5.cz/ptah/internal/dbtarget"
)

// TestInferenceABehindOutboxIsNotAMissingModeE2E is stokaro/ptah#2646.
//
// `BuildCutoverPlan` blanked `Evidence.ConsistencyMode` whenever the guarantee
// was incomplete, so the decision layer refused with "the source is mutable and
// the run declared no consistency mode" — over a specification declaring
// `consistency.mode: outbox`, when the only thing wrong was that one committed
// change had not been caught up. `verify` got the same state right on the same
// run, so the two surfaces disagreed about one fact, and `status --format json`
// put the wrong sentence in `readiness.blockers` for a rollout gate to read.
func TestInferenceABehindOutboxIsNotAMissingModeE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.TimescaleDB)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	adminDB, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()

	name := fmt.Sprintf("ptah_mode_diag_%d", time.Now().UnixNano())
	createE2EDatabase(c, ctx, adminDB, name)
	defer dropE2EDatabase(c, context.Background(), adminDB, name)

	dbName := replaceDatabaseName(c, dbURL, name)
	db, err := sql.Open("pgx", dbName)
	c.Assert(err, qt.IsNil)
	defer db.Close()
	seedCLIArticles(c, ctx, db)

	endpoint := httptest.NewServer(http.HandlerFunc(embeddingsHandler(c)))
	defer endpoint.Close()
	specPath := writeCLISpec(c, endpoint.URL)

	runInference(c, ctx, "prepare", "--spec", specPath, "--db-url", dbName, "--run-id", cliRunID)

	// Before any catch-up: the watermark is legitimately empty, and the reason
	// is that catch-up has not run — not that this mode records no boundary.
	assertAnOutboxWithNoCatchUpYetSaysSo(c, ctx, specPath, dbName)

	runInference(c, ctx, "backfill",
		"--spec", specPath, "--db-url", dbName, "--run-id", cliRunID, "--batch-rows", "10")
	_, err = db.ExecContext(ctx,
		`INSERT INTO articles (id, title, body, updated_at)
		 VALUES (60, 'Unprocessed', 'not caught up yet', '9')`)
	c.Assert(err, qt.IsNil)

	assertTheRefusalNamesTheBarrierNotTheMode(c, ctx, specPath, dbName)
	assertACaughtUpRunIsNotRefusedForConsistency(c, ctx, specPath, dbName)
}

// assertAnOutboxWithNoCatchUpYetSaysSo is finding 2.
func assertAnOutboxWithNoCatchUpYetSaysSo(
	c *qt.C, ctx context.Context, specPath, dbURL string,
) {
	c.Helper()
	output := runInference(c, ctx, "status",
		"--spec", specPath, "--db-url", dbURL, "--run-id", cliRunID)

	c.Assert(output, qt.Contains, "none yet, because catch-up has not run")
	c.Assert(output, qt.Not(qt.Contains), "the selected consistency mode records no boundary")
}

// assertTheRefusalNamesTheBarrierNotTheMode is findings 1, 3 and 4.
//
// The refusal now carries the barrier's OWN reasons rather than a sentence the
// decision layer wrote. That is why they are more specific than `verify`'s:
// the barrier knows how many changes are unprocessed and between which two
// transactions, and a decision layer restating it could only have guessed.
func assertTheRefusalNamesTheBarrierNotTheMode(
	c *qt.C, ctx context.Context, specPath, dbURL string,
) {
	c.Helper()
	output, err := runInferenceExpectingFailure(c, ctx, "cutover",
		"--spec", specPath, "--db-url", dbURL, "--run-id", cliRunID)

	c.Assert(err, qt.IsNotNil)
	c.Assert(output, qt.Contains, "cutover refused")
	c.Assert(output, qt.Contains, "source changes are unprocessed")
	c.Assert(output, qt.Contains, "so changes between them are unprocessed")
	// The sentence that sent an operator to configure a mode they had already
	// configured.
	c.Assert(output, qt.Not(qt.Contains), "declared no consistency mode")
}

// assertACaughtUpRunIsNotRefusedForConsistency is the control.
//
// A change that always emitted the barrier sentence would satisfy the
// assertion above while refusing every cutover forever. Once the catch-up has
// run, no consistency blocker is left.
func assertACaughtUpRunIsNotRefusedForConsistency(
	c *qt.C, ctx context.Context, specPath, dbURL string,
) {
	c.Helper()
	runInference(c, ctx, "catchup",
		"--spec", specPath, "--db-url", dbURL, "--run-id", cliRunID, "--batch-rows", "10")

	output, err := runInferenceExpectingFailure(c, ctx, "cutover",
		"--spec", specPath, "--db-url", dbURL, "--run-id", cliRunID)

	// Still refused, for the approval this policy requires — and that is the
	// point: the consistency reasons are gone.
	c.Assert(err, qt.IsNotNil)
	c.Assert(output, qt.Contains, "requires an approval")
	c.Assert(output, qt.Not(qt.Contains), "source changes are unprocessed")
	c.Assert(output, qt.Not(qt.Contains), "declared no consistency mode")
}
