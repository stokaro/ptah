//go:build integration

// The two terminal phases have a producer.
//
// stokaro/ptah#2649 finding 6: run-status-and-findings.md states the order ends
// "either rolled_back or retired", the transition table declares
// `cut_over -> {retired, rolled_back}`, and neither verb reached either. A run
// whose generation was rolled off the pointer, or destroyed, kept reporting
// `cut_over` forever -- not a high-water mark that stopped, a state the
// lifecycle could never enter.
//
// It is measured through `status`, which is where an operator reads a phase,
// and against a live database because the phase is a row.

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

	"go.5x5.cz/ptah/internal/dbtarget"
)

// TestInferenceRollbackEndsTheRunE2E runs the lifecycle to a cutover and then
// rolls the pointer off the generation it built.
func TestInferenceRollbackEndsTheRunE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.TimescaleDB)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	adminDB, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()

	name := fmt.Sprintf("ptah_terminal_%d", time.Now().UnixNano())
	createE2EDatabase(c, ctx, adminDB, name)
	defer dropE2EDatabase(c, context.Background(), adminDB, name)

	dbName := replaceDatabaseName(c, dbURL, name)
	db, err := sql.Open("pgx", dbName)
	c.Assert(err, qt.IsNil)
	defer db.Close()
	seedCLIArticles(c, ctx, db)

	endpoint := httptest.NewServer(http.HandlerFunc(embeddingsHandler(c)))
	defer endpoint.Close()

	// Two real generations, because every fabricated stand-in for the first one
	// is refused by a policy the rollback is entitled to apply: a bare
	// registration has never been verified, is not maintained, and holds no
	// vectors. Building both through the verbs is the state a second migration
	// actually leaves.
	firstSpec := writeCLISpec(c, endpoint.URL)
	secondSpec := writeCLISpecWithMetric(c, endpoint.URL, "cosine", "embedding_v2")

	first := completeCutOverRun(c, ctx, firstSpec, dbName, "terminal-first")
	second := completeCutOverRun(c, ctx, secondSpec, dbName, "terminal-second")

	c.Assert(activeGenerationOfRun(c, ctx, secondSpec, dbName, "terminal-second"), qt.Equals, second)
	c.Assert(runPhase(c, ctx, db, "terminal-second"), qt.Equals, "cut_over")

	runInference(c, ctx, "rollback",
		"--spec", secondSpec, "--db-url", dbName, "--to", first, "--window", "1h")

	c.Assert(activeGenerationOfRun(c, ctx, firstSpec, dbName, "terminal-first"), qt.Equals, first)
	c.Assert(runPhase(c, ctx, db, "terminal-second"), qt.Equals, "rolled_back",
		qt.Commentf("the run that built the generation rolled off the pointer kept its phase"))
	// The run that BUILT the generation returned to is not ended by being
	// returned to. Rolling back resumes it; it did not finish.
	c.Assert(runPhase(c, ctx, db, "terminal-first"), qt.Equals, "cut_over")
}

// TestInferenceRollbackLeavesAnUnrelatedRunAloneE2E is the control.
//
// Ending every run, or every run the lookup returns, would satisfy the test
// above. This builds a third generation under its own run, never cuts over to
// it, and requires the rollback to leave it exactly where it was.
func TestInferenceRollbackLeavesAnUnrelatedRunAloneE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.TimescaleDB)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	adminDB, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()

	name := fmt.Sprintf("ptah_terminal_control_%d", time.Now().UnixNano())
	createE2EDatabase(c, ctx, adminDB, name)
	defer dropE2EDatabase(c, context.Background(), adminDB, name)

	dbName := replaceDatabaseName(c, dbURL, name)
	db, err := sql.Open("pgx", dbName)
	c.Assert(err, qt.IsNil)
	defer db.Close()
	seedCLIArticles(c, ctx, db)

	endpoint := httptest.NewServer(http.HandlerFunc(embeddingsHandler(c)))
	defer endpoint.Close()

	firstSpec := writeCLISpec(c, endpoint.URL)
	secondSpec := writeCLISpecWithMetric(c, endpoint.URL, "cosine", "embedding_v2")
	bystanderSpec := writeCLISpecWithMetric(c, endpoint.URL, "cosine", "embedding_v3")

	first := completeCutOverRun(c, ctx, firstSpec, dbName, "control-first")
	completeCutOverRun(c, ctx, secondSpec, dbName, "control-second")
	runInference(c, ctx, "prepare",
		"--spec", bystanderSpec, "--db-url", dbName, "--run-id", "control-bystander")

	// And a SECOND run of the generation being rolled off, left where prepare
	// puts it. This one IS returned by the lookup, so it is what decides
	// whether only the cut-over run is advanced: asking a run at
	// boundary_captured to reach rolled_back is a transition the lifecycle
	// refuses, and the refusal would fail the rollback itself.
	runInference(c, ctx, "prepare",
		"--spec", secondSpec, "--db-url", dbName, "--run-id", "control-same-generation")

	before := runPhase(c, ctx, db, "control-bystander")
	beforeSame := runPhase(c, ctx, db, "control-same-generation")

	runInference(c, ctx, "rollback",
		"--spec", secondSpec, "--db-url", dbName, "--to", first, "--window", "1h")

	c.Assert(runPhase(c, ctx, db, "control-second"), qt.Equals, "rolled_back")
	c.Assert(runPhase(c, ctx, db, "control-bystander"), qt.Equals, before,
		qt.Commentf("a rollback ended a run that built a different generation"))
	c.Assert(runPhase(c, ctx, db, "control-same-generation"), qt.Equals, beforeSame,
		qt.Commentf("a rollback moved a run that never cut over"))
}

// completeCutOverRun drives one generation from prepare to cutover and returns
// its identity.
//
// The window is asked for on every cutover so the generation this one replaces
// stays a way back, which is what makes the rollback in these tests legal.
func completeCutOverRun(
	c *qt.C, ctx context.Context, specPath, dbURL, runID string,
) string {
	c.Helper()
	runInference(c, ctx, "prepare", "--spec", specPath, "--db-url", dbURL, "--run-id", runID)
	runInference(c, ctx, "backfill",
		"--spec", specPath, "--db-url", dbURL, "--run-id", runID, "--batch-rows", "10")
	runInference(c, ctx, "catchup",
		"--spec", specPath, "--db-url", dbURL, "--run-id", runID, "--batch-rows", "10")
	runInference(c, ctx, "verify", "--spec", specPath, "--db-url", dbURL, "--run-id", runID)
	runInference(c, ctx, "cutover",
		"--spec", specPath, "--db-url", dbURL, "--run-id", runID,
		"--approve", planDigestOfRun(c, ctx, specPath, dbURL, runID),
		"--approver", "an operator", "--stabilize-for", "1h")
	return activeGenerationOfRun(c, ctx, specPath, dbURL, runID)
}

// runPhase reads a run's phase straight from the table.
//
// Not through `status`, deliberately: `status` renders a report assembled from
// the same row, so a phase it printed correctly from a row that was never
// written would still pass. The row is the thing this is about.
func runPhase(c *qt.C, ctx context.Context, db *sql.DB, runID string) string {
	c.Helper()
	var phase string
	err := db.QueryRowContext(ctx,
		`SELECT phase FROM ptah_embedding_run WHERE id = $1`, runID).Scan(&phase)
	c.Assert(err, qt.IsNil)
	return phase
}
