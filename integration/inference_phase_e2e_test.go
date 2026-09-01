//go:build integration

package integration_test

// The run phase, driven through the verbs that own each step.
//
// Until stokaro/ptah#2441 it never moved: `embedrun.Run.Advance` was the only
// thing that could move one and had no non-test caller, so `status` reported
// whatever `prepare` wrote while the progress counters beside it climbed. Two
// halves of one line, disagreeing.
//
// These read the phase back from the store rather than from what a verb said,
// because the failure this is about is a verb reporting a step it did not
// record.

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the pgx driver for database/sql

	"go.5x5.cz/ptah/internal/dbtarget"
)

// TestInferencePhaseE2E walks the lifecycle and requires each verb to leave the
// phase it owns.
func TestInferencePhaseE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.TimescaleDB)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	dbName, specPath := phaseFixture(c, ctx, dbURL)

	// prepare reaches boundary_captured rather than prepared: it captures the
	// boundary in the same call, and the phase names the step rather than the
	// artifact -- a mode recording no boundary answered the step rather than
	// skipping it.
	runInference(c, ctx, "prepare", "--spec", specPath, "--db-url", dbName, "--run-id", cliRunID)
	c.Assert(phaseOf(c, ctx, specPath, dbName), qt.Equals, "boundary_captured")

	// A completed backfill leaves `backfilled`, not `backfilling`. The phase
	// used to be set to `backfilling` AFTER the walk finished, which made it
	// the phase of a backfill that had ended and left verification with no
	// fact to read (stokaro/ptah#2649).
	runInference(c, ctx, "backfill",
		"--spec", specPath, "--db-url", dbName, "--run-id", cliRunID, "--batch-rows", "10")
	c.Assert(phaseOf(c, ctx, specPath, dbName), qt.Equals, "backfilled")

	runInference(c, ctx, "catchup",
		"--spec", specPath, "--db-url", dbName, "--run-id", cliRunID, "--batch-rows", "10")
	c.Assert(phaseOf(c, ctx, specPath, dbName), qt.Equals, "caught_up")

	runInference(c, ctx, "index", "--spec", specPath, "--db-url", dbName, "--run-id", cliRunID)
	c.Assert(phaseOf(c, ctx, specPath, dbName), qt.Equals, "indexed")

	runInference(c, ctx, "verify", "--spec", specPath, "--db-url", dbName, "--run-id", cliRunID)
	c.Assert(phaseOf(c, ctx, specPath, dbName), qt.Equals, "verified")

	assertCatchingUpAgainDoesNotMoveThePhaseBack(c, ctx, specPath, dbName)
	assertCutoverReachesItsPhase(c, ctx, specPath, dbName)
}

// assertCatchingUpAgainDoesNotMoveThePhaseBack is what the high-water rule is
// for.
//
// A catch-up after a verification is ordinary -- the source keeps moving -- and
// it asks to reach a phase the run went past. The verb must succeed and the
// phase must stay where it was: an implementation that reported an error would
// break re-running, and one that set the phase would lose what the run reached.
func assertCatchingUpAgainDoesNotMoveThePhaseBack(
	c *qt.C, ctx context.Context, specPath, dbURL string,
) {
	c.Helper()
	output := runInference(c, ctx, "catchup",
		"--spec", specPath, "--db-url", dbURL, "--run-id", cliRunID, "--batch-rows", "10")

	c.Assert(output, qt.Contains, "caught up to transaction")
	c.Assert(phaseOf(c, ctx, specPath, dbURL), qt.Equals, "verified")
}

// assertCutoverReachesItsPhase closes the walk.
func assertCutoverReachesItsPhase(c *qt.C, ctx context.Context, specPath, dbURL string) {
	c.Helper()
	digest := planDigestOf(c, ctx, specPath, dbURL)
	runInference(c, ctx, "cutover",
		"--spec", specPath, "--db-url", dbURL, "--run-id", cliRunID,
		"--approve", digest, "--approver", "an operator")

	c.Assert(phaseOf(c, ctx, specPath, dbURL), qt.Equals, "cut_over")
}

// phaseOf reads the phase the store holds, through the verb that reports it.
func phaseOf(c *qt.C, ctx context.Context, specPath, dbURL string) string {
	c.Helper()
	status := runInference(c, ctx, "status",
		"--spec", specPath, "--db-url", dbURL, "--run-id", cliRunID)
	first, _, _ := strings.Cut(status, "\n")
	_, after, found := strings.Cut(first, ": ")
	c.Assert(found, qt.IsTrue, qt.Commentf("no phase in %q", first))
	phase, _, _ := strings.Cut(after, ",")
	return phase
}

// phaseFixture builds a database of its own with an index-declaring
// specification, so every phase of the walk has a verb that reaches it.
func phaseFixture(c *qt.C, ctx context.Context, dbURL string) (string, string) {
	c.Helper()
	admin, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = admin.Close() })

	name := fmt.Sprintf("ptah_phase_%d", time.Now().UnixNano())
	createE2EDatabase(c, ctx, admin, name)
	c.Cleanup(func() { dropE2EDatabase(c, context.Background(), admin, name) })

	dbName := replaceDatabaseName(c, dbURL, name)
	db, err := sql.Open("pgx", dbName)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = db.Close() })
	seedCLIArticles(c, ctx, db)

	endpoint := httptest.NewServer(http.HandlerFunc(embeddingsHandler(c)))
	c.Cleanup(endpoint.Close)
	return dbName, writeIndexedCLISpec(c, endpoint.URL)
}

// TestInferenceSnapshotCompletionIsMeasuredE2E is stokaro/ptah#2649 finding 3,
// and it asserts the two directions the old expression got wrong.
//
// `SnapshotComplete` read `run.Phase != PhaseBackfilling`, which is true for
// every phase BEFORE the backfill as well as after it. So a run that had never
// backfilled was told its snapshot was complete, and a run whose backfill had
// embedded every row was told it was not -- the phase being set to
// `backfilling` only once the walk had finished.
//
// Both halves are asserted against ONE run, in sequence, because either alone
// passes under an expression that is simply inverted.
func TestInferenceSnapshotCompletionIsMeasuredE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.TimescaleDB)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	dbName, specPath := phaseFixture(c, ctx, dbURL)
	const snapshot = "the backfill has not reached the end of its snapshot"

	runInference(c, ctx, "prepare", "--spec", specPath, "--db-url", dbName, "--run-id", cliRunID)

	// Nothing has walked the snapshot, so verification must say so. This is the
	// direction the old expression got backwards and no test covered.
	before, beforeErr := runInferenceExpectingFailure(c, ctx, "verify",
		"--spec", specPath, "--db-url", dbName, "--run-id", cliRunID)
	c.Assert(beforeErr, qt.IsNotNil)
	c.Assert(before, qt.Contains, snapshot)

	runInference(c, ctx, "backfill",
		"--spec", specPath, "--db-url", dbName, "--run-id", cliRunID, "--batch-rows", "10")

	// And now it must not, about the same run, on the same corpus.
	after, afterErr := runInferenceExpectingFailure(c, ctx, "verify",
		"--spec", specPath, "--db-url", dbName, "--run-id", cliRunID)
	c.Assert(afterErr, qt.IsNotNil)
	c.Assert(after, qt.Not(qt.Contains), snapshot)
	// The control: verification is still reporting, so the absence above is an
	// answer rather than a report that stopped being produced.
	c.Assert(after, qt.Contains, "catch-up has not reached the barrier")
}
