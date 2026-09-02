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

// TestInferenceARollbackIsReversibleE2E is the regression the first fix for
// these phases introduced.
//
// The guide calls a rollback reversible, and cutting the generation over again
// is how it is reversed. `rolled_back` was recorded as a phase nothing follows,
// so the reversal left the run saying it had been rolled back while the pointer
// named its generation as the one queries read -- two rows about one fact,
// disagreeing, with no way back into agreement (stokaro/ptah#2649 finding 6).
func TestInferenceARollbackIsReversibleE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.TimescaleDB)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	db, dbName := freshTerminalPhaseDatabase(c, ctx, dbURL, "ptah_phase_reverse")
	seedCLIArticles(c, ctx, db)
	endpoint := httptest.NewServer(http.HandlerFunc(embeddingsHandler(c)))
	defer endpoint.Close()

	firstSpec := writeCLISpec(c, endpoint.URL)
	secondSpec := writeCLISpecWithMetric(c, endpoint.URL, "cosine", "embedding_v2")
	first := completeCutOverRun(c, ctx, firstSpec, dbName, "reverse-first")
	second := completeCutOverRun(c, ctx, secondSpec, dbName, "reverse-second")

	runInference(c, ctx, "rollback",
		"--spec", secondSpec, "--db-url", dbName, "--to", first, "--window", "1h")
	c.Assert(runPhase(c, ctx, db, "reverse-second"), qt.Equals, "rolled_back")

	// Reverse it. The generation is caught up and verified again first, which
	// is what an operator does before cutting over to anything.
	runInference(c, ctx, "catchup",
		"--spec", secondSpec, "--db-url", dbName, "--run-id", "reverse-second", "--batch-rows", "10")
	runInference(c, ctx, "verify",
		"--spec", secondSpec, "--db-url", dbName, "--run-id", "reverse-second")
	runInference(c, ctx, "cutover",
		"--spec", secondSpec, "--db-url", dbName, "--run-id", "reverse-second",
		"--approve", planDigestOfRun(c, ctx, secondSpec, dbName, "reverse-second"),
		"--approver", "an operator", "--stabilize-for", "1h")

	// The pointer and the run agree again, which is the whole property.
	c.Assert(activeGenerationOfRun(c, ctx, secondSpec, dbName, "reverse-second"), qt.Equals, second)
	c.Assert(runPhase(c, ctx, db, "reverse-second"), qt.Equals, "cut_over")
	// The generation this cutover displaced keeps `cut_over`, and that is the
	// right answer rather than an omission: `rolled_back` means a rollback
	// returned queries to an earlier generation, which is not what happened to
	// this one. It was replaced, which every generation but the newest has been,
	// and the phase is a high-water mark of what the run reached. Which
	// generation queries read now is the pointer's to say.
	c.Assert(runPhase(c, ctx, db, "reverse-first"), qt.Equals, "cut_over")
}

// TestInferenceARolledBackGenerationCanBeRetiredE2E is the other move the
// forward-only table refused.
//
// Rolling a generation off the pointer and then retiring it is the ordinary end
// of one nobody wants back. The retirement destroyed the vectors and the phase
// change was refused, so the row stood at `rolled_back` describing a corpus that
// no longer existed -- and the run never became complete.
func TestInferenceARolledBackGenerationCanBeRetiredE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.TimescaleDB)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	db, dbName := freshTerminalPhaseDatabase(c, ctx, dbURL, "ptah_phase_retire")
	seedCLIArticles(c, ctx, db)
	endpoint := httptest.NewServer(http.HandlerFunc(embeddingsHandler(c)))
	defer endpoint.Close()

	firstSpec := writeCLISpec(c, endpoint.URL)
	secondSpec := writeCLISpecWithMetric(c, endpoint.URL, "cosine", "embedding_v2")
	first := completeCutOverRun(c, ctx, firstSpec, dbName, "retire-first")
	second := completeCutOverRun(c, ctx, secondSpec, dbName, "retire-second")

	runInference(c, ctx, "rollback",
		"--spec", secondSpec, "--db-url", dbName, "--to", first, "--window", "1h")
	c.Assert(runPhase(c, ctx, db, "retire-second"), qt.Equals, "rolled_back")
	activeStatusBefore, activeOwnerBefore := runStatusAndLease(c, ctx, db, "retire-first")
	c.Assert(activeStatusBefore, qt.Equals, "running")
	c.Assert(activeOwnerBefore, qt.Equals, "")

	digest := retirementDigestOf(c, ctx, secondSpec, dbName, second)
	runInference(c, ctx, "retire",
		"--spec", secondSpec, "--db-url", dbName, "--generation", second,
		"--approve", digest, "--approver", "an operator")

	c.Assert(runPhase(c, ctx, db, "retire-second"), qt.Equals, "retired")
	// And the run is complete, with no lease left on a generation that no
	// longer exists. `complete` had no producer at all before this: every run
	// ever built reported `running` for the life of the registry.
	status, owner := runStatusAndLease(c, ctx, db, "retire-second")
	c.Assert(status, qt.Equals, "complete")
	c.Assert(owner, qt.Equals, "")
	// The control against a completion that reached too far: the run behind the
	// generation queries actually read is untouched. It stays running and keeps
	// the empty lease recorded by cutover. That `rolled_back` itself does not
	// complete a run is the other control, and it is a unit test -- reaching it
	// here would need a rollback this fixture then never reverses.
	activeStatus, activeOwner := runStatusAndLease(c, ctx, db, "retire-first")
	c.Assert(activeStatus, qt.Equals, activeStatusBefore)
	c.Assert(activeOwner, qt.Equals, activeOwnerBefore)
}

// freshTerminalPhaseDatabase makes a database of its own and hands back a
// connection and the URL that reaches it.
func freshTerminalPhaseDatabase(
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

// runStatusAndLease reads a run's status and who holds it, from the row.
func runStatusAndLease(
	c *qt.C, ctx context.Context, db *sql.DB, runID string,
) (status, leaseOwner string) {
	c.Helper()
	err := db.QueryRowContext(ctx,
		`SELECT status, COALESCE(lease_owner, '') FROM ptah_embedding_run WHERE id = $1`, runID).
		Scan(&status, &leaseOwner)
	c.Assert(err, qt.IsNil)
	return status, leaseOwner
}
