//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the pgx driver for database/sql

	"go.5x5.cz/ptah/cmd/root"
	"go.5x5.cz/ptah/internal/dbtarget"
	"go.5x5.cz/ptah/internal/embedrelease"
)

// TestInferenceCLIE2E drives the whole lifecycle through the command line.
//
// Every other test of this feature reaches into a package. This one goes
// through the cobra tree an operator actually runs, against a live PostgreSQL
// with pgvector and a real HTTP endpoint speaking the embeddings API, and it is
// the only place that can catch what wiring gets wrong: a flag that is parsed
// and never used, a verb that resolves a specification differently from its
// neighbour, an exit code that says success over a refusal.
//
// The order is the lifecycle's, because each verb depends on the state the last
// one left (stokaro/ptah#2068).
func TestInferenceCLIE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.TimescaleDB)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	adminDB, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()

	name := fmt.Sprintf("ptah_cli_%d", time.Now().UnixNano())
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

	// The verbs, in the order the lifecycle runs them.
	assertPlanSaysWhatItKnows(c, ctx, specPath, dbName)
	assertPrepareIsIdempotent(c, ctx, specPath, dbName)
	assertBackfillEmbedsTheSource(c, ctx, specPath, dbName)
	assertCutoverIsRefusedBeforeCatchUp(c, ctx, specPath, dbName)
	assertCatchUpProcessesWhatChanged(c, ctx, db, specPath, dbName)
	assertVerifyPasses(c, ctx, specPath, dbName)
	assertStatusReportsTheRun(c, ctx, specPath, dbName)
	assertStatusAnswersARolloutGate(c, ctx, specPath, dbName)
	assertPauseStopsTheRunAndSaysWhy(c, ctx, specPath, dbName)
	assertResumeReturnsItToRunning(c, ctx, specPath, dbName)
	assertCutoverBindsToItsPlan(c, ctx, specPath, dbName)
	assertRetireIsRefusedWhileQueriesReadIt(c, ctx, specPath, dbName)
	assertRollbackIsRefusedWithoutEvidence(c, ctx, specPath, dbName)
	assertACutoverIsRefusedWhenTheSourceHasMovedOn(c, ctx, db, specPath, dbName)
	assertACutoverIsRefusedWhenSomebodyElseMovedThePointer(c, ctx, db, specPath, dbName)
	assertAnUnmaintainedPreviousGenerationBlocksNothing(c, ctx, db, specPath, dbName)
	assertCatchUpRefusesAModeThatRecordsNothing(c, ctx, endpoint.URL, dbName)
	assertAPostgreSQLURLIsNotRefusedAsAnotherEngine(c, ctx, specPath, dbName)
}

// assertAPostgreSQLURLIsNotRefusedAsAnotherEngine is the control for
// stokaro/ptah#2386's refusal.
//
// The unit tests beside cmd/inference measure every engine this namespace turns
// away, and every one of them would still pass if the check had been written to
// refuse everything. This is the half that needs a live server: a PostgreSQL URL
// reaches the database and answers from it.
func assertAPostgreSQLURLIsNotRefusedAsAnotherEngine(
	c *qt.C, ctx context.Context, specPath, dbURL string,
) {
	c.Helper()
	output := runInference(c, ctx, "status", "--spec", specPath, "--db-url", dbURL, "--run-id", cliRunID)

	c.Assert(output, qt.Not(qt.Contains), "ptah inference works against PostgreSQL")
	c.Assert(output, qt.Contains, "run "+cliRunID)
}

// TestInferenceCLIRollbackE2E is Phase K and Phase L, which the lifecycle above
// deliberately leaves unreachable.
//
// A cutover with no stabilization window leaves no rollback, and that is the
// honest answer rather than a gap: the old generation stops receiving changes
// the moment queries stop reading it, so what makes it a way back is somebody
// keeping it current. This asks for the window and then goes back through it.
//
// It is a separate test because it needs its own database: the run above ends
// with a pointer deliberately moved to a generation that does not exist, and a
// rollback over that would be measuring the wreckage (stokaro/ptah#2068).
func TestInferenceCLIRollbackE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.TimescaleDB)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	adminDB, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()

	name := fmt.Sprintf("ptah_cli_rollback_%d", time.Now().UnixNano())
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
	runInference(c, ctx, "backfill",
		"--spec", specPath, "--db-url", dbName, "--run-id", cliRunID, "--batch-rows", "10")
	runInference(c, ctx, "catchup",
		"--spec", specPath, "--db-url", dbName, "--run-id", cliRunID, "--batch-rows", "10")
	runInference(c, ctx, "verify", "--spec", specPath, "--db-url", dbName, "--run-id", cliRunID)

	generation := activeGenerationFrom(c, ctx, specPath, dbName)
	assertACutoverWithoutAWindowLeavesNoWayBack(c, ctx, db, specPath, dbName, generation)
	assertAWindowMakesTheGenerationAWayBack(c, ctx, db, specPath, dbName, generation)
	assertRollbackMovesThePointerBack(c, ctx, specPath, dbName, generation)
	assertNoWindowIsAskedForMeansNoWindow(c, ctx, db, specPath, dbName)
	assertADriftedGenerationIsNotAWayBack(c, ctx, db, specPath, dbName, generation)
	assertASkipIsNotAGapAndAGapIsNotASkip(c, ctx, db, specPath, dbName, generation)
	assertMaintainingAGenerationKeepsItAWayBack(c, ctx, db, specPath, dbName, generation)
	assertRetirementRecordsWhatItDestroyed(c, ctx, db, specPath, dbName)
}

// assertRetirementRecordsWhatItDestroyed is the one record whose subject cannot
// be inspected afterwards.
//
// Every other record here describes something still in the database. This
// describes an absence, so it names the objects that went rather than counting
// them -- and it is asserted last, because what it does cannot be undone.
func assertRetirementRecordsWhatItDestroyed(
	c *qt.C, ctx context.Context, db *sql.DB, specPath, dbURL string,
) {
	c.Helper()
	// A generation nothing points at: registered, never built, and safe to
	// destroy because there is nothing behind it to lose.
	registerBareGeneration(c, ctx, db, "a-retirable-one")
	path := filepath.Join(c.TempDir(), "retirement.json")

	digest := retirementDigestOf(c, ctx, specPath, dbURL, "a-retirable-one")
	output := runInference(c, ctx, "retire",
		"--spec", specPath, "--db-url", dbURL, "--generation", "a-retirable-one",
		"--approve", digest, "--approver", "an operator",
		"--evidence-file", path)
	c.Assert(output, qt.Contains, "is gone")

	body, err := os.ReadFile(path)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", output))
	var record embedrelease.Retirement
	c.Assert(json.Unmarshal(body, &record), qt.IsNil)
	c.Assert(record.Generation, qt.Equals, "a-retirable-one")
	c.Assert(record.Approver, qt.Equals, "an operator")
	// Named rather than counted, and named for what was ACTUALLY there. This
	// generation was registered and never built, so it has no index, and the
	// record says so: `DropsIndex` used to be a literal `true` and the record
	// claimed an index had been dropped whether or not one existed
	// (stokaro/ptah#2642).
	c.Assert(record.Objects, qt.HasLen, 1)
	c.Assert(record.Objects[0], qt.Contains, "column ")
	c.Assert(strings.Join(record.Objects, " "), qt.Not(qt.Contains), "index over")
	c.Assert(record.PlanDigest, qt.HasLen, 64)
	c.Assert(record.RetiredAt.IsZero(), qt.IsFalse)
}

// retirementDigestOf runs a refused retirement to read its plan digest.
func retirementDigestOf(
	c *qt.C, ctx context.Context, specPath, dbURL, generation string,
) string {
	c.Helper()
	refused, err := runInferenceExpectingFailure(c, ctx, "retire",
		"--spec", specPath, "--db-url", dbURL, "--generation", generation)
	c.Assert(err, qt.IsNotNil)
	return planDigestFrom(c, refused)
}

// assertMaintainingAGenerationKeepsItAWayBack is what makes a stabilization
// window true rather than promised.
//
// The window says a generation stays a way back for a period. Nothing makes
// that so on its own: an old generation stops receiving changes the moment
// queries stop reading it. Keeping it current means catching it up, and the
// window has to move with the catch-up.
//
// The fixture is the failure and the repair in one run: the source moves, the
// generation stops being a way back, and one catch-up with a window makes it one
// again.
func assertMaintainingAGenerationKeepsItAWayBack(
	c *qt.C, ctx context.Context, db *sql.DB, specPath, dbURL, generation string,
) {
	c.Helper()
	makeThePointerRecordItAsPrevious(c, ctx, db, generation)
	// Both halves broken at once, which is the state a stabilization window
	// decays into on its own: the source moved, and the promise to keep the
	// generation current has run out.
	_, err := db.ExecContext(ctx,
		`UPDATE articles SET body = 'moved again', updated_at = '40' WHERE id = 2`)
	c.Assert(err, qt.IsNil)
	_, err = db.ExecContext(ctx,
		`UPDATE ptah_embedding_generation SET maintained_until = NULL WHERE identity = $1`,
		generation)
	c.Assert(err, qt.IsNil)

	drifted, err := runInferenceExpectingFailure(c, ctx, "rollback",
		"--spec", specPath, "--db-url", dbURL, "--to", generation, "--window", "24h")
	c.Assert(err, qt.IsNotNil)
	c.Assert(drifted, qt.Contains, "rows are stale and this policy allows 0")
	c.Assert(drifted, qt.Contains, "no longer maintained")

	// One catch-up, with the window moving alongside it.
	output := runInference(c, ctx, "catchup",
		"--spec", specPath, "--db-url", dbURL, "--run-id", cliRunID,
		"--batch-rows", "10", "--maintain-for", "1h")
	c.Assert(output, qt.Contains, "stays a way back until ")
	// The window is what this catch-up wrote, not what an earlier step left:
	// it was cleared above, and nothing else has touched it.
	c.Assert(maintainedUntil(c, ctx, db, generation).Valid, qt.IsTrue)

	makeThePointerRecordItAsPrevious(c, ctx, db, generation)
	c.Assert(runInference(c, ctx, "rollback",
		"--spec", specPath, "--db-url", dbURL, "--to", generation, "--window", "24h"),
		qt.Contains, "queries now read "+generation)
}

// makeThePointerRecordItAsPrevious puts the generation where a rollback would
// return to it, leaving its maintenance window alone.
func makeThePointerRecordItAsPrevious(
	c *qt.C, ctx context.Context, db *sql.DB, generation string,
) {
	c.Helper()
	_, err := db.ExecContext(ctx,
		`UPDATE ptah_embedding_pointer SET active_generation = 'the-newest-one',
			previous_generation = $1 WHERE target_table = 'articles'`, generation)
	c.Assert(err, qt.IsNil)
}

// assertASkipIsNotAGapAndAGapIsNotASkip separates the two things a coverage
// layer reports about a rollback target.
//
// A row the specification declined to embed is a deliberate gap and does not
// make a generation unusable -- counting it would refuse a rollback to a corpus
// that is exactly what was asked for. A row nothing ever embedded is a real
// gap, and going back to a generation that has none of it answers those queries
// with nothing.
//
// The two are one finding apart in the same layer, which is why they need one
// fixture each.
func assertASkipIsNotAGapAndAGapIsNotASkip(
	c *qt.C, ctx context.Context, db *sql.DB, specPath, dbURL, generation string,
) {
	c.Helper()
	// Bring the generation back up to date after the drift above, so the only
	// thing either step below changes is coverage.
	runInference(c, ctx, "catchup",
		"--spec", specPath, "--db-url", dbURL, "--run-id", cliRunID, "--batch-rows", "10")
	makeTheGenerationAWayBack(c, ctx, db, generation)
	c.Assert(runInference(c, ctx, "rollback",
		"--spec", specPath, "--db-url", dbURL, "--to", generation, "--window", "24h"),
		qt.Contains, "queries now read "+generation)

	// A row the empty policy declines, caught up so the generation records the
	// skip. It is still a way back.
	_, err := db.ExecContext(ctx,
		`INSERT INTO articles (id, title, body, updated_at) VALUES (77, '', '', '30')`)
	c.Assert(err, qt.IsNil)
	runInference(c, ctx, "catchup",
		"--spec", specPath, "--db-url", dbURL, "--run-id", cliRunID, "--batch-rows", "10")
	makeTheGenerationAWayBack(c, ctx, db, generation)

	c.Assert(runInference(c, ctx, "rollback",
		"--spec", specPath, "--db-url", dbURL, "--to", generation, "--window", "24h"),
		qt.Contains, "queries now read "+generation)

	// A row nothing embedded, with no catch-up behind it. Now it is not.
	_, err = db.ExecContext(ctx,
		`INSERT INTO articles (id, title, body, updated_at) VALUES (78, 'Never', 'embedded', '31')`)
	c.Assert(err, qt.IsNil)
	makeTheGenerationAWayBack(c, ctx, db, generation)

	output, err := runInferenceExpectingFailure(c, ctx, "rollback",
		"--spec", specPath, "--db-url", dbURL, "--to", generation, "--window", "24h")

	c.Assert(err, qt.IsNotNil)
	c.Assert(output, qt.Contains, "1 rows are missing and this policy allows 0")
}

// makeTheGenerationAWayBack puts the generation where a rollback would return
// to it, with a live window, so nothing but the corpus can be the reason.
func makeTheGenerationAWayBack(c *qt.C, ctx context.Context, db *sql.DB, generation string) {
	c.Helper()
	_, err := db.ExecContext(ctx,
		`UPDATE ptah_embedding_pointer SET active_generation = 'the-newest-one',
			previous_generation = $1 WHERE target_table = 'articles'`, generation)
	c.Assert(err, qt.IsNil)
	_, err = db.ExecContext(ctx,
		`UPDATE ptah_embedding_generation SET maintained_until = now() + interval '1 hour'
		 WHERE identity = $1`, generation)
	c.Assert(err, qt.IsNil)
}

// assertADriftedGenerationIsNotAWayBack is the epic's rule that rollback is
// measured against the source rather than read off a status column.
//
// The generation is verified, maintained and present -- everything a status
// field would record. The source has moved since, so going back to it would
// answer queries with text that is no longer there, and the only thing that can
// say so is a comparison against the source right now.
func assertADriftedGenerationIsNotAWayBack(
	c *qt.C, ctx context.Context, db *sql.DB, specPath, dbURL, generation string,
) {
	c.Helper()
	// Put the generation back where a rollback would return to it, with a live
	// window, so nothing else can be the reason.
	_, err := db.ExecContext(ctx,
		`UPDATE ptah_embedding_pointer SET active_generation = 'the-newest-one',
			previous_generation = $1 WHERE target_table = 'articles'`, generation)
	c.Assert(err, qt.IsNil)
	_, err = db.ExecContext(ctx,
		`UPDATE ptah_embedding_generation SET maintained_until = now() + interval '1 hour'
		 WHERE identity = $1`, generation)
	c.Assert(err, qt.IsNil)

	// It is a way back, right up until the source moves.
	c.Assert(runInference(c, ctx, "rollback",
		"--spec", specPath, "--db-url", dbURL, "--to", generation, "--window", "24h"),
		qt.Contains, "queries now read "+generation)

	_, err = db.ExecContext(ctx,
		`UPDATE articles SET body = 'about pricing, revised again', updated_at = '20' WHERE id = 1`)
	c.Assert(err, qt.IsNil)
	_, err = db.ExecContext(ctx,
		`UPDATE ptah_embedding_pointer SET active_generation = 'the-newest-one',
			previous_generation = $1 WHERE target_table = 'articles'`, generation)
	c.Assert(err, qt.IsNil)

	output, err := runInferenceExpectingFailure(c, ctx, "rollback",
		"--spec", specPath, "--db-url", dbURL, "--to", generation, "--window", "24h")

	c.Assert(err, qt.IsNotNil)
	c.Assert(output, qt.Contains, "rollback refused")
	c.Assert(output, qt.Contains, "1 rows are stale and this policy allows 0")
}

// assertNoWindowIsAskedForMeansNoWindow is the flag's other answer, over a
// pointer that has somewhere to point.
//
// The first cutover in this test had no previous generation at all, so it took
// the same branch for a different reason. This one replaces a real generation
// and still asks for nothing, which is the only shape that separates "there is
// nobody to keep current" from "you did not ask me to".
func assertNoWindowIsAskedForMeansNoWindow(
	c *qt.C, ctx context.Context, db *sql.DB, specPath, dbURL string,
) {
	c.Helper()
	registerBareGeneration(c, ctx, db, "the-replaced-one")
	_, err := db.ExecContext(ctx,
		`UPDATE ptah_embedding_pointer SET active_generation = 'the-replaced-one',
			previous_generation = NULL
		 WHERE target_schema = 'public' AND target_table = 'articles'`)
	c.Assert(err, qt.IsNil)

	digest := planDigestOf(c, ctx, specPath, dbURL)
	output := runInference(c, ctx, "cutover",
		"--spec", specPath, "--db-url", dbURL, "--run-id", cliRunID,
		"--approve", digest, "--approver", "an operator")

	// A previous generation exists here, so this is the window's own sentence
	// and not the first-generation one. It is the control for the split above.
	c.Assert(output, qt.Contains, "no stabilization window was asked for")
	c.Assert(output, qt.Not(qt.Contains), "this is the first generation")
	c.Assert(maintainedUntil(c, ctx, db, "the-replaced-one").Valid, qt.IsFalse)
}

// assertACutoverWithoutAWindowLeavesNoWayBack is the default, and it says so.
//
// An operator who did not ask for a stabilization window does not get one, and
// the cutover tells them at the moment it happens rather than leaving them to
// find out when they need it.
func assertACutoverWithoutAWindowLeavesNoWayBack(
	c *qt.C, ctx context.Context, db *sql.DB, specPath, dbURL, generation string,
) {
	c.Helper()
	digest := planDigestOf(c, ctx, specPath, dbURL)

	output := runInference(c, ctx, "cutover",
		"--spec", specPath, "--db-url", dbURL, "--run-id", cliRunID,
		"--approve", digest, "--approver", "an operator")

	// This is the FIRST cutover over this target, so the sentence is about
	// there being no previous generation rather than about the window. The two
	// were one branch, and this assertion read the wrong one of them
	// (stokaro/ptah#2647): `assertNoWindowIsAskedForMeansNoWindow` below is
	// where a previous generation exists and no window was asked for.
	c.Assert(output, qt.Contains, "this is the first generation over this target")
	c.Assert(output, qt.Contains, "nothing to roll back to")
	c.Assert(output, qt.Not(qt.Contains), "no stabilization window was asked for")
	c.Assert(maintainedUntil(c, ctx, db, generation).Valid, qt.IsFalse)
}

// assertAWindowMakesTheGenerationAWayBack is Phase K, through the flag that
// opens it.
//
// The pointer is put back where it was and the cutover redone with
// `--stabilize-for`, so the window is opened by the verb rather than written
// into the table by the test. A fixture that set the column itself would pass
// over a cutover that never opened one.
func assertAWindowMakesTheGenerationAWayBack(
	c *qt.C, ctx context.Context, db *sql.DB, specPath, dbURL, generation string,
) {
	c.Helper()
	// Rewind: the previous cutover left this generation active, and a second
	// migration would leave it as the previous one. This is that state, minus
	// the window -- which is what the cutover below has to add.
	registerBareGeneration(c, ctx, db, "the-older-one")
	_, err := db.ExecContext(ctx,
		`UPDATE ptah_embedding_pointer SET active_generation = 'the-older-one', previous_generation = NULL
		 WHERE target_table = 'articles'`)
	c.Assert(err, qt.IsNil)
	c.Assert(maintainedUntil(c, ctx, db, generation).Valid, qt.IsFalse)

	digest := planDigestOf(c, ctx, specPath, dbURL)
	output := runInference(c, ctx, "cutover",
		"--spec", specPath, "--db-url", dbURL, "--run-id", cliRunID,
		"--approve", digest, "--approver", "an operator", "--stabilize-for", "1h")

	c.Assert(output, qt.Contains, "stays a way back until ")
	// The window landed on the generation being REPLACED, which is the one a
	// rollback would return to -- not on the one being cut over to.
	c.Assert(maintainedUntil(c, ctx, db, "the-older-one").Valid, qt.IsTrue)

	// And now the pointer records this generation as previous, so the rollback
	// below has somewhere to go back from.
	_, err = db.ExecContext(ctx,
		`UPDATE ptah_embedding_pointer SET active_generation = 'the-newer-one', previous_generation = $1
		 WHERE target_table = 'articles'`, generation)
	c.Assert(err, qt.IsNil)
	_, err = db.ExecContext(ctx,
		`UPDATE ptah_embedding_generation SET maintained_until =
			(SELECT maintained_until FROM ptah_embedding_generation WHERE identity = $2)
		 WHERE identity = $1`, generation, "the-older-one")
	c.Assert(err, qt.IsNil)
}

// assertRollbackMovesThePointerBack is Phase L, and the whole point of the two
// records above.
//
// The generation is verified and maintained, so going back to it is something
// Ptah will do rather than refuse. Every refusal in the other test was a
// missing one of those.
func assertRollbackMovesThePointerBack(
	c *qt.C, ctx context.Context, specPath, dbURL, generation string,
) {
	c.Helper()

	path := filepath.Join(c.TempDir(), "rollback.json")
	output := runInference(c, ctx, "rollback",
		"--spec", specPath, "--db-url", dbURL, "--to", generation, "--window", "24h",
		"--evidence-file", path)

	c.Assert(output, qt.Contains, "queries now read "+generation)
	c.Assert(output, qt.Contains, "which replaced the-newer-one")
	c.Assert(activeGenerationFrom(c, ctx, specPath, dbURL), qt.Equals, generation)

	// And a record of what was undone, which is a different question from why
	// the corpus changed: a reader looking for it in a list of cutovers finds a
	// pointer move with nothing attached to it.
	body, err := os.ReadFile(path)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", output))
	var record embedrelease.Rollback
	c.Assert(json.Unmarshal(body, &record), qt.IsNil)
	c.Assert(record.Generation, qt.Equals, generation)
	c.Assert(record.Replaced, qt.Equals, "the-newer-one")
	// What made going back possible, rather than only that it happened.
	c.Assert(record.Maintained, qt.IsTrue)
	c.Assert(record.Expires.IsZero(), qt.IsFalse)
	c.Assert(record.RolledBackAt.IsZero(), qt.IsFalse)
}

// planDigestOf runs a cutover without an approval to read the plan's digest.
func planDigestOf(c *qt.C, ctx context.Context, specPath, dbURL string) string {
	c.Helper()
	return planDigestOfRun(c, ctx, specPath, dbURL, cliRunID)
}

// planDigestOfRun is the same for a run this file did not name.
//
// The run id was a constant here, so a caller with its own run drove a cutover
// for a run that does not exist and got an error carrying no digest at all --
// which reads as "the plan has no digest" rather than as "you asked about the
// wrong run".
func planDigestOfRun(
	c *qt.C, ctx context.Context, specPath, dbURL, runID string,
) string {
	c.Helper()
	refused, err := runInferenceExpectingFailure(c, ctx, "cutover",
		"--spec", specPath, "--db-url", dbURL, "--run-id", runID)
	c.Assert(err, qt.IsNotNil)
	return planDigestFrom(c, refused)
}

// registerBareGeneration puts a generation in the registry with nothing behind
// it.
//
// It stands in for the one a previous migration left: the pointer names it, the
// registry knows it, and this test is not about how it was built.
func registerBareGeneration(c *qt.C, ctx context.Context, db *sql.DB, identity string) {
	c.Helper()
	_, err := db.ExecContext(ctx,
		`INSERT INTO ptah_embedding_generation (
			identity, spec_digest, reproducibility, dimension,
			target_schema, target_table, target_column, created_at)
		 VALUES ($1, $1, 'full', 4, 'public', 'articles', 'embedding', now())
		 ON CONFLICT (identity) DO NOTHING`, identity)
	c.Assert(err, qt.IsNil)
}

// maintainedUntil reads the maintenance window off a generation.
func maintainedUntil(
	c *qt.C, ctx context.Context, db *sql.DB, generation string,
) sql.NullTime {
	c.Helper()
	var until sql.NullTime
	c.Assert(db.QueryRowContext(ctx,
		`SELECT maintained_until FROM ptah_embedding_generation WHERE identity = $1`,
		generation).Scan(&until), qt.IsNil)
	return until
}

// assertCatchUpRefusesAModeThatRecordsNothing is structural absence rather than
// a silent no-op.
//
// A catch-up that "succeeded" over a mode recording no changes is a run
// reporting itself caught up on a source it never watched -- and the cutover
// that follows would rest on it.
func assertCatchUpRefusesAModeThatRecordsNothing(
	c *qt.C, ctx context.Context, endpointURL, dbURL string,
) {
	c.Helper()
	specPath := writeCLISpecWithMode(c, endpointURL, "immutable")

	output, err := runInferenceExpectingFailure(c, ctx, "catchup",
		"--spec", specPath, "--db-url", dbURL, "--run-id", cliRunID)

	c.Assert(err, qt.IsNotNil)
	c.Assert(output+err.Error(), qt.Contains,
		`catch-up needs a consistency mode that records changes, and this specification selects "immutable"`)
}

// assertAnUnmaintainedPreviousGenerationBlocksNothing records where this build
// actually stands, rather than where the design points.
//
// The retirement decision refuses a generation something can still roll back
// to. Reaching that refusal needs the previous generation to be ELIGIBLE, and
// eligibility is measured: verified recently, still maintained, index intact.
// Nothing here maintains an old generation after a cutover -- keeping both
// generations current for a stabilization window is Phase K, and it is not
// built.
//
// So the dependency is recorded and it protects nothing, which is the correct
// behaviour for the state the product is in: a way back nobody is keeping
// current is not a way back, and refusing a retirement to preserve it would
// leave the corpus twice its size for a rollback that would not work.
//
// The assertion is written this way on purpose. A fixture that forced the
// refusal by inventing eligibility would be testing a Phase K that does not
// exist, and it would go on passing after Phase K arrived and changed the
// answer (stokaro/ptah#2068).
func assertAnUnmaintainedPreviousGenerationBlocksNothing(
	c *qt.C, ctx context.Context, db *sql.DB, specPath, dbURL string,
) {
	c.Helper()
	generation := activeGenerationFrom(c, ctx, specPath, dbURL)
	// The state a cutover leaves behind: the pointer records this generation as
	// the one before the active one.
	_, err := db.ExecContext(ctx,
		`UPDATE ptah_embedding_pointer
		 SET active_generation = 'the-newer-one', previous_generation = $1
		 WHERE target_table = 'articles'`, generation)
	c.Assert(err, qt.IsNil)

	// Going back to it is refused, and the reason is what makes the retirement
	// answer below correct rather than lax.
	back, err := runInferenceExpectingFailure(c, ctx, "rollback",
		"--spec", specPath, "--db-url", dbURL, "--to", generation)
	c.Assert(err, qt.IsNotNil)
	c.Assert(back, qt.Contains, "no longer maintained")

	output, err := runInferenceExpectingFailure(c, ctx, "retire",
		"--spec", specPath, "--db-url", dbURL, "--generation", generation)

	c.Assert(err, qt.IsNotNil)
	// Refused for the approval the policy requires, and NOT for a rollback
	// dependency that would not survive being relied on.
	c.Assert(output, qt.Contains, "this policy requires an approval and none was given")
	c.Assert(output, qt.Not(qt.Contains), "can still be rolled back to this one")
	// The dependency is measured and reported even though it blocks nothing.
	// An operator about to destroy this needs to know it was somebody's way
	// back and that the way back had already stopped working -- destroying it
	// is fine today and would not have been last week, and the difference is
	// not in the refusal.
	c.Assert(output, qt.Contains,
		`generation "the-newer-one" records this as its way back, and that way back is `+
			`no longer eligible, so nothing here preserves it`)
}

// assertACutoverIsRefusedWhenTheSourceHasMovedOn is the verification result
// reaching the decision.
//
// A row changes and nothing catches up, so the vector answers for text the
// source no longer has. Verification says so, and the cutover has to refuse for
// that reason -- a plan that carried "verification passed" regardless would let
// a stale corpus through while the report beside it said otherwise.
func assertACutoverIsRefusedWhenTheSourceHasMovedOn(
	c *qt.C, ctx context.Context, db *sql.DB, specPath, dbURL string,
) {
	c.Helper()
	_, err := db.ExecContext(ctx,
		`UPDATE articles SET body = 'about pricing, revised', updated_at = '11' WHERE id = 1`)
	c.Assert(err, qt.IsNil)

	report, verifyErr := runInferenceExpectingFailure(c, ctx, "verify",
		"--spec", specPath, "--db-url", dbURL, "--run-id", cliRunID)
	c.Assert(verifyErr, qt.IsNotNil)
	c.Assert(report, qt.Contains, "computed from a source state that has since changed")

	output, err := runInferenceExpectingFailure(c, ctx, "cutover",
		"--spec", specPath, "--db-url", dbURL, "--run-id", cliRunID)

	c.Assert(err, qt.IsNotNil)
	c.Assert(output, qt.Contains, "verification did not pass and nothing was accepted")
}

// assertACutoverIsRefusedWhenSomebodyElseMovedThePointer is what protects a
// cutover from somebody else's.
//
// The pointer moves between the run that printed the digest and the run that
// approves it. This CLI builds and executes in one process, so the plan it
// builds the second time reads the NEW pointer -- and is therefore a different
// plan, with a different digest, which the approval does not cover. The
// approval is the guard here, and the refusal names both digests.
//
// The domain's own drift check -- comparing the plan's expected previous
// generation against what is read back -- is for a caller that PERSISTS a plan
// and executes it later. That caller does not exist yet, and pretending this
// one is it would be a fixture asserting something the code cannot reach.
func assertACutoverIsRefusedWhenSomebodyElseMovedThePointer(
	c *qt.C, ctx context.Context, db *sql.DB, specPath, dbURL string,
) {
	c.Helper()
	// Catch up first, so the only thing left to refuse is the pointer.
	runInference(c, ctx, "catchup",
		"--spec", specPath, "--db-url", dbURL, "--run-id", cliRunID, "--batch-rows", "10")

	refused, err := runInferenceExpectingFailure(c, ctx, "cutover",
		"--spec", specPath, "--db-url", dbURL, "--run-id", cliRunID)
	c.Assert(err, qt.IsNotNil)
	digest := planDigestFrom(c, refused)

	_, err = db.ExecContext(ctx,
		`UPDATE ptah_embedding_pointer SET active_generation = 'somebody-elses-generation'
		 WHERE target_table = 'articles'`)
	c.Assert(err, qt.IsNil)

	output, err := runInferenceExpectingFailure(c, ctx, "cutover",
		"--spec", specPath, "--db-url", dbURL, "--run-id", cliRunID,
		"--approve", digest, "--approver", "an operator")

	c.Assert(err, qt.IsNotNil)
	c.Assert(output, qt.Contains, "the approval is bound to plan "+digest+" and this plan is ")
	// And the pointer is left where the other operator put it.
	var active string
	c.Assert(db.QueryRowContext(ctx,
		`SELECT active_generation FROM ptah_embedding_pointer WHERE target_table = 'articles'`).
		Scan(&active), qt.IsNil)
	c.Assert(active, qt.Equals, "somebody-elses-generation")
}

// cliRunID is the run every verb below addresses.
const cliRunID = "cli-run"

// seedCLIArticles creates the source table the specification names.
//
// The source table and nothing else. The vector column and its metadata are
// `prepare`'s job, and a fixture that wrote them made that verb's own work
// invisible: `prepare` returned without creating anything for as long as this
// function did it first, and every assertion downstream still passed
// (stokaro/ptah#2390). The extension stays, because Ptah refuses to install one.
func seedCLIArticles(c *qt.C, ctx context.Context, db *sql.DB) {
	c.Helper()
	for _, statement := range []string{
		`CREATE EXTENSION IF NOT EXISTS vector`,
		`CREATE TABLE articles (
			id BIGINT PRIMARY KEY, title TEXT, body TEXT, updated_at TEXT NOT NULL)`,
		`INSERT INTO articles (id, title, body, updated_at) VALUES
			(1, 'First',  'about pricing', '7'),
			(2, 'Second', 'about support', '7'),
			(3, 'Third',  'about billing', '7')`,
	} {
		_, err := db.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}
}

// writeCLISpec writes the specification file the verbs are given.
func writeCLISpec(c *qt.C, endpoint string) string {
	c.Helper()
	return writeCLISpecWithMode(c, endpoint, "outbox")
}

// writeCLISpecWithMetric writes one with a chosen distance metric.
func writeCLISpecWithMetric(c *qt.C, endpoint, metric, column string) string {
	c.Helper()
	return writeCLISpecFull(c, endpoint, "outbox", metric, column)
}

// writeCLISpecWithMode writes one with a chosen consistency mode.
func writeCLISpecWithMode(c *qt.C, endpoint, mode string) string {
	c.Helper()
	return writeCLISpecFull(c, endpoint, mode, "cosine", "embedding")
}

// writeCLISpecFull writes one with both chosen.
func writeCLISpecFull(c *qt.C, endpoint, mode, metric, column string) string {
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
  column: %s
  representation: vector
  metric: %s
consistency:
  mode: %s
policy:
  require_exact_approval: true
  require_consistency_mode: true
`, endpoint, column, metric, mode)
	path := filepath.Join(c.TempDir(), "spec.yaml")
	c.Assert(os.WriteFile(path, []byte(document), 0o600), qt.IsNil)
	return path
}

// embeddingsHandler answers the embeddings API with a vector derived from each
// input, so a value read back from the database says which text produced it.
func embeddingsHandler(c *qt.C) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		var body struct {
			Model string   `json:"model"`
			Input []string `json:"input"`
		}
		c.Assert(json.NewDecoder(request.Body).Decode(&body), qt.IsNil)

		type entry struct {
			Index     int       `json:"index"`
			Embedding []float32 `json:"embedding"`
		}
		answer := struct {
			Data  []entry `json:"data"`
			Model string  `json:"model"`
			Usage struct {
				PromptTokens int `json:"prompt_tokens"`
				TotalTokens  int `json:"total_tokens"`
			} `json:"usage"`
		}{Model: body.Model}
		for index, input := range body.Input {
			vector := make([]float32, 4)
			for component := range vector {
				vector[component] = float32(len(input) + component)
			}
			answer.Data = append(answer.Data, entry{Index: index, Embedding: vector})
		}
		answer.Usage.PromptTokens = len(body.Input)
		answer.Usage.TotalTokens = len(body.Input) * 2
		writer.Header().Set("Content-Type", "application/json")
		c.Assert(json.NewEncoder(writer).Encode(answer), qt.IsNil)
	}
}

// assertPlanSaysWhatItKnows is the read-only verb, and the one that has to say
// where its answers came from.
func assertPlanSaysWhatItKnows(c *qt.C, ctx context.Context, specPath, dbURL string) {
	c.Helper()

	output := runInference(c, ctx, "plan", "--spec", specPath, "--db-url", dbURL)

	c.Assert(output, qt.Contains, "source.estimated_rows = 3 (measured)")
	c.Assert(output, qt.Contains, "target.capability.vector_type = true (measured)")
	c.Assert(output, qt.Contains, "[backfill] embed 3 in-scope source rows")
	c.Assert(output, qt.Contains, "Consistency mode: outbox")
	// Nothing has run yet, so the column is not there and the plan says it
	// would create one. The other half -- that a plan stops proposing work
	// already done -- is asserted after prepare, where the state it reads is
	// one Ptah produced rather than one this fixture wrote (stokaro/ptah#2390).
	c.Assert(output, qt.Contains, "target.exists = false (measured)")
	c.Assert(output, qt.Contains, "create the vector column")
}

// assertPrepareIsIdempotent runs the mutating setup twice.
//
// Twice is what happens when a run is restarted, and the second time must not
// overwrite a run that may be halfway through a backfill.
func assertPrepareIsIdempotent(c *qt.C, ctx context.Context, specPath, dbURL string) {
	c.Helper()

	first := runInference(c, ctx, "prepare",
		"--spec", specPath, "--db-url", dbURL, "--run-id", cliRunID)
	second := runInference(c, ctx, "prepare",
		"--spec", specPath, "--db-url", dbURL, "--run-id", cliRunID)

	c.Assert(first, qt.Contains, "prepared run "+cliRunID)
	c.Assert(first, qt.Contains, "snapshot boundary: ")
	c.Assert(second, qt.Contains, "already exists; leaving it as it is")

	// The columns exist now, and running prepare twice did not disturb them.
	assertTargetColumns(c, ctx, dbURL)

	// And the plan stops proposing the work. This is the assertion the fixture
	// used to satisfy by writing the DDL itself, which made it a statement
	// about the fixture rather than about prepare.
	after := runInference(c, ctx, "plan", "--spec", specPath, "--db-url", dbURL)
	c.Assert(after, qt.Contains, "target.exists = true (measured)")
	c.Assert(after, qt.Not(qt.Contains), "create the vector column")
}

// assertTargetColumns reads the catalog rather than the verb's own output.
//
// prepare printed "prepared run ..." for as long as it created nothing, so a
// test reading what it said would have passed throughout.
func assertTargetColumns(c *qt.C, ctx context.Context, dbURL string) {
	c.Helper()
	db, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer db.Close()

	rows, err := db.QueryContext(ctx, `SELECT column_name, udt_name
		FROM information_schema.columns
		WHERE table_name = 'articles' AND column_name LIKE 'embedding%'
		ORDER BY column_name`)
	c.Assert(err, qt.IsNil)
	defer rows.Close()

	found := map[string]string{}
	for rows.Next() {
		var name, kind string
		c.Assert(rows.Scan(&name, &kind), qt.IsNil)
		found[name] = kind
	}
	c.Assert(rows.Err(), qt.IsNil)

	c.Assert(found, qt.DeepEquals, map[string]string{
		"embedding":                "vector",
		"embedding_generation":     "text",
		"embedding_input_hash":     "text",
		"embedding_source_version": "text",
		"embedding_state":          "text",
	})
}

// assertBackfillEmbedsTheSource runs the loop through the CLI, against a real
// HTTP endpoint.
func assertBackfillEmbedsTheSource(c *qt.C, ctx context.Context, specPath, dbURL string) {
	c.Helper()

	output := runInference(c, ctx, "backfill",
		"--spec", specPath, "--db-url", dbURL, "--run-id", cliRunID,
		"--batch-rows", "2", "--batch-inputs", "2")

	c.Assert(output, qt.Contains, "backfill finished: 3 scanned, 3 embedded, 0 skipped")
}

// assertCutoverIsRefusedBeforeCatchUp is the epic's cutover rule, reached
// through the command line.
//
// The source is mutable and catch-up has not run, so nothing establishes that
// the backfill covers the source as it is now. The refusal has to be an
// error -- an exit code of zero over a refusal is the failure that makes every
// automated pipeline downstream wrong.
func assertCutoverIsRefusedBeforeCatchUp(c *qt.C, ctx context.Context, specPath, dbURL string) {
	c.Helper()

	output, err := runInferenceExpectingFailure(c, ctx, "cutover",
		"--spec", specPath, "--db-url", dbURL, "--run-id", cliRunID)

	c.Assert(err, qt.IsNotNil)
	c.Assert(output, qt.Contains, "cutover refused")
	c.Assert(output, qt.Contains, "the source is mutable and the run declared no consistency mode")

	// The gate says the same thing, at the same moment, in the form a rollout
	// system reads. This is the half that makes the later "ready" assertion
	// mean something: a readiness that answered true unconditionally would
	// satisfy that one and this is where it reddens.
	body := runInference(c, ctx, "status",
		"--spec", specPath, "--db-url", dbURL, "--run-id", cliRunID, "--format", "json")
	var document struct {
		Readiness struct {
			CutoverReady bool     `json:"cutover_ready"`
			Blockers     []string `json:"blockers"`
		} `json:"readiness"`
	}
	c.Assert(json.Unmarshal([]byte(body), &document), qt.IsNil, qt.Commentf("%s", body))
	c.Assert(document.Readiness.CutoverReady, qt.IsFalse)
	c.Assert(document.Readiness.Blockers, qt.Contains,
		"the source is mutable and the run declared no consistency mode")

	// And as the refusal a gate waits on. An init container that keeps failing
	// is the whole of a rollout gate, so this is the interface rather than the
	// JSON for anyone who is not parsing it. The exit CODE is asserted from a
	// process, in TestInferenceRolloutGateE2E: a command run in this process
	// returns an error and never a status.
	gate, gateErr := runInferenceExpectingFailure(c, ctx, "status",
		"--spec", specPath, "--db-url", dbURL, "--run-id", cliRunID, "--require-ready")
	c.Assert(gateErr, qt.ErrorMatches, `the generation is not ready: verified=.*, cutover ready=false`)
	// The report is still on stdout. A gate that failed silently leaves whoever
	// reads the pod's logs with a number and nothing else.
	c.Assert(gate, qt.Contains, "cutover ready: false")
}

// assertCatchUpProcessesWhatChanged changes the source and catches up.
func assertCatchUpProcessesWhatChanged(
	c *qt.C, ctx context.Context, db *sql.DB, specPath, dbURL string,
) {
	c.Helper()
	for _, statement := range []string{
		`UPDATE articles SET title = 'First rewritten', updated_at = '8' WHERE id = 1`,
		`INSERT INTO articles (id, title, body, updated_at) VALUES (4, 'Fourth', 'about renewals', '8')`,
		`DELETE FROM articles WHERE id = 3`,
	} {
		_, err := db.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}

	output := runInference(c, ctx, "catchup",
		"--spec", specPath, "--db-url", dbURL, "--run-id", cliRunID, "--batch-rows", "10")

	c.Assert(output, qt.Contains, "caught up to transaction ")
	c.Assert(output, qt.Contains, "1 tombstoned")
}

// assertVerifyPasses runs the deterministic layers over what the two loops
// produced.
func assertVerifyPasses(c *qt.C, ctx context.Context, specPath, dbURL string) {
	c.Helper()

	output := runInference(c, ctx, "verify",
		"--spec", specPath, "--db-url", dbURL, "--run-id", cliRunID)

	c.Assert(output, qt.Contains, "every deterministic layer passed")
	c.Assert(output, qt.Contains, "3 source rows, 3 target rows")
}

// assertStatusReportsTheRun is the read-only verb an operator reaches for after
// a refusal.
func assertStatusReportsTheRun(c *qt.C, ctx context.Context, specPath, dbURL string) {
	c.Helper()

	output := runInference(c, ctx, "status",
		"--spec", specPath, "--db-url", dbURL, "--run-id", cliRunID)

	c.Assert(output, qt.Contains, "run "+cliRunID+": ")
	// Six scanned across both loops: three by the backfill, and three changed
	// keys by catch-up. Five embedded: the backfill's three, plus the rewritten
	// row and the inserted one -- the third change was a delete, which is the
	// tombstone rather than an embedding.
	c.Assert(output, qt.Contains, "scanned 6, embedded 5, skipped 0, deleted 1")
	c.Assert(output, qt.Contains, "snapshot boundary: ")
	c.Assert(output, qt.Contains, "catch-up watermark: ")
}

// assertStatusAnswersARolloutGate is what a deployment waits on.
//
// A new model's deployment must not start until the persistent state it will
// read has been built and measured, and the two conditions it waits for are
// these. This asserts them at the point in the lifecycle where they first
// become true: the corpus is embedded, caught up and verified, and the only
// thing left is somebody signing for it.
//
// The approval is reported separately from readiness on purpose. Under
// `require_exact_approval`, which this specification sets, folding it into
// `cutover_ready` would leave a gate waiting forever on a state that is
// finished.
func assertStatusAnswersARolloutGate(c *qt.C, ctx context.Context, specPath, dbURL string) {
	c.Helper()

	body := runInference(c, ctx, "status",
		"--spec", specPath, "--db-url", dbURL, "--run-id", cliRunID, "--format", "json")

	var document struct {
		Run struct {
			RunID string `json:"run_id"`
			Phase string `json:"phase"`
		} `json:"run"`
		Readiness struct {
			Verified         bool     `json:"verified"`
			CutoverReady     bool     `json:"cutover_ready"`
			ApprovalRequired bool     `json:"approval_required"`
			PlanDigest       string   `json:"plan_digest"`
			Blockers         []string `json:"blockers"`
			SourceRows       int      `json:"source_rows"`
			TargetRows       int      `json:"target_rows"`
			MeasuredAt       string   `json:"measured_at"`
		} `json:"readiness"`
	}
	c.Assert(json.Unmarshal([]byte(body), &document), qt.IsNil, qt.Commentf("%s", body))

	c.Assert(document.Run.RunID, qt.Equals, cliRunID)
	c.Assert(document.Readiness.Verified, qt.IsTrue)
	c.Assert(document.Readiness.CutoverReady, qt.IsTrue,
		qt.Commentf("blocked by %v", document.Readiness.Blockers))
	c.Assert(document.Readiness.Blockers, qt.HasLen, 0)
	// Owed, and named, so the gate can tell "not finished" from "waiting for a
	// person" -- and so whoever that person is knows what to approve.
	c.Assert(document.Readiness.ApprovalRequired, qt.IsTrue)
	c.Assert(document.Readiness.PlanDigest, qt.HasLen, 64)
	// Measured rather than remembered: the counts are this run's, taken now.
	c.Assert(document.Readiness.SourceRows, qt.Equals, 3)
	c.Assert(document.Readiness.TargetRows, qt.Equals, 3)
	c.Assert(document.Readiness.MeasuredAt, qt.Not(qt.Equals), "")

	// And the digest the gate reports is the one the cutover verb accepts,
	// which is the whole point of the two sharing a decision. A gate agreeing
	// with the verb by coincidence is one that will eventually let a deployment
	// proceed against a generation the cutover then refuses.
	c.Assert(document.Readiness.PlanDigest, qt.Contains, planDigestOf(c, ctx, specPath, dbURL))

	// And the gate opens, which is the assertion the refused one before
	// catch-up exists to give meaning to: a --require-ready that always
	// succeeded would pass here and redden there.
	gate := runInference(c, ctx, "status",
		"--spec", specPath, "--db-url", dbURL, "--run-id", cliRunID, "--require-ready")
	c.Assert(gate, qt.Contains, "verified: true, cutover ready: true")
}

// assertPauseStopsTheRunAndSaysWhy is stokaro/ptah#2474: the run status had a
// paused value and the checkpoint code knew how to enter it, and an operator
// could not.
//
// A long backfill against a rate-limited provider is exactly when pausing is
// the thing you want, and the answer was to kill the process -- which works,
// because the run is resumable, but it leaves the run reading `running` while
// nothing runs.
func assertPauseStopsTheRunAndSaysWhy(c *qt.C, ctx context.Context, specPath, dbURL string) {
	c.Helper()

	before := fencingTokenOf(c, ctx, specPath, dbURL)
	output := runInference(c, ctx, "pause",
		"--spec", specPath, "--db-url", dbURL, "--run-id", cliRunID,
		"--reason", "the provider is rate limiting us")

	c.Assert(output, qt.Contains, "paused run "+cliRunID)
	c.Assert(output, qt.Contains, "the provider is rate limiting us")

	// The token moved, which is what makes a pause take effect against a worker
	// that is running rather than take note beside it.
	after := fencingTokenOf(c, ctx, specPath, dbURL)
	c.Assert(after > before, qt.IsTrue, qt.Commentf("token %d did not move past %d", after, before))

	// And status answers the question the reason was required for. It read the
	// failure class alone, which a pause does not set, so the reason was stored
	// and shown nowhere.
	status := runInference(c, ctx, "status",
		"--spec", specPath, "--db-url", dbURL, "--run-id", cliRunID)
	c.Assert(status, qt.Contains, "paused")
	c.Assert(status, qt.Contains, "paused: the provider is rate limiting us")
}

// assertResumeReturnsItToRunning is the other half, and the refusal that keeps
// the verb from being a way to set a status.
func assertResumeReturnsItToRunning(c *qt.C, ctx context.Context, specPath, dbURL string) {
	c.Helper()

	paused := fencingTokenOf(c, ctx, specPath, dbURL)
	output := runInference(c, ctx, "resume",
		"--spec", specPath, "--db-url", dbURL, "--run-id", cliRunID)
	c.Assert(output, qt.Contains, "resumed run "+cliRunID)

	status := runInference(c, ctx, "status",
		"--spec", specPath, "--db-url", dbURL, "--run-id", cliRunID)
	c.Assert(status, qt.Not(qt.Contains), "paused")
	// And the reason goes with it. This asserts what an operator SEES: status
	// prints a reason for a paused run, so a resumed one shows none either way.
	// That the field is cleared in the run itself is asserted where it can be
	// read directly, in TestResume_FencesTheWorkerThePauseStopped.
	c.Assert(status, qt.Not(qt.Contains), "rate limiting")
	// Resuming fences too. The worker the pause stopped is not necessarily
	// gone, and returning the run to running under its token would put it back
	// where the fence exists to stop it.
	c.Assert(fencingTokenOf(c, ctx, specPath, dbURL) > paused, qt.IsTrue)

	// A second resume is refused rather than quietly setting the status again.
	_, err := runInferenceExpectingFailure(c, ctx, "resume",
		"--spec", specPath, "--db-url", dbURL, "--run-id", cliRunID)
	c.Assert(err, qt.ErrorMatches, `.*only a paused run resumes, and this one is running`)
}

// fencingTokenOf reads the run's token out of what status prints, which is the
// number the refusal a fenced worker sees names.
func fencingTokenOf(c *qt.C, ctx context.Context, specPath, dbURL string) int {
	c.Helper()
	output := runInference(c, ctx, "status",
		"--spec", specPath, "--db-url", dbURL, "--run-id", cliRunID)
	_, after, found := strings.Cut(output, "fencing token ")
	c.Assert(found, qt.IsTrue, qt.Commentf("status printed no fencing token:\n%s", output))
	line, _, _ := strings.Cut(after, "\n")
	token, err := strconv.Atoi(strings.TrimSpace(line))
	c.Assert(err, qt.IsNil)
	return token
}

// assertCutoverBindsToItsPlan is what an approval is for.
//
// The first run is refused for having no approval and prints the plan's digest.
// The second supplies it and succeeds. A digest for a different plan is refused,
// which is the whole mechanism: the approval covers this evidence and nothing
// else.
func assertCutoverBindsToItsPlan(c *qt.C, ctx context.Context, specPath, dbURL string) {
	c.Helper()

	unapproved, err := runInferenceExpectingFailure(c, ctx, "cutover",
		"--spec", specPath, "--db-url", dbURL, "--run-id", cliRunID)
	c.Assert(err, qt.IsNotNil)
	c.Assert(unapproved, qt.Contains, "this policy requires an approval and none was given")

	wrong, err := runInferenceExpectingFailure(c, ctx, "cutover",
		"--spec", specPath, "--db-url", dbURL, "--run-id", cliRunID,
		"--approve", "0123456789ab", "--approver", "an operator")
	c.Assert(err, qt.IsNotNil)
	c.Assert(wrong, qt.Contains, "the approval is bound to plan 0123456789ab and this plan is ")

	digest := planDigestFrom(c, unapproved)
	approved := runInference(c, ctx, "cutover",
		"--spec", specPath, "--db-url", dbURL, "--run-id", cliRunID,
		"--approve", digest, "--approver", "an operator")

	c.Assert(approved, qt.Contains, "queries now read generation ")
	c.Assert(approved, qt.Contains, "(plan "+digest+")")
}

// planDigestFrom reads the short plan digest a refusal printed.
//
// The first field after the word rather than the rest of the line: a cutover
// prints the digest alone and a retirement prints it with the row count beside
// it, and a helper that took the whole remainder handed the second one a string
// no approval could ever match -- while the refusal rendered both to the same
// twelve characters and read as though they agreed.
func planDigestFrom(c *qt.C, output string) string {
	c.Helper()
	for line := range strings.SplitSeq(output, "\n") {
		if after, found := strings.CutPrefix(line, "plan "); found {
			return strings.Fields(after)[0]
		}
	}
	c.Fatalf("no plan digest in:\n%s", output)
	return ""
}

// assertRetireIsRefusedWhileQueriesReadIt is the destructive verb's first
// guard.
//
// The generation was just cut over to, so retiring it would leave queries
// nothing to read.
func assertRetireIsRefusedWhileQueriesReadIt(c *qt.C, ctx context.Context, specPath, dbURL string) {
	c.Helper()
	active := activeGenerationFrom(c, ctx, specPath, dbURL)

	output, err := runInferenceExpectingFailure(c, ctx, "retire",
		"--spec", specPath, "--db-url", dbURL, "--generation", active)

	c.Assert(err, qt.IsNotNil)
	c.Assert(output, qt.Contains, "retirement refused")
	c.Assert(output, qt.Contains,
		"queries read this generation, so retiring it would leave them nothing to read")
}

// activeGenerationFrom reads which generation the cutover made active.
func activeGenerationFrom(c *qt.C, ctx context.Context, specPath, dbURL string) string {
	c.Helper()
	return activeGenerationOfRun(c, ctx, specPath, dbURL, cliRunID)
}

// activeGenerationOfRun is the same for a run this file did not name. See
// [planDigestOfRun] for why the constant had to become a parameter.
func activeGenerationOfRun(
	c *qt.C, ctx context.Context, specPath, dbURL, runID string,
) string {
	c.Helper()
	status := runInference(c, ctx, "status",
		"--spec", specPath, "--db-url", dbURL, "--run-id", runID)
	for line := range strings.SplitSeq(status, "\n") {
		if after, found := strings.CutPrefix(strings.TrimSpace(line), "- generation: "); found {
			return strings.TrimSpace(after)
		}
	}
	c.Fatalf("no generation in:\n%s", status)
	return ""
}

// assertRollbackIsRefusedWithoutEvidence is the epic's rule that rollback is
// measured rather than assumed.
//
// There is a previous generation in the pointer's history and it has never been
// verified, so going back to it is not something Ptah will report as available.
func assertRollbackIsRefusedWithoutEvidence(c *qt.C, ctx context.Context, specPath, dbURL string) {
	c.Helper()
	active := activeGenerationFrom(c, ctx, specPath, dbURL)

	output, err := runInferenceExpectingFailure(c, ctx, "rollback",
		"--spec", specPath, "--db-url", dbURL, "--to", active)

	c.Assert(err, qt.IsNotNil)
	c.Assert(output, qt.Contains, "rollback refused")
	// Verified, because `verify` ran above and recorded it. Not maintained,
	// because nothing asked for a stabilization window -- and a generation
	// nobody is keeping current drifts from the source with every write.
	c.Assert(output, qt.Contains, "no longer maintained")
}

// runInference runs one verb and requires it to succeed.
func runInference(c *qt.C, ctx context.Context, args ...string) string {
	c.Helper()
	output, err := runInferenceCommand(ctx, args...)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", output))
	return output
}

// runInferenceExpectingFailure runs one verb and returns what it printed and
// why it failed.
//
// Separate from runInference rather than a flag, because the two assert
// opposite things and a shared helper choosing between them would be the
// conditional the style rules forbid.
func runInferenceExpectingFailure(
	c *qt.C, ctx context.Context, args ...string,
) (string, error) {
	c.Helper()
	return runInferenceCommand(ctx, args...)
}

// runInferenceCommand drives the real cobra tree.
func runInferenceCommand(ctx context.Context, args ...string) (string, error) {
	cmd := root.NewRootCommand()
	var output bytes.Buffer
	cmd.SetOut(&output)
	cmd.SetErr(&output)
	cmd.SetArgs(append([]string{"inference"}, args...))
	err := cmd.ExecuteContext(ctx)
	return output.String(), err
}

// TestInferenceAFirstCutoverIsNotToldItAskedForNothingE2E is stokaro/ptah#2647
// findings 2 to 4.
//
// `openStabilization` folded "there is no previous generation" and "you asked
// for no window" into one branch and printed the second sentence for both, so
// an operator who typed `--stabilize-for 24h` on a first cutover was told they
// had not asked for a window.
func TestInferenceAFirstCutoverIsNotToldItAskedForNothingE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.TimescaleDB)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	adminDB, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()

	name := fmt.Sprintf("ptah_first_cutover_%d", time.Now().UnixNano())
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
	runInference(c, ctx, "backfill",
		"--spec", specPath, "--db-url", dbName, "--run-id", cliRunID, "--batch-rows", "10")
	runInference(c, ctx, "catchup",
		"--spec", specPath, "--db-url", dbName, "--run-id", cliRunID, "--batch-rows", "10")
	runInference(c, ctx, "verify", "--spec", specPath, "--db-url", dbName, "--run-id", cliRunID)

	digest := planDigestOf(c, ctx, specPath, dbName)
	output := runInference(c, ctx, "cutover",
		"--spec", specPath, "--db-url", dbName, "--run-id", cliRunID,
		"--approve", digest, "--approver", "an operator", "--stabilize-for", "24h")

	c.Assert(output, qt.Contains, "queries now read generation ")
	// What is true: there is no previous generation. What is not: that nobody
	// asked for a window.
	c.Assert(output, qt.Contains, "this is the first generation over this target")
	c.Assert(output, qt.Not(qt.Contains), "no stabilization window was asked for")
}
