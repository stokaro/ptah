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

	"ptah.run/internal/dbtarget"
)

// TestInferenceASecondGenerationUnderOneRunIDE2E is stokaro/ptah#2637, driven
// as the guide writes it.
//
// The migrate-to-another-model workflow edits the specification and re-runs the
// lifecycle. The run id is derived from a date in the guide and exported as
// PTAH_RUN_ID in the quick start, so leaving it alone is the ordinary thing to
// do — and nothing compared the specification with the generation the run was
// prepared for.
//
// What that produced, measured: the second `prepare` registered the new
// generation and added five columns to the user's table, then printed "run X
// already exists; leaving it as it is" at exit 0, and the `backfill` after it
// resumed the FIRST generation's finished cursor and reported
// "3 scanned, 3 embedded" having made no provider request and written no vector.
func TestInferenceASecondGenerationUnderOneRunIDE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.TimescaleDB)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	adminDB, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()

	name := fmt.Sprintf("ptah_two_generations_%d", time.Now().UnixNano())
	createE2EDatabase(c, ctx, adminDB, name)
	defer dropE2EDatabase(c, context.Background(), adminDB, name)

	dbName := replaceDatabaseName(c, dbURL, name)
	db, err := sql.Open("pgx", dbName)
	c.Assert(err, qt.IsNil)
	defer db.Close()
	seedCLIArticles(c, ctx, db)

	endpoint := httptest.NewServer(http.HandlerFunc(embeddingsHandler(c)))
	defer endpoint.Close()

	first := writeCLISpecWithMetric(c, endpoint.URL, "cosine", "embedding")
	second := writeCLISpecWithMetric(c, endpoint.URL, "l2", "embedding_v2")

	const runID = "2026-08-31-v2"
	runInference(c, ctx, "prepare", "--spec", first, "--db-url", dbName, "--run-id", runID)
	runInference(c, ctx, "backfill",
		"--spec", first, "--db-url", dbName, "--run-id", runID, "--batch-rows", "10")

	assertASecondPrepareIsRefusedBeforeItTouchesTheTable(c, ctx, db, second, dbName, runID)
	assertABackfillForAnotherGenerationIsRefused(c, ctx, second, dbName, runID)
	assertStatusForAnotherGenerationIsRefused(c, ctx, second, dbName, runID, "running")
	assertTheFirstGenerationIsStillUsable(c, ctx, first, dbName, runID)
	runInference(c, ctx, "abandon",
		"--db-url", dbName, "--run-id", runID, "--reason", "superseded test run")
	assertStatusForAnotherGenerationIsRefused(c, ctx, second, dbName, runID, "abandoned")
}

// assertStatusForAnotherGenerationIsRefused covers both sides of the status
// reporting path. A running run used the wrong specification to measure
// readiness; a terminal run short-circuited readiness and used the wrong
// consistency mode to render its stored watermarks.
func assertStatusForAnotherGenerationIsRefused(
	c *qt.C, ctx context.Context, specPath, dbURL, runID, state string,
) {
	c.Helper()
	_, err := runInferenceExpectingFailure(c, ctx, "status",
		"--spec", specPath, "--db-url", dbURL, "--run-id", runID)
	c.Assert(err, qt.ErrorMatches, `(?s).*was prepared for a different generation.*`,
		qt.Commentf("wrong-spec status must refuse a %s run", state))
}

// assertASecondPrepareIsRefusedBeforeItTouchesTheTable is finding 2.
//
// The refusal has to arrive before the DDL, not after it. `EnsureTarget` and
// `RegisterGeneration` both ran before the conflict was noticed, and neither is
// undone by noticing — so the old behavior left five columns on a production
// table and a second registry row, and said "leaving it as it is".
func assertASecondPrepareIsRefusedBeforeItTouchesTheTable(
	c *qt.C, ctx context.Context, db *sql.DB, specPath, dbURL, runID string,
) {
	c.Helper()
	before := columnsNamed(c, ctx, db, "embedding_v2%")

	_, err := runInferenceExpectingFailure(c, ctx, "prepare",
		"--spec", specPath, "--db-url", dbURL, "--run-id", runID)

	c.Assert(err, qt.ErrorMatches, `(?s).*was prepared for a different generation.*`)
	// Nothing was added. The count is taken from the catalog rather than from
	// what the verb said, because what the verb said was the defect.
	c.Assert(columnsNamed(c, ctx, db, "embedding_v2%"), qt.Equals, before)
	c.Assert(before, qt.Equals, 0,
		qt.Commentf("the fixture must start with no second-generation columns"))
}

// assertABackfillForAnotherGenerationIsRefused is finding 1.
//
// Without the check this exits 0 reporting rows it did not embed: the run's
// cursor is finished, so the scan returns nothing and the loop ends immediately
// with the counters the previous generation left behind.
func assertABackfillForAnotherGenerationIsRefused(
	c *qt.C, ctx context.Context, specPath, dbURL, runID string,
) {
	c.Helper()
	_, err := runInferenceExpectingFailure(c, ctx, "backfill",
		"--spec", specPath, "--db-url", dbURL, "--run-id", runID, "--batch-rows", "10")

	c.Assert(err, qt.ErrorMatches, `(?s).*was prepared for a different generation.*`)
}

// assertTheFirstGenerationIsStillUsable is the control.
//
// A check that refused every run would satisfy both assertions above while
// making the tool useless, and a restart under the same specification is what
// `prepare --help` promises is safe.
func assertTheFirstGenerationIsStillUsable(
	c *qt.C, ctx context.Context, specPath, dbURL, runID string,
) {
	c.Helper()
	output := runInference(c, ctx, "prepare",
		"--spec", specPath, "--db-url", dbURL, "--run-id", runID)
	c.Assert(output, qt.Contains, "already exists; leaving it as it is")

	backfilled := runInference(c, ctx, "backfill",
		"--spec", specPath, "--db-url", dbURL, "--run-id", runID, "--batch-rows", "10")
	c.Assert(backfilled, qt.Contains, "backfill finished")

	runInference(c, ctx, "catchup",
		"--spec", specPath, "--db-url", dbURL, "--run-id", runID, "--batch-rows", "10")
	verified := runInference(c, ctx, "verify",
		"--spec", specPath, "--db-url", dbURL, "--run-id", runID)
	c.Assert(verified, qt.Contains, "every deterministic layer passed")
}

// columnsNamed counts the columns of the source table matching a pattern.
func columnsNamed(c *qt.C, ctx context.Context, db *sql.DB, pattern string) int {
	c.Helper()
	var count int
	c.Assert(db.QueryRowContext(ctx,
		`SELECT count(*) FROM information_schema.columns
		  WHERE table_name = 'articles' AND column_name LIKE $1`, pattern).Scan(&count), qt.IsNil)
	return count
}
