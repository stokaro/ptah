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

// TestInferenceAPassReportsItsOwnWorkE2E is stokaro/ptah#2645.
//
// `backfill` and `catchup` printed `run.Progress`, which is cumulative for the
// whole run. So a catch-up with nothing to do reported the backfill's row count
// as "changed rows", and the completion signal the guide tells an operator to
// wait for — `0 changed rows, 0 tombstoned` — was unreachable on any run whose
// backfill had scanned anything. A second backfill that scanned nothing
// reported the same numbers as the first that did everything.
func TestInferenceAPassReportsItsOwnWorkE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.TimescaleDB)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	adminDB, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()

	name := fmt.Sprintf("ptah_pass_counters_%d", time.Now().UnixNano())
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

	first := runInference(c, ctx, "backfill",
		"--spec", specPath, "--db-url", dbName, "--run-id", cliRunID, "--batch-rows", "10")
	// The non-vacuity this needs: the first pass did something, so "nothing"
	// below is a difference rather than a fixture with no rows.
	c.Assert(first, qt.Contains, "backfill finished: 3 scanned, 3 embedded, 0 skipped")

	assertASecondBackfillReportsNothing(c, ctx, specPath, dbName)
	assertACatchUpWithNothingToDoSaysSo(c, ctx, specPath, dbName)
	assertACatchUpReportsOnlyWhatChanged(c, ctx, db, specPath, dbName)
}

// assertASecondBackfillReportsNothing is finding 2.
//
// The run is exhausted, so this pass scans nothing — and used to print the
// first pass's three rows as its own work.
func assertASecondBackfillReportsNothing(c *qt.C, ctx context.Context, specPath, dbURL string) {
	c.Helper()
	output := runInference(c, ctx, "backfill",
		"--spec", specPath, "--db-url", dbURL, "--run-id", cliRunID, "--batch-rows", "10")

	c.Assert(output, qt.Contains, "backfill finished: 0 scanned, 0 embedded, 0 skipped")
}

// assertACatchUpWithNothingToDoSaysSo is findings 1 and 3, and it is the
// sentence the guide publishes as the stop condition.
func assertACatchUpWithNothingToDoSaysSo(c *qt.C, ctx context.Context, specPath, dbURL string) {
	c.Helper()
	output := runInference(c, ctx, "catchup",
		"--spec", specPath, "--db-url", dbURL, "--run-id", cliRunID, "--batch-rows", "10")

	c.Assert(output, qt.Contains, "0 changed rows, 0 tombstoned")
}

// assertACatchUpReportsOnlyWhatChanged is the control.
//
// A counter that always printed zero would satisfy both assertions above while
// telling an operator nothing at all. One row changes, and exactly one is
// reported — not four, which is what the cumulative counter said.
func assertACatchUpReportsOnlyWhatChanged(
	c *qt.C, ctx context.Context, db *sql.DB, specPath, dbURL string,
) {
	c.Helper()
	_, err := db.ExecContext(ctx,
		`UPDATE articles SET title = 'First rewritten', updated_at = '9' WHERE id = 1`)
	c.Assert(err, qt.IsNil)

	output := runInference(c, ctx, "catchup",
		"--spec", specPath, "--db-url", dbURL, "--run-id", cliRunID, "--batch-rows", "10")

	c.Assert(output, qt.Contains, "1 changed rows, 0 tombstoned")
}
