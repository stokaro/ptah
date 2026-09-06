//go:build integration

package integration_test

// The claim, driven through the verbs an operator runs.
//
// The package-level tests beside this one drive the engine directly. This runs
// the real sequence and reads the run table, because the claim has to happen in
// each verb that does work and a test of one says nothing about the others --
// which is what a mutation sweep reported: with backfill covered, removing the
// claim from `catchup` and from `prepare` changed nothing that was measured.

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

// TestInferenceClaimE2E walks the verbs and requires each one that works to
// take the run.
func TestInferenceClaimE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.TimescaleDB)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	adminDB, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()

	name := fmt.Sprintf("ptah_claim_cli_%d", time.Now().UnixNano())
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

	// prepare claims rather than writing the token as a literal. The observable
	// difference is the lease: a run created with `FencingToken: 1` and no
	// expiry holds a lease that never looks lapsed, and `status` prints it.
	runInference(c, ctx, "prepare",
		"--spec", specPath, "--db-url", dbName, "--run-id", cliRunID, "--worker", "worker-a")
	prepared, preparedExpiry := leaseOf(c, ctx, db, cliRunID)
	c.Assert(prepared > 0, qt.IsTrue)
	c.Assert(preparedExpiry.After(time.Now().UTC()), qt.IsTrue,
		qt.Commentf("the run holds a lease with no expiry, so it never looks lapsed"))

	runInference(c, ctx, "backfill",
		"--spec", specPath, "--db-url", dbName, "--run-id", cliRunID, "--batch-rows", "2")
	backfilled, _ := leaseOf(c, ctx, db, cliRunID)
	c.Assert(backfilled > prepared, qt.IsTrue,
		qt.Commentf("backfill did not take the run: the token stayed at %d", prepared))

	runInference(c, ctx, "catchup",
		"--spec", specPath, "--db-url", dbName, "--run-id", cliRunID, "--batch-rows", "2")
	caughtUp, _ := leaseOf(c, ctx, db, cliRunID)
	c.Assert(caughtUp > backfilled, qt.IsTrue,
		qt.Commentf("catchup did not take the run: the token stayed at %d", backfilled))
}

// leaseOf reads the run's fencing token and lease expiry.
func leaseOf(c *qt.C, ctx context.Context, db *sql.DB, runID string) (int64, time.Time) {
	c.Helper()
	var token int64
	var expires time.Time
	c.Assert(db.QueryRowContext(ctx,
		`SELECT fencing_token, lease_expires FROM ptah_embedding_run WHERE id = $1`, runID).
		Scan(&token, &expires), qt.IsNil)
	return token, expires.UTC()
}
