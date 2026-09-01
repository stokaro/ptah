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
	"go.5x5.cz/ptah/internal/embedpg"
)

// TestInferenceRollbackRefusalsE2E is stokaro/ptah#2647.
//
// Two states a rollback has to refuse, and it did neither.
//
// A rollback to the generation queries ALREADY read moved the pointer onto
// itself and recorded it as its own predecessor, which destroys the way back
// that row exists to hold. It printed "queries now read X, which replaced X"
// and exited 0.
//
// A rollback to a RETIRED generation died with a raw
// `column "..._generation" does not exist` before the decision layer ran. The
// refusal for it is written -- "the generation was retired, which is not
// something you come back from" -- and it was unreachable, because measuring
// freshness reads columns retirement has already dropped.
//
// Both assertions read the POINTER afterwards. A refusal that printed the right
// sentence and moved the pointer anyway would satisfy an assertion on stdout,
// and the pointer is the thing an operator's queries follow.
func TestInferenceRollbackRefusalsE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.TimescaleDB)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	adminDB, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()

	name := fmt.Sprintf("ptah_rbrefuse_%d", time.Now().UnixNano())
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
	const runID = "rbrefuse-1"

	runInference(c, ctx, "prepare", "--spec", specPath, "--db-url", dbName, "--run-id", runID)
	runInference(c, ctx, "backfill",
		"--spec", specPath, "--db-url", dbName, "--run-id", runID, "--batch-rows", "10")
	runInference(c, ctx, "catchup",
		"--spec", specPath, "--db-url", dbName, "--run-id", runID, "--batch-rows", "10")
	runInference(c, ctx, "verify", "--spec", specPath, "--db-url", dbName, "--run-id", runID)

	refused, err := runInferenceExpectingFailure(c, ctx, "cutover",
		"--spec", specPath, "--db-url", dbName, "--run-id", runID)
	c.Assert(err, qt.IsNotNil)
	runInference(c, ctx, "cutover",
		"--spec", specPath, "--db-url", dbName, "--run-id", runID,
		"--approve", planDigestFrom(c, refused), "--approver", "an operator")

	active := activePointerOf(c, ctx, db)
	assertARollbackToTheActiveGenerationIsRefused(c, ctx, db, specPath, dbName, active)
	assertARollbackToARetiredGenerationIsRefusedByName(c, ctx, db, endpoint.URL, dbName, specPath, active)
}

// assertARollbackToTheActiveGenerationIsRefused keeps a pointer from becoming
// its own predecessor.
//
// The previous generation a pointer names is the way back. Overwriting it with
// the active generation leaves a row that says "to go back, go here" pointing
// at where you already are, and the real way back is gone -- silently, at
// exit 0.
func assertARollbackToTheActiveGenerationIsRefused(
	c *qt.C, ctx context.Context, db *sql.DB, specPath, dbURL, active string,
) {
	c.Helper()
	before := previousPointerOf(c, ctx, db)

	output, err := runInferenceExpectingFailure(c, ctx, "rollback",
		"--spec", specPath, "--db-url", dbURL, "--to", active, "--window", "24h")

	c.Assert(err, qt.IsNotNil)
	c.Assert(output, qt.Contains, "queries already read that generation")
	c.Assert(activePointerOf(c, ctx, db), qt.Equals, active)
	c.Assert(previousPointerOf(c, ctx, db), qt.Equals, before)
}

// assertARollbackToARetiredGenerationIsRefusedByName is the designed refusal
// that a read failure used to preempt.
//
// The message matters as much as the exit code. `column does not exist` sends
// an operator to look at their schema; "the generation was retired" tells them
// what actually happened and that no amount of retrying will change it.
func assertARollbackToARetiredGenerationIsRefusedByName(
	c *qt.C, ctx context.Context, db *sql.DB, endpointURL, dbURL, specPath, active string,
) {
	c.Helper()
	// Registered through the fixture the rest of the suite uses, then put in
	// the state a retirement leaves: columns dropped, `retired_at` set. Writing
	// the row by hand instead is how a fixture comes to describe a state the
	// product cannot produce -- and the registry's own NOT NULL columns catch
	// that, which is what happened to the first draft of this one.
	// The specification recorded for it NAMES that column. RollbackState reads
	// the generation's own recorded document rather than the caller's, so a
	// fixture whose registry row and document disagree about the column
	// measures a column that is still there -- and the read this test is about
	// never happens. Measured: with the base specification recorded instead,
	// the mutant that restores the unconditional freshness read survives.
	retiredSpec := writeCLISpecWithMetric(c, endpointURL, "cosine", "embedding_gone")
	registerBareGenerationInColumn(c, ctx, db, retiredSpec, "a-retired-one", "embedding_gone")
	for _, suffix := range []string{
		"", embedpg.GenerationSuffix, embedpg.InputHashSuffix,
		embedpg.VersionSuffix, embedpg.StateSuffix,
	} {
		_, err := db.ExecContext(ctx, fmt.Sprintf(
			`ALTER TABLE articles DROP COLUMN IF EXISTS %q`, "embedding_gone"+suffix))
		c.Assert(err, qt.IsNil)
	}
	_, err := db.ExecContext(ctx,
		`UPDATE ptah_embedding_generation SET retired_at = now() WHERE identity = 'a-retired-one'`)
	c.Assert(err, qt.IsNil)

	output, err := runInferenceExpectingFailure(c, ctx, "rollback",
		"--spec", specPath, "--db-url", dbURL, "--to", "a-retired-one", "--window", "24h")

	c.Assert(err, qt.IsNotNil)
	c.Assert(output, qt.Contains, "was retired")
	c.Assert(output, qt.Not(qt.Contains), "does not exist")
	c.Assert(activePointerOf(c, ctx, db), qt.Equals, active)
}

// activePointerOf reads which generation queries currently read.
func activePointerOf(c *qt.C, ctx context.Context, db *sql.DB) string {
	c.Helper()
	var active string
	c.Assert(db.QueryRowContext(ctx,
		`SELECT active_generation FROM ptah_embedding_pointer`).Scan(&active), qt.IsNil)
	return active
}

// previousPointerOf reads the way back the pointer records, empty when none.
func previousPointerOf(c *qt.C, ctx context.Context, db *sql.DB) string {
	c.Helper()
	var previous sql.NullString
	c.Assert(db.QueryRowContext(ctx,
		`SELECT previous_generation FROM ptah_embedding_pointer`).Scan(&previous), qt.IsNil)
	return previous.String
}
