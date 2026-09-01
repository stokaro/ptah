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

// TestInferenceSnapshotCompletenessIsMeasuredE2E is stokaro/ptah#2649 finding 3.
//
// "The backfill has not reached the end of its snapshot" was decided by a phase
// reading. The first spelling, `Phase != backfilling`, was true for every phase
// BEFORE the backfill as well as after it. The second, `Reached(backfilled)`,
// closed that direction and left the one a high-water mark cannot express: a run
// whose backfill once finished and was then given more to do still read as
// complete, so the whole consistency layer went quiet for a run that had work
// left.
//
// The measurement is the source itself, through the same scan the backfill
// resumes with, so this test moves the source between assertions and requires
// the answer to follow it in both directions.
func TestInferenceSnapshotCompletenessIsMeasuredE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.TimescaleDB)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	db, dbName := freshSnapshotDatabase(c, ctx, dbURL, "ptah_snapshot")
	seedCLIArticles(c, ctx, db)
	endpoint := httptest.NewServer(http.HandlerFunc(embeddingsHandler(c)))
	defer endpoint.Close()

	specPath := writeCLISpec(c, endpoint.URL)
	runInference(c, ctx, "prepare", "--spec", specPath, "--db-url", dbName, "--run-id", cliRunID)
	runInference(c, ctx, "backfill",
		"--spec", specPath, "--db-url", dbName, "--run-id", cliRunID, "--batch-rows", "10")
	runInference(c, ctx, "catchup",
		"--spec", specPath, "--db-url", dbName, "--run-id", cliRunID, "--batch-rows", "10")

	// The control. A finished backfill says nothing, and a fix that always
	// reported the finding would fail here.
	c.Assert(runInference(c, ctx, "verify",
		"--spec", specPath, "--db-url", dbName, "--run-id", cliRunID),
		qt.Not(qt.Contains), "has not reached the end of its snapshot")

	// The finding itself: rows past the cursor the backfill has not walked.
	_, err := db.ExecContext(ctx,
		`INSERT INTO articles (id, title, body, updated_at) VALUES
			(4, 'Fourth', 'about shipping', '8'), (5, 'Fifth', 'about returns', '8')`)
	c.Assert(err, qt.IsNil)

	output, err := runInferenceExpectingFailure(c, ctx, "verify",
		"--spec", specPath, "--db-url", dbName, "--run-id", cliRunID)
	c.Assert(err, qt.IsNotNil, qt.Commentf("%s", output))
	c.Assert(output, qt.Contains,
		"[consistency/blocking] the backfill has not reached the end of its snapshot")

	// And it goes away when the walk catches up, which is the direction that
	// proves the answer is measured rather than latched. A phase reading could
	// produce the sentence above only by never having reached `backfilled`, and
	// then it could not take it back.
	runInference(c, ctx, "backfill",
		"--spec", specPath, "--db-url", dbName, "--run-id", cliRunID, "--batch-rows", "10")
	runInference(c, ctx, "catchup",
		"--spec", specPath, "--db-url", dbName, "--run-id", cliRunID, "--batch-rows", "10")
	c.Assert(runInference(c, ctx, "verify",
		"--spec", specPath, "--db-url", dbName, "--run-id", cliRunID),
		qt.Not(qt.Contains), "has not reached the end of its snapshot")
}

// TestInferenceTwoRunsInOneStateAgreeE2E is the control the phase reading could
// not pass.
//
// Two runs over one source, in the same state -- no cursor, nothing scanned,
// the same rows uncovered -- were told different things, because one of them
// had once passed through a backfill that returned. A verification's answer
// about the source must come from the source.
func TestInferenceTwoRunsInOneStateAgreeE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.TimescaleDB)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	db, dbName := freshSnapshotDatabase(c, ctx, dbURL, "ptah_snapshot_pair")
	seedCLIArticles(c, ctx, db)
	endpoint := httptest.NewServer(http.HandlerFunc(embeddingsHandler(c)))
	defer endpoint.Close()

	specPath := writeCLISpecWithMode(c, endpoint.URL, "immutable")
	runInference(c, ctx, "prepare", "--spec", specPath, "--db-url", dbName, "--run-id", "walked")

	// The first run walks an empty source, so its backfill reaches the end
	// having done nothing, and the phase advances.
	_, err := db.ExecContext(ctx, `DELETE FROM articles`)
	c.Assert(err, qt.IsNil)
	runInference(c, ctx, "backfill",
		"--spec", specPath, "--db-url", dbName, "--run-id", "walked", "--batch-rows", "10")

	// The source comes back, and a second run is prepared over it. Neither run
	// has covered a row, and neither has a cursor.
	_, err = db.ExecContext(ctx,
		`INSERT INTO articles (id, title, body, updated_at) VALUES
			(1, 'First', 'about pricing', '7'), (2, 'Second', 'about support', '7')`)
	c.Assert(err, qt.IsNil)
	runInference(c, ctx, "prepare", "--spec", specPath, "--db-url", dbName, "--run-id", "unwalked")

	walked, walkedErr := runInferenceExpectingFailure(c, ctx, "verify",
		"--spec", specPath, "--db-url", dbName, "--run-id", "walked")
	unwalked, unwalkedErr := runInferenceExpectingFailure(c, ctx, "verify",
		"--spec", specPath, "--db-url", dbName, "--run-id", "unwalked")

	c.Assert(walkedErr, qt.IsNotNil, qt.Commentf("%s", walked))
	c.Assert(unwalkedErr, qt.IsNotNil, qt.Commentf("%s", unwalked))
	c.Assert(walked, qt.Contains, "the backfill has not reached the end of its snapshot")
	c.Assert(unwalked, qt.Contains, "the backfill has not reached the end of its snapshot")
}

// freshSnapshotDatabase makes a database of its own and hands back a connection
// and the URL that reaches it.
func freshSnapshotDatabase(
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
