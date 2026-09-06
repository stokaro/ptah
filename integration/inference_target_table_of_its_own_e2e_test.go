//go:build integration

// A generation whose vectors live in a table of their own.
//
// stokaro/ptah#2736: the verification walk selected the target's four state
// columns -- `<column>_generation` and its siblings -- out of the SOURCE
// relation. That is the same relation whenever a specification embeds a table
// into itself, and a missing column whenever it does not, so `verify`, `status`
// and the cutover they gate all died with
// `column "embedding_generation" does not exist` on a specification that keeps
// its vectors out of the hot table. Nothing earlier in the lifecycle noticed:
// `prepare` creates the columns on the target, `backfill` writes them there,
// and `catchup` and `index` address the target as well.
//
// Both files that had target and source apart before this one -- the outbox
// ownership pair -- stop at `prepare`, so the whole of the walk was covered
// only by fixtures where the two names were equal and the wrong relation was
// the right one.

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

	"ptah.run/internal/dbtarget"
)

// TestInferenceVerifiesAGenerationWhoseVectorsLiveInTheirOwnTableE2E is the
// reported defect, driven the way it was reported.
//
// Through the CLI rather than through the reader, because every verb between
// the specification and the failure is part of the claim: the run that dies at
// `verify` is one that `prepare`, `backfill` and `catchup` all reported success
// for, and a test starting at the reader would not establish that the shape is
// one the product accepts.
func TestInferenceVerifiesAGenerationWhoseVectorsLiveInTheirOwnTableE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.TimescaleDB)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	db, dbName := freshTwoRelationDatabase(c, ctx, dbURL, "ptah_target_of_its_own")
	endpoint := httptest.NewServer(http.HandlerFunc(embeddingsHandler(c)))
	defer endpoint.Close()

	spec := writeCLISpecWithTargetTable(c, endpoint.URL, twoRelationTargetTable)
	runInference(c, ctx, "prepare", "--spec", spec, "--db-url", dbName, "--run-id", "own-table")
	runInference(c, ctx, "backfill", "--spec", spec, "--db-url", dbName, "--run-id", "own-table")
	runInference(c, ctx, "catchup", "--spec", spec, "--db-url", dbName, "--run-id", "own-table")

	// The vectors are in the other table, which is what the specification asked
	// for. Asserted before the verification so a fixture that quietly wrote
	// them into `articles` could not make the rest of this pass.
	c.Assert(vectorsIn(c, ctx, db, twoRelationTargetTable), qt.Equals, 3)

	verified := runInference(c, ctx, "verify", "--spec", spec, "--db-url", dbName, "--run-id", "own-table")
	c.Assert(verified, qt.Contains, "3 source rows, 3 target rows")
	c.Assert(verified, qt.Contains, "every deterministic layer passed")

	status := runInference(c, ctx, "status", "--spec", spec, "--db-url", dbName, "--run-id", "own-table")
	c.Assert(status, qt.Contains, "verified: true, cutover ready: true")

	// And the cutover the two of them gate, which is the verb an operator is
	// actually trying to reach.
	digest := planDigestOfRun(c, ctx, spec, dbName, "own-table")
	cutover := runInference(c, ctx, "cutover", "--spec", spec, "--db-url", dbName,
		"--run-id", "own-table", "--approve", digest, "--approver", "an operator")
	c.Assert(cutover, qt.Contains, "queries now read generation "+generationOfRun(c, ctx, db, "own-table"))
}

// TestInferenceATwoRelationVerificationSeesRowsOnlyOneSideHasE2E is the
// discrimination the repair needs beyond not failing.
//
// Two relations can disagree about which keys exist, which one relation cannot:
// a row is in both halves or in neither. So the walk became a join, and a join
// answers the question only if it is a FULL OUTER one. A left join from the
// source reads every source row and passes the test above, while a vector
// standing at a key the source no longer has -- a deleted row, a catch-up
// tombstone -- is a row it never visits, and the generation is reported clean
// by not looking.
//
// Both directions are asserted here, and the clean control first: a walk that
// reported every corpus would satisfy either half on its own.
func TestInferenceATwoRelationVerificationSeesRowsOnlyOneSideHasE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.TimescaleDB)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	db, dbName := freshTwoRelationDatabase(c, ctx, dbURL, "ptah_two_relation_sides")
	endpoint := httptest.NewServer(http.HandlerFunc(embeddingsHandler(c)))
	defer endpoint.Close()

	spec := writeCLISpecWithTargetTable(c, endpoint.URL, twoRelationTargetTable)
	runInference(c, ctx, "prepare", "--spec", spec, "--db-url", dbName, "--run-id", "sides")
	runInference(c, ctx, "backfill", "--spec", spec, "--db-url", dbName, "--run-id", "sides")
	runInference(c, ctx, "catchup", "--spec", spec, "--db-url", dbName, "--run-id", "sides")

	clean := runInference(c, ctx, "verify", "--spec", spec, "--db-url", dbName, "--run-id", "sides")
	c.Assert(clean, qt.Contains, "every deterministic layer passed")

	// A vector standing where the source has no row. It is written directly,
	// for the reason the out-of-scope fixture states: how it got there is not
	// the question, and a test reproducing one particular cause stops covering
	// the check the moment that cause is fixed.
	orphaned := twoRelationTargetRow(c, ctx, db, generationOfRun(c, ctx, db, "sides"), 99)
	c.Assert(orphaned, qt.Equals, int64(1))

	strayFound, err := runInferenceExpectingFailure(c, ctx,
		"verify", "--spec", spec, "--db-url", dbName, "--run-id", "sides")
	c.Assert(err, qt.IsNotNil, qt.Commentf("%s", strayFound))
	c.Assert(strayFound, qt.Contains, "3 source rows, 4 target rows")
	c.Assert(strayFound, qt.Contains, "1 target rows are outside the generation's source scope")
	c.Assert(strayFound, qt.Contains, "keys: 99")

	// The other direction, and the reason the source side cannot be dropped
	// either: a key the target has no row for at all is a coverage gap rather
	// than a row that quietly is not walked.
	c.Assert(deleteTwoRelationTargetRow(c, ctx, db, 99), qt.Equals, int64(1))
	c.Assert(deleteTwoRelationTargetRow(c, ctx, db, 2), qt.Equals, int64(1))

	gapFound, err := runInferenceExpectingFailure(c, ctx,
		"verify", "--spec", spec, "--db-url", dbName, "--run-id", "sides")
	c.Assert(err, qt.IsNotNil, qt.Commentf("%s", gapFound))
	c.Assert(gapFound, qt.Contains, "1 in-scope source rows have no vector in this generation")
	c.Assert(gapFound, qt.Contains, "keys: 2")
}

// twoRelationTargetTable is where these specifications keep their vectors.
const twoRelationTargetTable = "article_vectors"

// freshTwoRelationDatabase seeds a database whose source and target are two
// tables carrying the same key.
//
// The target rows exist before the run, because the write path is an UPDATE:
// `EnsureTarget` adds the vector and state columns to a relation, and a
// generation writes into rows that are already there. A fixture creating an
// empty target would make the backfill affect nothing and every count below
// would be zero for a reason that is not the one under test.
func freshTwoRelationDatabase(
	c *qt.C, ctx context.Context, dbURL, prefix string,
) (*sql.DB, string) {
	c.Helper()
	adminDB, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { adminDB.Close() })

	name := fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
	createE2EDatabase(c, ctx, adminDB, name)
	c.Cleanup(func() { dropE2EDatabase(c, context.Background(), adminDB, name) })

	dbName := replaceDatabaseName(c, dbURL, name)
	db, err := sql.Open("pgx", dbName)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { db.Close() })

	seedCLIArticles(c, ctx, db)
	for _, statement := range []string{
		`CREATE TABLE ` + twoRelationTargetTable + ` (id BIGINT PRIMARY KEY)`,
		`INSERT INTO ` + twoRelationTargetTable + ` (id) SELECT id FROM articles`,
	} {
		_, err := db.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}
	return db, dbName
}

// vectorsIn counts the rows of a relation carrying a vector.
func vectorsIn(c *qt.C, ctx context.Context, db *sql.DB, table string) int {
	c.Helper()
	var count int
	// #nosec G201 -- the relation is this file's own constant.
	c.Assert(db.QueryRowContext(ctx,
		"SELECT count(*) FROM "+table+" WHERE embedding IS NOT NULL").Scan(&count), qt.IsNil)
	return count
}

// twoRelationTargetRow puts a vector at a key the source table does not have.
func twoRelationTargetRow(
	c *qt.C, ctx context.Context, db *sql.DB, generation string, id int64,
) int64 {
	c.Helper()
	// #nosec G201 -- the relation is this file's own constant, and the columns
	// are the ones writeCLISpec names; every value is bound.
	result, err := db.ExecContext(ctx, `INSERT INTO `+twoRelationTargetTable+`
		(id, embedding, embedding_generation, embedding_input_hash,
		 embedding_source_version, embedding_state)
		VALUES ($1, $2, $3, 'x', '7', 'embedded')`, id, "[1,2,3,4]", generation)
	c.Assert(err, qt.IsNil)
	affected, err := result.RowsAffected()
	c.Assert(err, qt.IsNil)
	return affected
}

// deleteTwoRelationTargetRow removes one row of the target relation, leaving
// the source row it belonged to with nowhere for a vector to stand.
func deleteTwoRelationTargetRow(c *qt.C, ctx context.Context, db *sql.DB, id int64) int64 {
	c.Helper()
	// #nosec G201 -- the relation is this file's own constant.
	result, err := db.ExecContext(ctx,
		"DELETE FROM "+twoRelationTargetTable+" WHERE id = $1", id)
	c.Assert(err, qt.IsNil)
	affected, err := result.RowsAffected()
	c.Assert(err, qt.IsNil)
	return affected
}
