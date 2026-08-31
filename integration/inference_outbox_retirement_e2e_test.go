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

// TestInferenceRetirementRemovesTheOutboxE2E is stokaro/ptah#2649 finding 1.
//
// `retire` dropped a generation's index and columns and left the outbox alone,
// so after retirement both triggers went on firing on the operator's table for
// every write, the capture function stayed, and `ptah_embedding_outbox_articles`
// grew with nothing that would ever read or trim it. The guide's own section
// headed "When the triggers go away" says `retire` removes them.
//
// It runs live because the subject is catalog state on the operator's table:
// what is asserted is `pg_trigger`, `to_regproc` and `to_regclass`, not what
// the verb said about itself.
func TestInferenceRetirementRemovesTheOutboxE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.TimescaleDB)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	adminDB, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()

	name := fmt.Sprintf("ptah_outbox_retire_%d", time.Now().UnixNano())
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

	// The fixture has to start with the outbox installed, or every assertion
	// below is about an absence that was always there.
	c.Assert(outboxTriggerCount(c, ctx, db), qt.Equals, 2)
	c.Assert(outboxObjectsExist(c, ctx, db), qt.IsTrue)

	generation := activeGenerationFrom(c, ctx, specPath, dbName)
	assertASharedOutboxSurvivesOneRetirement(c, ctx, db, specPath, dbName)
	assertTheLastRetirementTakesTheOutbox(c, ctx, db, specPath, dbName, generation)
	assertAModeWithNoOutboxClaimsNothing(c, ctx, db, endpoint.URL, dbName)
}

// assertAModeWithNoOutboxClaimsNothing covers the branch the two assertions
// above cannot reach.
//
// `immutable` installs no outbox, so retirement has nothing to remove — and the
// thing to get wrong is not the removal but the SENTENCE. A guard that fell
// through here would print "the outbox is gone" about a table Ptah never put a
// trigger on, which is the same class of false report this whole change is
// about.
func assertAModeWithNoOutboxClaimsNothing(
	c *qt.C, ctx context.Context, db *sql.DB, endpoint, dbURL string,
) {
	c.Helper()
	frozen := writeCLISpecWithMode(c, endpoint, "immutable")
	// A column of its own, so the default retirement destroys something and
	// destroys nothing the live generation is using. A retirement whose plan
	// destroys nothing is refused, correctly, as a record of something that did
	// not happen.
	registerBareGenerationInColumn(c, ctx, db, frozen, "an-immutable-generation", "embedding_immutable")
	digest := retirementDigestOf(c, ctx, frozen, dbURL, "an-immutable-generation")

	output := runInference(c, ctx, "retire",
		"--spec", frozen, "--db-url", dbURL, "--generation", "an-immutable-generation",
		"--approve", digest, "--approver", "an operator")

	c.Assert(output, qt.Contains, "is gone, with")
	c.Assert(output, qt.Not(qt.Contains), "the outbox is gone")
	c.Assert(output, qt.Not(qt.Contains), "the outbox stays")
}

// assertASharedOutboxSurvivesOneRetirement is the control, and it is the reason
// retirement cannot simply always uninstall.
//
// An outbox belongs to the source TABLE, and two generations over one table
// share its changes. Retiring one while another still reads it must leave the
// capture in place, or the survivor's catch-up stops seeing writes.
func assertASharedOutboxSurvivesOneRetirement(
	c *qt.C, ctx context.Context, db *sql.DB, specPath, dbURL string,
) {
	c.Helper()
	registerBareGenerationInColumn(c, ctx, db, specPath, "a-second-generation", "embedding_second")
	// Its own column, for the reason registerBareGenerationInColumn states: a
	// retirement sharing the live generation's column can only run with
	// --drop-column=false, and then it destroys nothing and is refused.
	registerBareGenerationInColumn(c, ctx, db, specPath, "the-one-being-retired", "embedding_retired")

	// Read under the flags the run uses -- the default here. The plan digest
	// binds DropsColumn, so a digest read one way and approved the other is
	// refused, correctly, and reads as a test failure rather than as the
	// mismatch it is.
	digest := retirementDigestOf(c, ctx, specPath, dbURL, "the-one-being-retired")
	output := runInference(c, ctx, "retire",
		"--spec", specPath, "--db-url", dbURL, "--generation", "the-one-being-retired",
		"--approve", digest, "--approver", "an operator")

	c.Assert(output, qt.Contains, "the outbox stays")
	c.Assert(outboxTriggerCount(c, ctx, db), qt.Equals, 2)
	c.Assert(outboxObjectsExist(c, ctx, db), qt.IsTrue)
}

// assertTheLastRetirementTakesTheOutbox is the finding itself.
func assertTheLastRetirementTakesTheOutbox(
	c *qt.C, ctx context.Context, db *sql.DB, specPath, dbURL, generation string,
) {
	c.Helper()
	// Retire the second one first, so the generation under test is the last.
	second := retirementDigestOf(c, ctx, specPath, dbURL, "a-second-generation")
	runInference(c, ctx, "retire",
		"--spec", specPath, "--db-url", dbURL, "--generation", "a-second-generation",
		"--approve", second, "--approver", "an operator")

	// The live generation, and the last one, so its column goes with it. Read
	// and run under the same flags: the plan digest binds DropsColumn, and a
	// digest read one way and approved the other is refused.
	digest := retirementDigestOf(c, ctx, specPath, dbURL, generation)
	output := runInference(c, ctx, "retire",
		"--spec", specPath, "--db-url", dbURL, "--generation", generation,
		"--approve", digest, "--approver", "an operator")

	c.Assert(output, qt.Contains, "the outbox is gone")
	c.Assert(outboxTriggerCount(c, ctx, db), qt.Equals, 0)
	c.Assert(outboxObjectsExist(c, ctx, db), qt.IsFalse)

	// And the operator's table takes a write without recording anything, which
	// is the cost the finding measured: a trigger firing forever on a table
	// Ptah is finished with.
	_, err := db.ExecContext(ctx,
		`UPDATE articles SET title = 'after retirement', updated_at = '9' WHERE id = 1`)
	c.Assert(err, qt.IsNil)
	c.Assert(outboxObjectsExist(c, ctx, db), qt.IsFalse)
}

// outboxTriggerCount counts Ptah's triggers on the source table.
func outboxTriggerCount(c *qt.C, ctx context.Context, db *sql.DB) int {
	c.Helper()
	var count int
	c.Assert(db.QueryRowContext(ctx,
		`SELECT count(*) FROM pg_trigger t JOIN pg_class r ON r.oid = t.tgrelid
		  WHERE r.relname = 'articles' AND NOT t.tgisinternal
		    AND t.tgname LIKE 'ptah_embedding_outbox%'`).Scan(&count), qt.IsNil)
	return count
}

// outboxObjectsExist reports whether the capture function or the event table is
// still there.
//
// Matched by PREFIX rather than by a spelled-out name: the outbox table carries
// a digest of the source schema and table, so `ptah_embedding_outbox_articles`
// is not what it is called. A hard-coded name found nothing and made the
// fixture assertion fail before the product was ever exercised — which is what
// that assertion is for.
func outboxObjectsExist(c *qt.C, ctx context.Context, db *sql.DB) bool {
	c.Helper()
	var count int
	c.Assert(db.QueryRowContext(ctx,
		`SELECT (SELECT count(*) FROM pg_tables
		           WHERE tablename LIKE 'ptah_embedding_outbox_articles%')
		      + (SELECT count(*) FROM pg_proc
		           WHERE proname LIKE 'ptah_embedding_outbox_articles%_capture')`).
		Scan(&count), qt.IsNil)
	return count > 0
}
