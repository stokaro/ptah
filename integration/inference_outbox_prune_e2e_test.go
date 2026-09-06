//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the pgx driver for database/sql

	"ptah.run/internal/dbtarget"
	"ptah.run/internal/embedpg"
	"ptah.run/internal/embedspec"
)

// TestInferenceCatchUpPrunesTheOutboxE2E is stokaro/ptah#2690.
//
// The outbox records every write to the source table, and nothing removed an
// event once catch-up had processed it. The watermark moved and the rows
// stayed, so the companion table grew for the whole life of the migration and
// only `retire` ever emptied it -- on a source the application writes to, a
// table that outgrows the one it watches, while every event in it is already
// spent.
//
// The count is read from the table. `catchup` prints what it pruned, and a verb
// that printed "pruned 3" over a DELETE that matched nothing is precisely the
// failure worth catching here, so stdout is the one place this cannot be
// measured.
func TestInferenceCatchUpPrunesTheOutboxE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.TimescaleDB)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	adminDB, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()

	name := fmt.Sprintf("ptah_outbox_prune_%d", time.Now().UnixNano())
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

	prepareAndBackfill(c, ctx, specPath, dbName, pruneRunID)
	changeTheSourceThreeWays(c, ctx, db)

	// Before anything reads them. This assertion is not decoration: an outbox
	// that captured nothing satisfies the one below, so without this a product
	// that prunes nothing passes on an empty table.
	c.Assert(outboxRowsFrom(c, ctx, db, specPath), qt.Equals, 3)

	report := runInference(c, ctx, "catchup",
		"--spec", specPath, "--db-url", dbName, "--run-id", pruneRunID, "--batch-rows", "10")

	// The operator is told what went, and told the truth about it. The count
	// below is what proves the delete happened; this is the other half, and it
	// is the half a reader watching the table shrink actually sees. Neither
	// stands alone: a report over a DELETE that matched nothing passes this and
	// fails the next line, and a silent delete passes the next line and fails
	// this one.
	c.Assert(report, qt.Contains, "pruned 3 processed event(s) from the outbox")

	// Zero, not fewer than three. One generation is reading this source, it has
	// now passed every event, and an event no live reader still owes has no
	// reason to survive the pass that consumed it.
	c.Assert(outboxRowsFrom(c, ctx, db, specPath), qt.Equals, 0)
}

// TestInferenceCatchUpKeepsWhatASecondGenerationOwesUntilItIsAbandonedE2E is
// the half that makes the pruning above safe to do at all, plus stokaro/ptah#2723's
// non-destructive way to release that reader.
//
// An outbox belongs to the source TABLE -- [embedpg.Outbox.TableName] digests
// the source's qualified name, not the generation's -- so two generations over
// one table read one set of events. Deleting what the run that is catching up
// has passed therefore deletes what the other generation has not reached, and
// it does it silently: a deleted event fails the pending predicate, so the
// second generation's catch-up reads an empty range, records that it is caught
// up, and cuts over carrying vectors for text that was rewritten while it
// waited.
//
// Both directions are asserted, and only together do they say anything. The
// first says a pass may prune an older prefix while naming the run that still
// holds a newer suffix; the second says that suffix goes once the reader is
// abandoned, which separates a released floor from a prune that never deletes
// anything.
func TestInferenceCatchUpKeepsWhatASecondGenerationOwesUntilItIsAbandonedE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.TimescaleDB)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	adminDB, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()

	name := fmt.Sprintf("ptah_outbox_prune_shared_%d", time.Now().UnixNano())
	createE2EDatabase(c, ctx, adminDB, name)
	defer dropE2EDatabase(c, context.Background(), adminDB, name)

	dbName := replaceDatabaseName(c, dbURL, name)
	db, err := sql.Open("pgx", dbName)
	c.Assert(err, qt.IsNil)
	defer db.Close()
	seedCLIArticles(c, ctx, db)

	endpoint := httptest.NewServer(http.HandlerFunc(embeddingsHandler(c)))
	defer endpoint.Close()

	// Two generations over one source, differing the way a migration differs:
	// a new model revision into a new column. The source table is the same in
	// both, which is what makes the outbox shared.
	first := writeRevisionSpec(c, endpoint.URL, "1", "embedding_v1")
	second := writeRevisionSpec(c, endpoint.URL, "2", "embedding_v2")

	// The premise, stated where it is relied on rather than assumed. Every
	// count below reads one table through one specification and expects it to
	// answer for what the other generation did; two specifications resolving to
	// two outboxes would leave the middle assertion -- the one this whole test
	// exists for -- passing about nothing.
	c.Assert(outboxTableFor(c, db, first), qt.Equals, outboxTableFor(c, db, second))

	// Prepared and backfilled, and no further. Such a run records a snapshot
	// boundary and no catch-up watermark, which is the reader whose position
	// sits below every event made after it -- the state the floor has to
	// respect and the one a bound read off the catching-up run cannot see.
	prepareAndBackfill(c, ctx, first, dbName, "prune-first")
	prepareAndBackfill(c, ctx, second, dbName, "prune-second")

	// Bring both runs through one event first. That establishes a floor above
	// the snapshot boundary, so the later held-floor pass can both prune an
	// older prefix and retain a newer suffix -- the ordinary case in which the
	// holder diagnostic used to disappear behind the prune count.
	changeTheSource(c, ctx, db, sourceChanges[0])
	runInference(c, ctx, "catchup",
		"--spec", first, "--db-url", dbName, "--run-id", "prune-first", "--batch-rows", "10")
	runInference(c, ctx, "catchup",
		"--spec", second, "--db-url", dbName, "--run-id", "prune-second", "--batch-rows", "10")
	c.Assert(outboxRowsFrom(c, ctx, db, first), qt.Equals, 0)

	changeTheSource(c, ctx, db, sourceChanges[1])
	runInference(c, ctx, "catchup",
		"--spec", first, "--db-url", dbName, "--run-id", "prune-first", "--batch-rows", "10")
	changeTheSource(c, ctx, db, sourceChanges[2])

	held := runInference(c, ctx, "catchup",
		"--spec", second, "--db-url", dbName, "--run-id", "prune-second", "--batch-rows", "10")

	// Said out loud, not merely done. The diagnostic names the exact run and
	// generation holding the floor, so the operator has an identifier to inspect
	// or abandon instead of a generic explanation with no next action.
	c.Assert(held, qt.Contains, "pruned 1 processed event(s) from the outbox")
	c.Assert(held, qt.Contains,
		"the outbox keeps events this run has processed: floor ")
	c.Assert(held, qt.Contains, "for public.articles is held by run prune-first "+
		"(generation "+generationFromSpec(c, first)+")")

	// The older event was pruned, but the newest remains because the first run
	// has not reached it. Asked through the first specification, which resolves
	// to the same table: two specifications addressing one outbox is the whole
	// premise.
	c.Assert(outboxRowsFrom(c, ctx, db, first), qt.Equals, 1)

	abandoned := runInference(c, ctx, "abandon",
		"--db-url", dbName, "--run-id", "prune-first",
		"--reason", "the migration was superseded")
	c.Assert(abandoned, qt.Contains, "abandoned run prune-first")
	c.Assert(abandoned, qt.Contains, "the run no longer holds shared outbox events")
	repeated := runInference(c, ctx, "abandon",
		"--db-url", dbName, "--run-id", "prune-first",
		"--reason", "a retry after the response was lost")
	c.Assert(repeated, qt.Contains, "the migration was superseded")
	c.Assert(repeated, qt.Not(qt.Contains), "a retry after the response was lost")

	// Abandonment releases a reader, not its corpus. The old generation remains
	// inspectable and explicitly retireable, including vectors already written.
	var kept bool
	c.Assert(db.QueryRowContext(ctx,
		`SELECT embedding_v1 IS NOT NULL FROM articles WHERE id = 1`).Scan(&kept), qt.IsNil)
	c.Assert(kept, qt.IsTrue)
	status := runInference(c, ctx, "status",
		"--spec", first, "--db-url", dbName, "--run-id", "prune-first")
	c.Assert(status, qt.Contains, "abandoned: the migration was superseded")
	statusJSON := runInference(c, ctx, "status",
		"--spec", first, "--db-url", dbName, "--run-id", "prune-first", "--format", "json")
	var document struct {
		Readiness struct {
			Verified     bool     `json:"verified"`
			CutoverReady bool     `json:"cutover_ready"`
			Blockers     []string `json:"blockers"`
			Unmeasured   []string `json:"unmeasured"`
		} `json:"readiness"`
	}
	c.Assert(json.Unmarshal([]byte(statusJSON), &document), qt.IsNil, qt.Commentf("%s", statusJSON))
	c.Assert(document.Readiness.Verified, qt.IsFalse)
	c.Assert(document.Readiness.CutoverReady, qt.IsFalse)
	c.Assert(strings.Join(document.Readiness.Blockers, "\n"), qt.Contains,
		"run prune-first was abandoned and cannot be cut over")
	c.Assert(strings.Join(document.Readiness.Unmeasured, "\n"), qt.Contains,
		"every deterministic layer, because the run is terminal")
	gate, gateErr := runInferenceExpectingFailure(c, ctx, "status",
		"--spec", first, "--db-url", dbName, "--run-id", "prune-first", "--require-ready")
	c.Assert(gateErr, qt.IsNotNil, qt.Commentf("%s", gate))

	released := runInference(c, ctx, "catchup",
		"--spec", second, "--db-url", dbName, "--run-id", "prune-second", "--batch-rows", "10")

	// The retained event is reported by the next catch-up after abandonment.
	// That is the direction that proves the terminal status changed reader
	// membership rather than merely changing what `status` prints.
	c.Assert(released, qt.Contains, "pruned 1 processed event(s) from the outbox")

	// Every live reader has now passed it, so the floor rises and the final event
	// goes.
	c.Assert(outboxRowsFrom(c, ctx, db, second), qt.Equals, 0)
}

// pruneRunID names the run the single-generation test drives.
const pruneRunID = "prune-run"

// prepareAndBackfill walks one specification up to the point catch-up starts.
//
// It stops there on purpose. `prepare` installs the outbox and records the
// boundary the backfill covers up to, and `backfill` embeds the source as of
// that boundary; everything written afterwards is catch-up's, and is still
// sitting in the outbox when this returns.
func prepareAndBackfill(c *qt.C, ctx context.Context, specPath, dbURL, runID string) {
	c.Helper()
	runInference(c, ctx, "prepare", "--spec", specPath, "--db-url", dbURL, "--run-id", runID)
	runInference(c, ctx, "backfill",
		"--spec", specPath, "--db-url", dbURL, "--run-id", runID, "--batch-rows", "10")
}

// changeTheSourceThreeWays makes one change of each kind the outbox captures.
//
// One per trigger, and both triggers: the write trigger fires on the insert and
// on the delete, and the update trigger fires on the update because `title` and
// `updated_at` are columns the generation reads. Three operations rather than
// three updates, so a prune whose predicate reached only one of them leaves a
// count neither assertion here accepts.
func changeTheSourceThreeWays(c *qt.C, ctx context.Context, db *sql.DB) {
	c.Helper()
	for _, statement := range sourceChanges {
		changeTheSource(c, ctx, db, statement)
	}
}

var sourceChanges = []string{
	`UPDATE articles SET title = 'First rewritten', updated_at = '8' WHERE id = 1`,
	`INSERT INTO articles (id, title, body, updated_at)
		VALUES (4, 'Fourth', 'about invoices', '8')`,
	`DELETE FROM articles WHERE id = 3`,
}

func changeTheSource(c *qt.C, ctx context.Context, db *sql.DB, statement string) {
	c.Helper()
	_, err := db.ExecContext(ctx, statement)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
}

// outboxRowsFrom counts every event the outbox for a specification's source
// table holds.
//
// Every event, which is what separates it from [outboxRowsFor] beside it:
// pruning is a statement about the table, and a count filtered by row key
// would report zero for the events it did not think to ask about.
//
// The table name is derived rather than written down, because it carries a
// digest of the source's qualified name and cannot be spelled by hand. That is
// also the reason the second test may ask through either of its two
// specifications and get one answer: they differ in model revision and target
// column, and an outbox is named for neither.
func outboxRowsFrom(c *qt.C, ctx context.Context, db *sql.DB, specPath string) int {
	c.Helper()
	var count int
	// #nosec G201 -- the table name comes from the outbox itself.
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", outboxTableFor(c, db, specPath))
	c.Assert(db.QueryRowContext(ctx, query).Scan(&count), qt.IsNil)
	return count
}

// outboxTableFor names the outbox a specification's source resolves to.
//
// Separate from the count above because the name is an answer in its own
// right: the second test asserts that its two specifications produce one of
// them, which is the condition under which its counts mean anything.
func outboxTableFor(c *qt.C, db *sql.DB, specPath string) string {
	c.Helper()
	body, err := os.ReadFile(specPath)
	c.Assert(err, qt.IsNil)
	loaded, err := embedspec.Parse(body, specPath)
	c.Assert(err, qt.IsNil)
	outbox, err := embedpg.NewOutbox(db, loaded.Spec)
	c.Assert(err, qt.IsNil)
	return outbox.TableName()
}

// generationFromSpec returns the identity printed in the floor diagnostic.
func generationFromSpec(c *qt.C, specPath string) string {
	c.Helper()
	body, err := os.ReadFile(specPath)
	c.Assert(err, qt.IsNil)
	loaded, err := embedspec.Parse(body, specPath)
	c.Assert(err, qt.IsNil)
	return loaded.Spec.Identity().Digest
}
