//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib"

	"go.5x5.cz/ptah/internal/dbtarget"
	"go.5x5.cz/ptah/internal/embedcatchup"
	"go.5x5.cz/ptah/internal/embedgen"
	"go.5x5.cz/ptah/internal/embedpg"
)

// TestEmbedPGOutboxE2E is the part of #2068 the epic itself flagged as needing
// measurement rather than design: the exact PostgreSQL sequence a transactional
// outbox has to follow.
//
// Two claims, and neither can be established anywhere but against a live
// server with a second connection open:
//
// The event and the source change are ONE transaction. A rolled-back change
// leaves no event, which is the property an application writing both sides
// cannot have -- it has two writes and a hope.
//
// And a read is bounded by the snapshot's xmin rather than by a sequence. A
// sequence is allocated when a row is inserted and a transaction becomes
// visible when it commits, so an event can be committed and visible while an
// EARLIER sequence is still in flight. A cursor advanced past the visible one
// steps over the other, and that change is then processed by nothing, ever.
//
// It runs against plain PostgreSQL: an outbox holds keys and versions, and no
// part of it needs pgvector.
func TestEmbedPGOutboxE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	adminDB, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()

	name := fmt.Sprintf("ptah_outbox_%d", time.Now().UnixNano())
	createE2EDatabase(c, ctx, adminDB, name)
	defer dropE2EDatabase(c, context.Background(), adminDB, name)

	url := replaceDatabaseName(c, dbURL, name)
	db, err := sql.Open("pgx", url)
	c.Assert(err, qt.IsNil)
	defer db.Close()

	spec := outboxSpec()
	_, err = db.ExecContext(ctx, `CREATE TABLE articles (
		id BIGINT PRIMARY KEY, title TEXT, body TEXT, updated_at TEXT NOT NULL)`)
	c.Assert(err, qt.IsNil)

	outbox, err := embedpg.NewOutbox(db, spec)
	c.Assert(err, qt.IsNil)

	assertNothingIsCapturedBeforeInstall(c, ctx, db, outbox)
	c.Assert(outbox.Install(ctx), qt.IsNil)
	// Twice, because installing is what happens at the start of a run and a run
	// can be restarted.
	c.Assert(outbox.Install(ctx), qt.IsNil)

	installed, err := outbox.Installed(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(installed, qt.IsTrue)

	assertEveryOperationIsCaptured(c, ctx, db, outbox)
	assertARolledBackChangeLeavesNoEvent(c, ctx, db, outbox)
	assertTheHorizonExcludesAnInFlightTransaction(c, ctx, db, url, outbox)
	assertAWriteToTheGenerationsOwnColumnsIsNotAChange(c, ctx, db, outbox)
	assertTheSequenceOrdersAndTheTransactionOnlyBounds(c, ctx, db, url, outbox)
	assertPruningRemovesOnlyWhatIsBehindTheCursor(c, ctx, outbox)
	assertAPageCutInsideATransactionResumesInsideIt(c, ctx, db, outbox)
	assertHalfAnInstallationIsNotInstalled(c, ctx, db, outbox)
}

// assertAPageCutInsideATransactionResumesInsideIt is stokaro/ptah#2628 measured
// against the server that produces it.
//
// One transaction writes more rows than the page holds. The read is bounded by
// the transaction and ORDERED by the sequence, so a cursor carrying only a
// transaction identity has to advance to the highest one the page held -- and
// every event of that transaction the page did not reach is then behind the
// cursor. Nothing reads it again: `Unprocessed` counts zero, the run reports
// caught up, and the rows keep whatever vector they had.
//
// The count that proves it is the UNION of two pages against the size of the
// transaction. Asserting only that the second page is non-empty passes with a
// cursor that went backwards, and asserting only its length passes with one
// that re-read the first page.
func assertAPageCutInsideATransactionResumesInsideIt(
	c *qt.C, ctx context.Context, db *sql.DB, outbox *embedpg.Outbox,
) {
	c.Helper()
	start, err := outbox.Horizon(ctx)
	c.Assert(err, qt.IsNil)

	transaction, err := db.BeginTx(ctx, nil)
	c.Assert(err, qt.IsNil)
	for id := 900; id < 906; id++ {
		_, err = transaction.ExecContext(ctx,
			`INSERT INTO articles (id, title, body, updated_at) VALUES ($1, 'Bulk', 'b', '1')`, id)
		c.Assert(err, qt.IsNil)
	}
	c.Assert(transaction.Commit(), qt.IsNil)

	from := embedcatchup.AtTransaction(start)
	first, _, err := outbox.Since(ctx, from, 4)
	c.Assert(err, qt.IsNil)
	c.Assert(first, qt.HasLen, 4)
	// One transaction wrote them all, so the page ends inside it: the highest
	// transaction identity the page holds is also the lowest.
	c.Assert(first[0].Transaction, qt.Equals, first[3].Transaction)

	next := embedcatchup.After(first[3])
	second, _, err := outbox.Since(ctx, next, 4)
	c.Assert(err, qt.IsNil)
	c.Assert(second, qt.HasLen, 2)
	c.Assert(second[0].Transaction, qt.Equals, first[0].Transaction)

	c.Assert(eventKeysOf(append(first, second...)), qt.DeepEquals,
		[]string{"900", "901", "902", "903", "904", "905"})

	// And the barrier agrees: after the second page nothing is owed, which is
	// the count a cutover reads.
	unprocessed, err := outbox.Unprocessed(ctx, embedcatchup.After(second[1]))
	c.Assert(err, qt.IsNil)
	c.Assert(unprocessed, qt.Equals, 0)
}

// eventKeysOf is the single-column key of each event, in order.
func eventKeysOf(events []embedcatchup.Event) []string {
	keys := make([]string, 0, len(events))
	for _, event := range events {
		keys = append(keys, event.Key[0])
	}
	return keys
}

// assertHalfAnInstallationIsNotInstalled is what makes the completion condition
// mean anything.
//
// The capture is two triggers, and one of them can be dropped -- by a migration
// that rebuilt the table, by somebody tidying up, by a restore from a dump
// taken before the outbox existed. An installation check that answered yes to
// half of it reports a run as capturing changes while every insert, or every
// update, goes unrecorded.
func assertHalfAnInstallationIsNotInstalled(
	c *qt.C, ctx context.Context, db *sql.DB, outbox *embedpg.Outbox,
) {
	c.Helper()
	names := outbox.TriggerNames()
	c.Assert(names, qt.HasLen, 2)

	for _, name := range names {
		_, err := db.ExecContext(ctx,
			fmt.Sprintf(`DROP TRIGGER %q ON articles`, name))
		c.Assert(err, qt.IsNil)

		installed, err := outbox.Installed(ctx)
		c.Assert(err, qt.IsNil)
		c.Assert(installed, qt.IsFalse, qt.Commentf("with %s dropped", name))

		c.Assert(outbox.Install(ctx), qt.IsNil)
		installed, err = outbox.Installed(ctx)
		c.Assert(err, qt.IsNil)
		c.Assert(installed, qt.IsTrue)
	}
}

// assertTheSequenceOrdersAndTheTransactionOnlyBounds separates when a row was
// WRITTEN from when its transaction first needed an identity.
//
// PostgreSQL assigns a transaction its identity at its first write ANYWHERE. So
// a transaction that touched an unrelated table, then waited, then updated this
// row holds an identity EARLIER than a transaction that reached this row and
// committed in the meantime -- and the update still happened after the insert.
//
// Ordering the outbox by transaction identity resolves that pair backwards and
// keeps the insert as the last word, which leaves the target holding a vector
// for text the row no longer has. The two orders agree in every ordinary case,
// which is why this fixture has to arrange the disagreement on purpose.
func assertTheSequenceOrdersAndTheTransactionOnlyBounds(
	c *qt.C, ctx context.Context, db *sql.DB, url string, outbox *embedpg.Outbox,
) {
	c.Helper()
	_, err := db.ExecContext(ctx, `CREATE TABLE unrelated (id BIGINT PRIMARY KEY)`)
	c.Assert(err, qt.IsNil)

	early, err := sql.Open("pgx", url)
	c.Assert(err, qt.IsNil)
	defer early.Close()

	// This transaction takes its identity now, from a table the outbox knows
	// nothing about.
	first, err := early.BeginTx(ctx, nil)
	c.Assert(err, qt.IsNil)
	_, err = first.ExecContext(ctx, `INSERT INTO unrelated (id) VALUES (1)`)
	c.Assert(err, qt.IsNil)

	// A later transaction writes to the source and commits, so its event has
	// the SMALLER sequence and the LARGER transaction identity.
	_, err = db.ExecContext(ctx,
		`INSERT INTO articles (id, title, body, updated_at) VALUES (7, 'Written', 'first', '1')`)
	c.Assert(err, qt.IsNil)

	// Now the older transaction reaches the source, and commits.
	_, err = first.ExecContext(ctx, `UPDATE articles SET title = 'Written second', updated_at = '2' WHERE id = 7`)
	c.Assert(err, qt.IsNil)
	c.Assert(first.Commit(), qt.IsNil)

	events, _, err := outbox.Since(ctx, embedcatchup.Cursor{}, 200)
	c.Assert(err, qt.IsNil)
	aboutSeven := eventsForKey(events, "7")
	c.Assert(aboutSeven, qt.HasLen, 2)
	// The disagreement is real: the second write carries the smaller identity.
	c.Assert(aboutSeven[1].Transaction < aboutSeven[0].Transaction, qt.IsTrue,
		qt.Commentf("the fixture did not produce the disagreement it exists for: %d then %d",
			aboutSeven[0].Transaction, aboutSeven[1].Transaction))
	c.Assert(aboutSeven[0].Operation, qt.Equals, embedcatchup.OperationInsert)
	c.Assert(aboutSeven[1].Operation, qt.Equals, embedcatchup.OperationUpdate)

	collapsed := embedcatchup.Collapse(aboutSeven)
	c.Assert(collapsed, qt.HasLen, 1)
	c.Assert(collapsed[0].Operation, qt.Equals, embedcatchup.OperationUpdate)
	c.Assert(collapsed[0].Version, qt.Equals, "2")
}

// eventsForKey picks out the events about one row, in the order they arrived.
func eventsForKey(events []embedcatchup.Event, key string) []embedcatchup.Event {
	var matching []embedcatchup.Event
	for _, event := range events {
		if len(event.Key) == 1 && event.Key[0] == key {
			matching = append(matching, event)
		}
	}
	return matching
}

// outboxSpec is the generation whose source the outbox watches.
func outboxSpec() embedgen.Spec {
	return embedgen.Spec{
		Source: embedgen.Source{
			Schema: "public", Table: "articles",
			KeyFields: []string{"id"}, InputFields: []string{"title", "body"},
			VersionStrategy: embedgen.VersionUpdatedAt, VersionField: "updated_at",
		},
		Preprocessing: embedgen.Preprocessing{Separator: "\n", NullPolicy: embedgen.NullAsEmpty},
		Model: embedgen.Model{
			Provider: "fake", Identifier: "fake-model", Revision: "1", ReportedDimension: 4,
		},
		Target: embedgen.Target{
			Schema: "public", Table: "articles", Column: "embedding",
			Representation: "vector", Metric: embedgen.MetricCosine,
		},
	}
}

// assertNothingIsCapturedBeforeInstall is the control the whole test needs.
//
// Without it, an outbox that captured nothing at all would satisfy the
// rolled-back-change assertion below and every other absence in this file.
func assertNothingIsCapturedBeforeInstall(
	c *qt.C, ctx context.Context, db *sql.DB, outbox *embedpg.Outbox,
) {
	c.Helper()
	installed, err := outbox.Installed(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(installed, qt.IsFalse)

	_, err = db.ExecContext(ctx,
		`INSERT INTO articles (id, title, body, updated_at) VALUES (99, 'before', 'install', '0')`)
	c.Assert(err, qt.IsNil)
	// And the outbox table does not exist yet, so there is nowhere for it to
	// have gone. Installing below is what creates it.
	_, err = db.ExecContext(ctx, `DELETE FROM articles WHERE id = 99`)
	c.Assert(err, qt.IsNil)
}

// assertEveryOperationIsCaptured walks the three event classes.
func assertEveryOperationIsCaptured(
	c *qt.C, ctx context.Context, db *sql.DB, outbox *embedpg.Outbox,
) {
	c.Helper()
	statements := []string{
		`INSERT INTO articles (id, title, body, updated_at) VALUES (1, 'First', 'a', '7')`,
		`UPDATE articles SET title = 'First again', updated_at = '8' WHERE id = 1`,
		`DELETE FROM articles WHERE id = 1`,
	}
	for _, statement := range statements {
		_, err := db.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}

	events, horizon, err := outbox.Since(ctx, embedcatchup.Cursor{}, 100)

	c.Assert(err, qt.IsNil)
	c.Assert(horizon > 0, qt.IsTrue)
	c.Assert(operationsOf(events), qt.DeepEquals, []embedcatchup.Operation{
		embedcatchup.OperationInsert, embedcatchup.OperationUpdate, embedcatchup.OperationDelete,
	})
	for _, event := range events {
		c.Assert(event.Key, qt.DeepEquals, []string{"1"})
	}
	// The version travels with the change rather than being read afterwards:
	// the update's event carries what the row said at the moment it changed,
	// and the delete carries what OLD held.
	c.Assert(versionsOf(events), qt.DeepEquals, []string{"7", "8", "8"})
	// And collapsing three events about one key leaves the last word.
	collapsed := embedcatchup.Collapse(events)
	c.Assert(collapsed, qt.HasLen, 1)
	c.Assert(collapsed[0].Operation, qt.Equals, embedcatchup.OperationDelete)
}

// assertAWriteToTheGenerationsOwnColumnsIsNotAChange is why the update trigger
// carries a WHEN clause.
//
// A generation's vector column lives ON the source table, so every vector Ptah
// writes is an update to it. An outbox that recorded those would hand catch-up
// an event for each vector it had just written, which it would reread,
// re-embed, and write again. Measured before the clause existed: the catch-up
// loop did not terminate.
//
// The control is the same statement touching a column the generation reads,
// which does produce an event -- otherwise a trigger that recorded nothing at
// all would satisfy this.
func assertAWriteToTheGenerationsOwnColumnsIsNotAChange(
	c *qt.C, ctx context.Context, db *sql.DB, outbox *embedpg.Outbox,
) {
	c.Helper()
	for _, statement := range []string{
		`ALTER TABLE articles ADD COLUMN embedding_state TEXT`,
		`ALTER TABLE articles ADD COLUMN unrelated_note TEXT`,
		`INSERT INTO articles (id, title, body, updated_at) VALUES (4, 'Fourth', 'text', '1')`,
	} {
		_, err := db.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}
	before, _, err := outbox.Since(ctx, embedcatchup.Cursor{}, 200)
	c.Assert(err, qt.IsNil)

	_, err = db.ExecContext(ctx, `UPDATE articles SET embedding_state = 'upsert' WHERE id = 4`)
	c.Assert(err, qt.IsNil)
	_, err = db.ExecContext(ctx, `UPDATE articles SET unrelated_note = 'a note' WHERE id = 4`)
	c.Assert(err, qt.IsNil)

	quiet, _, err := outbox.Since(ctx, embedcatchup.Cursor{}, 200)
	c.Assert(err, qt.IsNil)
	c.Assert(quiet, qt.HasLen, len(before))

	// The control: a column the generation reads.
	_, err = db.ExecContext(ctx, `UPDATE articles SET body = 'text again' WHERE id = 4`)
	c.Assert(err, qt.IsNil)
	loud, _, err := outbox.Since(ctx, embedcatchup.Cursor{}, 200)
	c.Assert(err, qt.IsNil)
	c.Assert(loud, qt.HasLen, len(before)+1)
	// And so is the version, even when the input is untouched: freshness
	// compares versions, so a row whose version moved is a row whose target
	// record is out of date.
	_, err = db.ExecContext(ctx, `UPDATE articles SET updated_at = '99' WHERE id = 4`)
	c.Assert(err, qt.IsNil)
	versioned, _, err := outbox.Since(ctx, embedcatchup.Cursor{}, 200)
	c.Assert(err, qt.IsNil)
	c.Assert(versioned, qt.HasLen, len(before)+2)

	_, err = db.ExecContext(ctx, `DELETE FROM articles WHERE id = 4`)
	c.Assert(err, qt.IsNil)
}

// assertARolledBackChangeLeavesNoEvent is the property a trigger has and an
// application write does not.
func assertARolledBackChangeLeavesNoEvent(
	c *qt.C, ctx context.Context, db *sql.DB, outbox *embedpg.Outbox,
) {
	c.Helper()
	before, _, err := outbox.Since(ctx, embedcatchup.Cursor{}, 100)
	c.Assert(err, qt.IsNil)

	transaction, err := db.BeginTx(ctx, nil)
	c.Assert(err, qt.IsNil)
	_, err = transaction.ExecContext(ctx,
		`INSERT INTO articles (id, title, body, updated_at) VALUES (2, 'Rolled', 'back', '9')`)
	c.Assert(err, qt.IsNil)
	c.Assert(transaction.Rollback(), qt.IsNil)

	after, _, err := outbox.Since(ctx, embedcatchup.Cursor{}, 100)

	c.Assert(err, qt.IsNil)
	c.Assert(after, qt.HasLen, len(before))
	// The control: the same statement committed does produce one.
	_, err = db.ExecContext(ctx,
		`INSERT INTO articles (id, title, body, updated_at) VALUES (2, 'Committed', 'yes', '9')`)
	c.Assert(err, qt.IsNil)
	committed, _, err := outbox.Since(ctx, embedcatchup.Cursor{}, 100)
	c.Assert(err, qt.IsNil)
	c.Assert(committed, qt.HasLen, len(before)+1)
}

// assertTheHorizonExcludesAnInFlightTransaction is the measurement the epic
// asked for.
//
// A second connection writes an event and holds its transaction open. That
// event is committed to nothing: it may appear or it may vanish. A reader that
// returned it would process a change that never happened; a reader that skipped
// past it would lose the change when it does commit. So it is not returned AND
// the horizon does not move past it -- and once the transaction commits, the
// same call returns it.
func assertTheHorizonExcludesAnInFlightTransaction(
	c *qt.C, ctx context.Context, db *sql.DB, url string, outbox *embedpg.Outbox,
) {
	c.Helper()
	settled, horizonBefore, err := outbox.Since(ctx, embedcatchup.Cursor{}, 100)
	c.Assert(err, qt.IsNil)

	// A separate pool, because a transaction held open on the shared one would
	// starve everything else in this test.
	other, err := sql.Open("pgx", url)
	c.Assert(err, qt.IsNil)
	defer other.Close()
	inFlight, err := other.BeginTx(ctx, nil)
	c.Assert(err, qt.IsNil)
	_, err = inFlight.ExecContext(ctx,
		`INSERT INTO articles (id, title, body, updated_at) VALUES (3, 'In', 'flight', '10')`)
	c.Assert(err, qt.IsNil)

	during, horizonDuring, err := outbox.Since(ctx, embedcatchup.Cursor{}, 100)
	c.Assert(err, qt.IsNil)
	c.Assert(during, qt.HasLen, len(settled))
	// The horizon is held back by the open transaction rather than racing past
	// it. That is what makes advancing a cursor to it safe.
	c.Assert(horizonDuring <= horizonBefore+1, qt.IsTrue,
		qt.Commentf("horizon moved from %d to %d with a transaction open", horizonBefore, horizonDuring))
	unprocessed, err := outbox.Unprocessed(ctx, embedcatchup.Cursor{})
	c.Assert(err, qt.IsNil)
	c.Assert(unprocessed, qt.Equals, len(settled))

	c.Assert(inFlight.Commit(), qt.IsNil)

	after, horizonAfter, err := outbox.Since(ctx, embedcatchup.Cursor{}, 100)
	c.Assert(err, qt.IsNil)
	c.Assert(after, qt.HasLen, len(settled)+1)
	c.Assert(horizonAfter > horizonDuring, qt.IsTrue,
		qt.Commentf("horizon %d did not move past the committed transaction", horizonAfter))
	c.Assert(after[len(after)-1].Key, qt.DeepEquals, []string{"3"})
	c.Assert(after[len(after)-1].Operation, qt.Equals, embedcatchup.OperationInsert)
}

// assertPruningRemovesOnlyWhatIsBehindTheCursor keeps a tombstone a paused run
// still owes.
//
// An outbox nobody prunes grows for as long as the application writes. One
// pruned by time rather than by what was processed drops the delete a stopped
// run has not seen, and the target then holds a vector for a row the source no
// longer has -- which verification reports as an unexpected row and no amount of
// re-running fixes.
func assertPruningRemovesOnlyWhatIsBehindTheCursor(
	c *qt.C, ctx context.Context, outbox *embedpg.Outbox,
) {
	c.Helper()
	events, _, err := outbox.Since(ctx, embedcatchup.Cursor{}, 100)
	c.Assert(err, qt.IsNil)
	c.Assert(len(events) >= 3, qt.IsTrue)
	cursor := events[1].Transaction

	removed, err := outbox.Prune(ctx, cursor)

	c.Assert(err, qt.IsNil)
	c.Assert(removed, qt.Equals, int64(1))
	remaining, _, err := outbox.Since(ctx, embedcatchup.Cursor{}, 100)
	c.Assert(err, qt.IsNil)
	c.Assert(remaining, qt.HasLen, len(events)-1)
	c.Assert(remaining[0].Transaction, qt.Equals, cursor)
}

// operationsOf lists what a set of events said happened.
func operationsOf(events []embedcatchup.Event) []embedcatchup.Operation {
	operations := make([]embedcatchup.Operation, 0, len(events))
	for _, event := range events {
		operations = append(operations, event.Operation)
	}
	return operations
}

// versionsOf lists the source versions the events carried.
func versionsOf(events []embedcatchup.Event) []string {
	versions := make([]string, 0, len(events))
	for _, event := range events {
		versions = append(versions, event.Version)
	}
	return versions
}
