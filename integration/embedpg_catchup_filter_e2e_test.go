//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the pgx driver for database/sql

	"ptah.run/internal/dbtarget"
	"ptah.run/internal/embedengine"
	"ptah.run/internal/embedgen"
	"ptah.run/internal/embedpg"
	"ptah.run/internal/embedrun"
)

// TestEmbedPGCatchUpHonorsTheSourceFilterE2E is stokaro/ptah#2638.
//
// The backfill's scan applied `source.filter`; the reread catch-up does per
// changed key did not. An insert outside the filter still writes an outbox
// event, so catch-up re-read that row without the filter, sent its text to the
// provider, and wrote the generation's vector onto it -- text the operator had
// excluded leaving the database, and a row the operator had excluded joining
// the corpus the index covers.
//
// It runs against a live server because the defect is in a query. A test that
// asserted the rendered SQL would agree with a filter clause PostgreSQL refuses,
// and the thing worth knowing is what came back.
func TestEmbedPGCatchUpHonorsTheSourceFilterE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.TimescaleDB)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	adminDB, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()

	name := fmt.Sprintf("ptah_catchup_filter_%d", time.Now().UnixNano())
	createE2EDatabase(c, ctx, adminDB, name)
	defer dropE2EDatabase(c, context.Background(), adminDB, name)

	db, err := sql.Open("pgx", replaceDatabaseName(c, dbURL, name))
	c.Assert(err, qt.IsNil)
	defer db.Close()

	spec := filteredLiveSpec()
	seedFilteredArticles(c, ctx, db, spec)
	engine, outbox, source := aFilteredCatchUpEngine(c, ctx, db, spec)

	// The pass's own progress, which is what Backfill returns beside the run
	// since stokaro/ptah#2645. Two of the four rows are published, and a
	// backfill that embedded more would make every assertion below meaningless.
	_, backfilled, err := engine.Backfill(ctx, filterRunID)
	c.Assert(err, qt.IsNil)
	c.Assert(backfilled.RowsEmbedded, qt.Equals, int64(2))

	changeTheSourceOutsideTheFilter(c, ctx, db)

	_, _, err = engine.CatchUp(ctx, filterRunID, outbox, source)
	c.Assert(err, qt.IsNil)

	assertOnlyPublishedRowsCarryVectors(c, ctx, db, spec)
}

// filterRunID names the run every step of this test drives.
const filterRunID = "catchup-filter-run"

// changeTheSourceOutsideTheFilter makes the two writes the finding measured.
//
// Both produce an outbox event -- an insert always does, and an update to a
// watched column does -- so both reach the reread. Neither may reach the
// provider.
func changeTheSourceOutsideTheFilter(c *qt.C, ctx context.Context, db *sql.DB) {
	c.Helper()
	statements := []string{
		// Inserted out of scope after the backfill had passed.
		`INSERT INTO articles (id, title, body, published, updated_at)
			VALUES (5, 'Draft', 'unpublished salary review', false, '8')`,
		// A row that was already out of scope, edited in a watched column.
		`UPDATE articles SET body = 'still unpublished', updated_at = '8' WHERE id = 4`,
		// The control: an in-scope row changed in the same catch-up, so a fix
		// that filtered everything out would fail here rather than pass.
		`UPDATE articles SET title = 'First rewritten', updated_at = '8' WHERE id = 1`,
	}
	for _, statement := range statements {
		_, err := db.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}
}

// assertOnlyPublishedRowsCarryVectors reads the corpus back through the server.
//
// The state column is asserted as well as the vector: a row the reread no
// longer finds is tombstoned rather than left alone, which is the same answer
// the reread already gave for a deleted row and the right one for a row that
// stopped qualifying.
func assertOnlyPublishedRowsCarryVectors(
	c *qt.C, ctx context.Context, db *sql.DB, spec embedgen.Spec,
) {
	c.Helper()
	rows, err := db.QueryContext(ctx, fmt.Sprintf(
		`SELECT id, published, %s IS NOT NULL, coalesce(%s, '')
		   FROM articles ORDER BY id`,
		spec.Target.Column, spec.Target.Column+embedpg.StateSuffix))
	c.Assert(err, qt.IsNil)
	defer rows.Close()

	var got []string
	for rows.Next() {
		var (
			id        int64
			published bool
			hasVector bool
			state     string
		)
		c.Assert(rows.Scan(&id, &published, &hasVector, &state), qt.IsNil)
		got = append(got, fmt.Sprintf("%d published=%t vector=%t state=%q",
			id, published, hasVector, state))
	}
	c.Assert(rows.Err(), qt.IsNil)

	c.Assert(got, qt.DeepEquals, []string{
		// In scope and changed during the catch-up: re-embedded.
		`1 published=true vector=true state="upsert"`,
		// In scope and untouched: the backfill's vector stands.
		`2 published=true vector=true state="upsert"`,
		// Out of scope for the whole run and never written about.
		`3 published=false vector=false state=""`,
		// Out of scope, edited in a watched column. The reread does not find
		// it, so it is tombstoned rather than embedded.
		`4 published=false vector=false state="tombstone"`,
		// Inserted out of scope after the backfill. Same answer.
		`5 published=false vector=false state="tombstone"`,
	})
}

// filteredLiveSpec is liveSpec with a filter and the column it reads.
func filteredLiveSpec() embedgen.Spec {
	spec := liveSpec()
	spec.Source.Filter = `published`
	return spec
}

// seedFilteredArticles seeds four rows, two of them outside the filter.
func seedFilteredArticles(c *qt.C, ctx context.Context, db *sql.DB, spec embedgen.Spec) {
	c.Helper()
	statements := []string{
		`CREATE EXTENSION IF NOT EXISTS vector`,
		`CREATE TABLE articles (
			id BIGINT PRIMARY KEY,
			title TEXT,
			body TEXT,
			published BOOLEAN NOT NULL,
			updated_at TEXT NOT NULL
		)`,
		fmt.Sprintf(`ALTER TABLE articles
			ADD COLUMN %s vector(%d),
			ADD COLUMN %s TEXT,
			ADD COLUMN %s TEXT,
			ADD COLUMN %s TEXT,
			ADD COLUMN %s TEXT`,
			spec.Target.Column, spec.Model.ReportedDimension,
			spec.Target.Column+embedpg.GenerationSuffix,
			spec.Target.Column+embedpg.InputHashSuffix,
			spec.Target.Column+embedpg.VersionSuffix,
			spec.Target.Column+embedpg.StateSuffix),
		`INSERT INTO articles (id, title, body, published, updated_at) VALUES
			(1, 'First',  'about pricing', true,  '7'),
			(2, 'Second', 'about support', true,  '7'),
			(3, 'Third',  'about drafts',  false, '7'),
			(4, 'Fourth', 'about billing', false, '7')`,
	}
	for _, statement := range statements {
		_, err := db.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}
}

// aFilteredCatchUpEngine wires the run this test drives.
func aFilteredCatchUpEngine(
	c *qt.C, ctx context.Context, db *sql.DB, spec embedgen.Spec,
) (*embedengine.Engine, *embedpg.Outbox, *embedpg.Source) {
	c.Helper()
	store := embedpg.NewStore(db)
	c.Assert(store.EnsureSchema(ctx), qt.IsNil)
	registerCatchUpGeneration(c, ctx, store, spec)
	source, err := embedpg.NewSource(db, spec)
	c.Assert(err, qt.IsNil)
	target, err := embedpg.NewTarget(db, spec)
	c.Assert(err, qt.IsNil)
	outbox, err := embedpg.NewOutbox(db, spec)
	c.Assert(err, qt.IsNil)

	c.Assert(outbox.InstallForIsolatedSource(ctx), qt.IsNil)
	boundary, err := outbox.Horizon(ctx)
	c.Assert(err, qt.IsNil)

	c.Assert(store.CreateRun(ctx, embedrun.Run{
		ID: filterRunID, SpecDigest: "spec-1", GenerationIdentity: spec.Identity().Digest,
		Environment: "test", Source: embedpg.SourceIdentity(spec.Source.Schema, spec.Source.Table),
		Target:          "public.articles.embedding",
		ProviderProfile: "fake", PtahVersion: "test", PolicyDigest: "policy",
		// Backfilled, because that is what catch-up runs after. Created at
		// `backfilling` these fixtures asked the engine to serve a run whose
		// snapshot walk had not finished, which it did (stokaro/ptah#2737).
		Phase: embedrun.PhaseBackfilled, Status: embedrun.StatusRunning,
		LeaseOwner: "worker-a", FencingToken: 1,
		SnapshotWatermark: strconv.FormatUint(boundary, 10),
		CreatedAt:         time.Now().UTC(), UpdatedAt: time.Now().UTC(),
	}), qt.IsNil)

	return &embedengine.Engine{
		Spec: spec, Source: source, Provider: &liveProvider{dimension: 4},
		Target: target, Store: store,
		Bounds: embedrun.BatchBounds{MaxRows: 2, MaxInputs: 2}, Worker: "worker-a",
	}, outbox, source
}

// TestEmbedPGAFilterColumnIsWatchedE2E covers stokaro/ptah#2659.
//
// The update trigger watched the key, the input fields and the version, and a
// row that left the filter's scope through any other column produced no event
// at all -- so catch-up never learned, and the row kept a vector for a
// generation whose specification excludes it.
//
// The assertion is on the OUTBOX rather than on the vector, deliberately. What
// this fixes is whether the change is observed; what catch-up then does with
// the event is stokaro/ptah#2638's question and is asserted by the test above.
func TestEmbedPGAFilterColumnIsWatchedE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.TimescaleDB)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	adminDB, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()

	name := fmt.Sprintf("ptah_filter_column_%d", time.Now().UnixNano())
	createE2EDatabase(c, ctx, adminDB, name)
	defer dropE2EDatabase(c, context.Background(), adminDB, name)

	db, err := sql.Open("pgx", replaceDatabaseName(c, dbURL, name))
	c.Assert(err, qt.IsNil)
	defer db.Close()

	spec := filteredLiveSpec()
	seedFilteredArticles(c, ctx, db, spec)
	outbox, err := embedpg.NewOutbox(db, spec)
	c.Assert(err, qt.IsNil)
	c.Assert(outbox.InstallForIsolatedSource(ctx), qt.IsNil)

	// A published row leaves the filter's scope. Nothing else about it changes:
	// no key, no input field, no version -- so before this, no event.
	_, err = db.ExecContext(ctx, `UPDATE articles SET published = false WHERE id = 1`)
	c.Assert(err, qt.IsNil)

	c.Assert(outboxRowsFor(c, ctx, db, spec, "1"), qt.Equals, 1)

	// The control: a column named by neither the filter nor the vector still
	// produces nothing. A trigger widened to WHEN TRUE would satisfy the
	// assertion above and record every write the application makes.
	_, err = db.ExecContext(ctx, `ALTER TABLE articles ADD COLUMN note TEXT`)
	c.Assert(err, qt.IsNil)
	_, err = db.ExecContext(ctx, `UPDATE articles SET note = 'irrelevant' WHERE id = 2`)
	c.Assert(err, qt.IsNil)

	c.Assert(outboxRowsFor(c, ctx, db, spec, "2"), qt.Equals, 0)
}

// TestEmbedPGAFilterProbeIgnoresAnIdenticallyNamedConstraintE2E is the review
// finding on stokaro/ptah#2698.
//
// The probe reads its answer out of `pg_constraint`, and a constraint name is
// unique only within a schema. Asked by name alone, the query returns the
// columns of every constraint in the database that happens to share the name --
// so an unrelated table carrying one adds ITS columns to what the update
// trigger watches. Measured on PostgreSQL 17 before the fix: two rows came
// back, `title` from the probe and `other` from the decoy.
//
// The cost is not cosmetic. A column the filter never reads becomes a column
// every application write to it fires the trigger on, and if the collision
// named the generation's own vector column, Ptah's writes would produce events
// about Ptah's writes -- the non-terminating catch-up loop ADR 0014 section 5
// exists to prevent.
//
// The decoy's constraint name mirrors the one the probe uses. That coupling is
// the point and also its limit: renaming the probe's constraint leaves this
// fixture green while testing nothing, and this comment is what says so.
func TestEmbedPGAFilterProbeIgnoresAnIdenticallyNamedConstraintE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.TimescaleDB)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	adminDB, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()

	name := fmt.Sprintf("ptah_filter_decoy_%d", time.Now().UnixNano())
	createE2EDatabase(c, ctx, adminDB, name)
	defer dropE2EDatabase(c, context.Background(), adminDB, name)

	db, err := sql.Open("pgx", replaceDatabaseName(c, dbURL, name))
	c.Assert(err, qt.IsNil)
	defer db.Close()

	spec := filteredLiveSpec()
	seedFilteredArticles(c, ctx, db, spec)

	// The column has to exist on `articles` as well, or the widened trigger
	// would fail to render and the defect would announce itself. A column that
	// exists on both is the quiet version: the trigger installs, watches one
	// column too many, and nothing says so until the events arrive.
	seedDecoyConstraint(c, ctx, db)

	outbox, err := embedpg.NewOutbox(db, spec)
	c.Assert(err, qt.IsNil)
	c.Assert(outbox.InstallForIsolatedSource(ctx), qt.IsNil)

	_, err = db.ExecContext(ctx, `UPDATE articles SET unwatched = 'x' WHERE id = 2`)
	c.Assert(err, qt.IsNil)

	c.Assert(outboxRowsFor(c, ctx, db, spec, "2"), qt.Equals, 0)

	// The control: the filter's own column is still watched, so the scoping did
	// not achieve its result by finding nothing at all.
	_, err = db.ExecContext(ctx, `UPDATE articles SET published = false WHERE id = 1`)
	c.Assert(err, qt.IsNil)

	c.Assert(outboxRowsFor(c, ctx, db, spec, "1"), qt.Equals, 1)
}

// seedDecoyConstraint plants a constraint sharing the probe's name on a table
// the generation has nothing to do with.
func seedDecoyConstraint(c *qt.C, ctx context.Context, db *sql.DB) {
	c.Helper()
	statements := []string{
		`ALTER TABLE articles ADD COLUMN unwatched TEXT`,
		`CREATE TABLE unrelated (id BIGINT PRIMARY KEY, unwatched TEXT)`,
		`ALTER TABLE unrelated ADD CONSTRAINT ptah_filter_probe_check CHECK (unwatched IS NULL OR unwatched <> '')`,
	}
	for _, statement := range statements {
		_, err := db.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}
}

// TestEmbedPGAFilterTheServerRefusesIsRefusedE2E is the other half: a filter
// whose columns cannot be established is refused at install rather than
// installed with a trigger that cannot see them.
func TestEmbedPGAFilterTheServerRefusesIsRefusedE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.TimescaleDB)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	adminDB, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()

	name := fmt.Sprintf("ptah_filter_refused_%d", time.Now().UnixNano())
	createE2EDatabase(c, ctx, adminDB, name)
	defer dropE2EDatabase(c, context.Background(), adminDB, name)

	db, err := sql.Open("pgx", replaceDatabaseName(c, dbURL, name))
	c.Assert(err, qt.IsNil)
	defer db.Close()

	spec := filteredLiveSpec()
	seedFilteredArticles(c, ctx, db, spec)

	// A subquery: PostgreSQL answers `cannot use subquery in check constraint`,
	// and a row-level trigger could not have evaluated one either.
	refused := filteredLiveSpec()
	refused.Source.Filter = `id IN (SELECT id FROM articles WHERE published)`
	outbox, err := embedpg.NewOutbox(db, refused)
	c.Assert(err, qt.IsNil)

	err = outbox.InstallForIsolatedSource(ctx)

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "cannot be watched for changes")
	// And nothing was installed, so the refusal is not half a state.
	installed, err := outbox.Installed(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(installed, qt.IsFalse)
}

// outboxRowsFor counts the events the outbox holds for one source key.
//
// The key is matched as the JSON array the capture function writes -- `["1"]`,
// not `1`. A source key is a LIST of fields, so the outbox stores every key the
// same way whether the specification names one column or three, and a test
// comparing against the bare value silently counts zero.
func outboxRowsFor(
	c *qt.C, ctx context.Context, db *sql.DB, spec embedgen.Spec, id string,
) int {
	c.Helper()
	outbox, err := embedpg.NewOutbox(db, spec)
	c.Assert(err, qt.IsNil)
	var count int
	// #nosec G201 -- the table name comes from the outbox itself.
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE row_key = $1", outbox.TableName())
	c.Assert(db.QueryRowContext(ctx, query, `["`+id+`"]`).Scan(&count), qt.IsNil)
	return count
}
