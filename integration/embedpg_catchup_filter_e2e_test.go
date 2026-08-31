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
	_ "github.com/jackc/pgx/v5/stdlib"

	"go.5x5.cz/ptah/internal/dbtarget"
	"go.5x5.cz/ptah/internal/embedengine"
	"go.5x5.cz/ptah/internal/embedgen"
	"go.5x5.cz/ptah/internal/embedpg"
	"go.5x5.cz/ptah/internal/embedrun"
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
	source, err := embedpg.NewSource(db, spec)
	c.Assert(err, qt.IsNil)
	target, err := embedpg.NewTarget(db, spec)
	c.Assert(err, qt.IsNil)
	outbox, err := embedpg.NewOutbox(db, spec)
	c.Assert(err, qt.IsNil)

	c.Assert(outbox.Install(ctx), qt.IsNil)
	boundary, err := outbox.Horizon(ctx)
	c.Assert(err, qt.IsNil)

	c.Assert(store.CreateRun(ctx, embedrun.Run{
		ID: filterRunID, SpecDigest: "spec-1", GenerationIdentity: spec.Identity().Digest,
		Environment: "test", Source: "public.articles", Target: "public.articles.embedding",
		ProviderProfile: "fake", PtahVersion: "test", PolicyDigest: "policy",
		Phase: embedrun.PhaseBackfilling, Status: embedrun.StatusRunning,
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
