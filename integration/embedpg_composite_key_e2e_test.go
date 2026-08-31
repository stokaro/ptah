//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the pgx driver for database/sql

	"go.5x5.cz/ptah/internal/dbtarget"
	"go.5x5.cz/ptah/internal/embedengine"
	"go.5x5.cz/ptah/internal/embedgen"
	"go.5x5.cz/ptah/internal/embedpg"
	"go.5x5.cz/ptah/internal/embedrun"
)

// TestEmbedPGCompositeKeyE2E runs the same machinery over a two-part key.
//
// A single-column key hides two defects at once, and both are silent. A write
// that addressed its row by the first key component alone would update every
// row sharing it -- every document of a tenant getting one document's vector --
// and every count in the run would still agree. And a keyset cursor built as a
// chain of ORs rather than a row comparison gets the tenant boundary wrong,
// skipping or repeating whole tenants.
//
// So the fixture is two tenants with overlapping local ids, which is the shape
// that separates both (stokaro/ptah#2068).
func TestEmbedPGCompositeKeyE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.TimescaleDB)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	adminDB, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()

	name := fmt.Sprintf("ptah_embedpg_composite_%d", time.Now().UnixNano())
	createE2EDatabase(c, ctx, adminDB, name)
	defer dropE2EDatabase(c, context.Background(), adminDB, name)

	db, err := sql.Open("pgx", replaceDatabaseName(c, dbURL, name))
	c.Assert(err, qt.IsNil)
	defer db.Close()

	spec := compositeSpec()
	seedNotes(c, ctx, db, spec)

	store := embedpg.NewStore(db)
	c.Assert(store.EnsureSchema(ctx), qt.IsNil)
	source, err := embedpg.NewSource(db, spec)
	c.Assert(err, qt.IsNil)
	target, err := embedpg.NewTarget(db, spec)
	c.Assert(err, qt.IsNil)
	c.Assert(store.CreateRun(ctx, embedrun.Run{
		ID: "composite-run", SpecDigest: "spec-composite",
		GenerationIdentity: spec.Identity().Digest,
		Environment:        "test", Source: "public.notes", Target: "public.notes.embedding",
		ProviderProfile: "fake", PtahVersion: "test", PolicyDigest: "policy",
		Phase: embedrun.PhaseBackfilling, Status: embedrun.StatusRunning,
		LeaseOwner: "worker-a", FencingToken: 1,
		CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
	}), qt.IsNil)

	engine := &embedengine.Engine{
		Spec: spec, Source: source, Provider: &liveProvider{dimension: 4},
		Target: target, Store: store,
		// Two rows a page, so the scan crosses the tenant boundary mid-run.
		Bounds: embedrun.BatchBounds{MaxRows: 2, MaxInputs: 2}, Worker: "worker-a",
	}

	finished, _, err := engine.Backfill(ctx, "composite-run")

	c.Assert(err, qt.IsNil)
	c.Assert(finished.Progress.RowsScanned, qt.Equals, int64(6))
	c.Assert(finished.Progress.RowsEmbedded, qt.Equals, int64(6))
	c.Assert(finished.Cursor, qt.DeepEquals, []string{"b", "3"})

	// Every row has its own vector. Under a write addressed by the first key
	// component alone, each tenant's three rows would carry one vector three
	// times -- whichever the batch wrote last.
	c.Assert(vectorsByKey(c, ctx, db, spec), qt.DeepEquals, map[string]string{
		"a/1": "[8,9,10,11]", "a/2": "[9,10,11,12]", "a/3": "[10,11,12,13]",
		"b/1": "[11,12,13,14]", "b/2": "[12,13,14,15]", "b/3": "[13,14,15,16]",
	})

	assertTheCursorCrossesTheTenantBoundary(c, ctx, source)
	assertANullKeyIsRefused(c, ctx, db)
	assertTheScanQuotesFiltersAndOrders(c, ctx, db)
}

// assertTheScanQuotesFiltersAndOrders covers the three things the well-behaved
// fixture above cannot say anything about.
//
// The notes table has a primary key starting with `tenant`, so PostgreSQL
// answers `ORDER BY tenant` from that index and hands back full key order for
// free -- a query that ordered by the first component only would look correct
// on it forever. This table has no index at all, so the order has to come from
// the query.
//
// Its identifiers need quoting and its filter excludes half the rows, and both
// of those are unmeasured by a fixture spelled in lowercase with no filter: an
// unquoted `"Local Id"` is a syntax error and an ignored filter embeds rows the
// specification excluded.
func assertTheScanQuotesFiltersAndOrders(c *qt.C, ctx context.Context, db *sql.DB) {
	c.Helper()
	for _, statement := range []string{
		// The quote inside a column name is not a curiosity: an identifier
		// from a specification is a string somebody wrote, and quoting that
		// does not double it produces a query PostgreSQL reads as ending
		// early.
		`CREATE TABLE "Odd Notes" (
			"Tenant" TEXT NOT NULL, "Local Id" BIGINT NOT NULL, "Bo""dy" TEXT, "Keep" BOOLEAN NOT NULL)`,
		`INSERT INTO "Odd Notes" ("Tenant", "Local Id", "Bo""dy", "Keep") VALUES
			('b', 2, 'bb', true), ('a', 3, 'ac', false), ('b', 1, 'ba', true),
			('a', 1, 'aa', true), ('b', 3, 'bc', false), ('a', 2, 'ab', true)`,
	} {
		_, err := db.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}
	odd := compositeSpec()
	odd.Source.Table = "Odd Notes"
	odd.Source.KeyFields = []string{"Tenant", "Local Id"}
	odd.Source.InputFields = []string{`Bo"dy`}
	odd.Source.Filter = `"Keep"`
	odd.Target.Table = "Odd Notes"
	source, err := embedpg.NewSource(db, odd)
	c.Assert(err, qt.IsNil)

	page, err := source.Scan(ctx, nil, 10)

	c.Assert(err, qt.IsNil)
	c.Assert(keysOf(page.Rows), qt.DeepEquals, []string{"a/1", "a/2", "b/1", "b/2"})
	// And the same order holds mid-scan, which is where an order taken from
	// the first key component alone comes apart.
	page, err = source.Scan(ctx, []string{"a", "1"}, 2)
	c.Assert(err, qt.IsNil)
	c.Assert(keysOf(page.Rows), qt.DeepEquals, []string{"a/2", "b/1"})
}

// keysOf renders a page's keys for an assertion.
func keysOf(rows []embedgen.Row) []string {
	keys := make([]string, 0, len(rows))
	for _, row := range rows {
		keys = append(keys, strings.Join(row.Key, "/"))
	}
	return keys
}

// assertANullKeyIsRefused keeps a row a keyset cannot name from being scanned
// past.
//
// A NULL key folded to an empty string makes two different rows share a cursor,
// and from there the scan either repeats one of them forever or passes both.
// Neither shows up in a count afterwards.
func assertANullKeyIsRefused(c *qt.C, ctx context.Context, db *sql.DB) {
	c.Helper()
	for _, statement := range []string{
		`CREATE TABLE loose_notes (tenant TEXT, local_id BIGINT, body TEXT)`,
		`INSERT INTO loose_notes (tenant, local_id, body) VALUES ('a', 1, 'fine'), (NULL, 2, 'not')`,
	} {
		_, err := db.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}
	loose := compositeSpec()
	loose.Source.Table = "loose_notes"
	loose.Target.Table = "loose_notes"
	source, err := embedpg.NewSource(db, loose)
	c.Assert(err, qt.IsNil)

	_, err = source.Scan(ctx, nil, 10)

	c.Assert(err, qt.ErrorMatches,
		`key column tenant is NULL in loose_notes, and a keyset scan cannot resume after a row it cannot name`)
}

// compositeSpec keys on a tenant and a local id.
func compositeSpec() embedgen.Spec {
	return embedgen.Spec{
		Source: embedgen.Source{
			Schema: "public", Table: "notes",
			KeyFields:   []string{"tenant", "local_id"},
			InputFields: []string{"body"},
		},
		Preprocessing: embedgen.Preprocessing{
			Separator: "\n", NullPolicy: embedgen.NullAsEmpty, EmptyPolicy: embedgen.EmptySkipRow,
		},
		Model: embedgen.Model{
			Provider: "fake", Identifier: "fake-model", Revision: "1", ReportedDimension: 4,
		},
		Target: embedgen.Target{
			Schema: "public", Table: "notes", Column: "embedding",
			Representation: "vector", Metric: embedgen.MetricCosine,
		},
	}
}

// seedNotes creates two tenants with the same local ids, and bodies whose
// lengths differ by one so that every vector is distinguishable.
func seedNotes(c *qt.C, ctx context.Context, db *sql.DB, spec embedgen.Spec) {
	c.Helper()
	statements := []string{
		`CREATE EXTENSION IF NOT EXISTS vector`,
		`CREATE TABLE notes (
			tenant TEXT NOT NULL,
			local_id BIGINT NOT NULL,
			body TEXT,
			PRIMARY KEY (tenant, local_id)
		)`,
		fmt.Sprintf(`ALTER TABLE notes
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
		// Inserted out of key order on purpose. A heap scan answers in
		// insertion order, so a query that forgot its ORDER BY -- or ordered
		// by the first key component only -- returns these rows in an order
		// the keyset then treats as ascending, and skips whatever it passed.
		// Rows inserted in key order hide that completely.
		`INSERT INTO notes (tenant, local_id, body) VALUES
			('b', 2, '123456789012'),
			('a', 3, '1234567890'),
			('b', 1, '12345678901'),
			('a', 1, '12345678'),
			('b', 3, '1234567890123'),
			('a', 2, '123456789')`,
	}
	for _, statement := range statements {
		_, err := db.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}
}

// vectorsByKey reads every row's vector back through the server.
func vectorsByKey(c *qt.C, ctx context.Context, db *sql.DB, spec embedgen.Spec) map[string]string {
	c.Helper()
	rows, err := db.QueryContext(ctx, fmt.Sprintf(
		`SELECT tenant, local_id, %s::text FROM notes ORDER BY tenant, local_id`, spec.Target.Column))
	c.Assert(err, qt.IsNil)
	defer rows.Close()

	vectors := make(map[string]string)
	for rows.Next() {
		var tenant string
		var localID int64
		var vector sql.NullString
		c.Assert(rows.Scan(&tenant, &localID, &vector), qt.IsNil)
		vectors[fmt.Sprintf("%s/%d", tenant, localID)] = vector.String
	}
	c.Assert(rows.Err(), qt.IsNil)
	return vectors
}

// assertTheCursorCrossesTheTenantBoundary is what a row comparison buys.
//
// Asked to continue after ('a', 3), a keyset built as `tenant >= $1 AND
// local_id > $2` answers with nothing at all -- tenant b's rows have local ids
// 1 to 3, and none of them is greater than 3. The whole tenant disappears, and
// the run reports a clean finish over two thirds of the corpus.
func assertTheCursorCrossesTheTenantBoundary(c *qt.C, ctx context.Context, source *embedpg.Source) {
	c.Helper()

	page, err := source.Scan(ctx, []string{"a", "3"}, 10)

	c.Assert(err, qt.IsNil)
	c.Assert(page.Rows, qt.HasLen, 3)
	c.Assert(page.Rows[0].Key, qt.DeepEquals, []string{"b", "1"})
	c.Assert(page.Rows[2].Key, qt.DeepEquals, []string{"b", "3"})
	c.Assert(page.Done, qt.IsTrue)

	// And the middle of a tenant resumes inside it rather than jumping to the
	// next one.
	page, err = source.Scan(ctx, []string{"a", "1"}, 2)
	c.Assert(err, qt.IsNil)
	c.Assert(page.Rows[0].Key, qt.DeepEquals, []string{"a", "2"})
	c.Assert(page.Rows[1].Key, qt.DeepEquals, []string{"a", "3"})
}
