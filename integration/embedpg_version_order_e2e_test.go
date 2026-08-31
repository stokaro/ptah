//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
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

// TestEmbedPGATrimmedTimestampIsNotStaleE2E is stokaro/ptah#2635.
//
// `version_strategy: updated_at` carries the driver's rendering of a
// timestamptz, and RFC 3339 trims trailing zeros from the fractional seconds.
// The write resolution compared versions as opaque strings by LENGTH first, so
// an update at 11:00:00.1 rendered shorter than its predecessor at
// 10:00:00.123456 and was classified as older. The fresh provider answer — a
// request already made and paid for — was discarded, catch-up exited 0
// reporting success, the row kept the vector of text it no longer contained,
// and the watermark moved past the event so nothing reprocessed it.
//
// It runs live because the value under test is what the DRIVER renders. A test
// that wrote the string itself would be asserting against a rendering nobody
// produces, and the rendering is the whole defect.
func TestEmbedPGATrimmedTimestampIsNotStaleE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.TimescaleDB)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	adminDB, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()

	name := fmt.Sprintf("ptah_version_order_%d", time.Now().UnixNano())
	createE2EDatabase(c, ctx, adminDB, name)
	defer dropE2EDatabase(c, context.Background(), adminDB, name)

	db, err := sql.Open("pgx", replaceDatabaseName(c, dbURL, name))
	c.Assert(err, qt.IsNil)
	defer db.Close()

	spec := timestampVersionedSpec()
	seedTimestampVersioned(c, ctx, db, spec)

	// The two renderings the defect turns on, produced by the driver rather
	// than written here. The second instant is an hour LATER and its rendering
	// is shorter, because PostgreSQL trims the trailing zeros.
	earlier := renderedVersion(c, ctx, db, 1)
	c.Assert(len(earlier) > 0, qt.IsTrue)
	_, err = db.ExecContext(ctx,
		`UPDATE articles SET body = 'SECOND body', updated_at = timestamptz '2026-01-01 11:00:00.100000+00' WHERE id = 1`)
	c.Assert(err, qt.IsNil)
	later := renderedVersion(c, ctx, db, 1)

	c.Assert(len(later) < len(earlier), qt.IsTrue,
		qt.Commentf("the fixture must produce a SHORTER rendering of a LATER instant, "+
			"or this test asserts nothing: %q then %q", earlier, later))

	// What the old comparison answered, and what the strategy's own order does.
	c.Assert(embedgen.VersionUpdatedAt.VersionOrder(), qt.Equals, embedrun.OrderTimestamp)

	existing := embedrun.TargetWrite{
		Key: []string{"1"}, Generation: "gen-1", Kind: embedrun.WriteUpsert,
		Version: earlier, InputHash: "hash-first", Vector: []float32{1, 2, 3, 4},
	}
	incoming := existing
	incoming.Version = later
	incoming.InputHash = "hash-second"
	incoming.Vector = []float32{5, 6, 7, 8}

	resolved, changed, err := embedrun.ResolveWrite(
		&existing, incoming, embedgen.VersionUpdatedAt.VersionOrder())

	c.Assert(err, qt.IsNil)
	c.Assert(changed, qt.IsTrue,
		qt.Commentf("the later instant renders shorter; a length-first order calls it stale"))
	c.Assert(resolved.InputHash, qt.Equals, "hash-second")

	// The control: the same pair the other way round is genuinely older and
	// must still lose, or the fix would be "accept everything".
	_, backwards, err := embedrun.ResolveWrite(
		&incoming, existing, embedgen.VersionUpdatedAt.VersionOrder())
	c.Assert(err, qt.IsNil)
	c.Assert(backwards, qt.IsFalse)
}

// renderedVersion reads one row's version column back the way the source scan
// reads it: through the driver, as a string.
func renderedVersion(c *qt.C, ctx context.Context, db *sql.DB, id int) string {
	c.Helper()
	var version string
	c.Assert(db.QueryRowContext(ctx,
		// The column itself, scanned as a string -- which is what the source
		// scan does. A `::text` cast is PostgreSQL rendering it, and the
		// defect is about what the DRIVER renders.
		`SELECT updated_at FROM articles WHERE id = $1`, id).Scan(&version), qt.IsNil)
	return version
}

// timestampVersionedSpec versions by an update timestamp, which is the
// documented first-class strategy this is about.
func timestampVersionedSpec() embedgen.Spec {
	spec := liveSpec()
	spec.Source.VersionStrategy = embedgen.VersionUpdatedAt
	spec.Source.VersionField = "updated_at"
	return spec
}

// seedTimestampVersioned makes the source table with a real timestamptz.
func seedTimestampVersioned(c *qt.C, ctx context.Context, db *sql.DB, spec embedgen.Spec) {
	c.Helper()
	statements := []string{
		`CREATE EXTENSION IF NOT EXISTS vector`,
		`CREATE TABLE articles (
			id BIGINT PRIMARY KEY,
			title TEXT,
			body TEXT,
			updated_at TIMESTAMPTZ NOT NULL
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
		`INSERT INTO articles (id, title, body, updated_at) VALUES
			(1, 'First', 'FIRST body', timestamptz '2026-01-01 10:00:00.123456+00')`,
	}
	for _, statement := range statements {
		_, err := db.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}
}

var _ = embedengine.Page{}
