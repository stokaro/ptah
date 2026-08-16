//go:build integration

// Package spanner_test holds the executed evidence for Cloud Spanner's
// PostgreSQL interface.
//
// Every other statement about Spanner in this repository rests on offline
// paths -- capability presets, planning, rendering, URL detection -- so nothing
// showed that a real endpoint accepts what Ptah asks it (stokaro/ptah#942).
// These tests are that evidence, and they are deliberately written to pin where
// the support currently STOPS as much as where it works: a boundary nobody has
// executed is a boundary nobody can trust.
//
// Nothing here declares a Spanner server yet, and that is held up by the
// support level rather than by the container: a line continuous integration
// exercises may not stay best-effort, and certifying this one needs the
// capability rows confirmed against a server, which is a different run from
// these two. So the address is supplied by hand:
//
//	docker run -d -p 5435:5432 \
//	  gcr.io/cloud-spanner-pg-adapter/pgadapter-emulator:v0.55.2
//	SPANNER_URL=spanner://localhost:5435/ptah_test?sslmode=disable \
//	  go test -tags integration ./integration/dbschema/spanner/...
package spanner_test

import (
	"context"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/dbtarget"
)

// The endpoint answers a banner with no token of its own, so the dialect cannot
// be detected from the server and has to come from the URL the operator wrote.
// That is the whole reason the `spanner` scheme exists, and it is invisible
// until something reads the catalog: the connection itself succeeds either way.
func TestSpannerLiveBannerCarriesNoProductToken(t *testing.T) {
	c := qt.New(t)
	rawURL := dbtarget.URL(c, dbtarget.Spanner)

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()

	conn, err := dbschema.ConnectToDatabase(ctx, rawURL)
	c.Assert(err, qt.IsNil)
	defer func() { _ = conn.Close() }()

	var version string
	c.Assert(conn.QueryRowContext(ctx, "SELECT version()").Scan(&version), qt.IsNil)

	c.Assert(version, qt.Contains, "PostgreSQL",
		qt.Commentf("the interface answers as PostgreSQL, which is what makes it reachable"))
	for _, token := range []string{"Spanner", "spanner", "Cloud"} {
		c.Assert(version, qt.Not(qt.Contains), token,
			qt.Commentf("banner=%q: if the banner ever names the product, detection can stop "+
				"depending on the URL scheme and this test should say so", version))
	}
}

// The schema read reaches the index catalog and stops there.
//
// Everything before it works: the reader asks the schema and table questions
// without the pg_catalog helpers this endpoint refuses, which is what
// stokaro/ptah#1579 built. Indexes are where that runs out -- the query joins
// pg_am, and Spanner has no such relation, so the read cannot degrade to a
// constant the way a missing function can.
//
// This asserts the boundary rather than skipping past it. When a Spanner index
// read exists, this test goes red and has to be rewritten, which is the point:
// the failure is the notification.
func TestSpannerLiveSchemaReadStopsAtTheIndexCatalog(t *testing.T) {
	c := qt.New(t)
	rawURL := dbtarget.URL(c, dbtarget.Spanner)

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()

	conn, err := dbschema.ConnectToDatabase(ctx, rawURL)
	c.Assert(err, qt.IsNil)
	defer func() { _ = conn.Close() }()

	_, readErr := conn.Reader().ReadSchema()

	c.Assert(readErr, qt.IsNotNil,
		qt.Commentf("a read that now succeeds means Spanner index support landed; rewrite this test"))
	c.Assert(readErr.Error(), qt.Contains, "failed to read indexes",
		qt.Commentf("the read is expected to get past schemas and tables and stop at indexes"))
	c.Assert(readErr.Error(), qt.Contains, "pg_am",
		qt.Commentf("and to stop on the missing relation, not on something earlier"))
}
