//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/adoptpreflight"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/migrator"
)

// TestAdoptPreflightDoesNotWriteToTheHistoryItInspectsE2E is the promise the
// adoption preflight is built on: it decides whether native Ptah may take over
// a database's migration history WITHOUT changing that history.
//
// stokaro/ptah#1215 forbids "rewriting persisted state merely to make a check
// pass", and the preflight reads through the migrator that owns the layout --
// whose read entry points all call Initialize, which creates an absent revision
// table and ALTERS an existing one into the current layout. Dry-run is what
// makes Initialize inspect instead, and this measures that it does.
//
// The base-column table is the case that discriminates. Measured on PostgreSQL
// 17 with the dry-run line removed, the preflight upgraded the operator's table
// from
//
//	version, description, applied_at
//
// to
//
//	version, description, applied_at, state, applied, total, error,
//	error_stmt, execution_time_ms, checksum
//
// An empty database proves nothing on its own: no table is located there, so
// nothing calls Initialize and the broken version passes. Both rows are here
// because only the first one fails when the guard is removed.
func TestAdoptPreflightDoesNotWriteToTheHistoryItInspectsE2E(t *testing.T) {
	adminURL := dbtarget.URL(t, dbtarget.PostgreSQL)

	tests := []struct {
		name    string
		seed    []string
		wantSQL string
		want    string
		why     string
	}{
		{
			name: "a revision table carrying only the base columns",
			seed: []string{
				`CREATE TABLE schema_migrations (
					version bigint PRIMARY KEY,
					description text NOT NULL,
					applied_at timestamptz NOT NULL DEFAULT now()
				)`,
				`INSERT INTO schema_migrations (version, description) VALUES (20260101000000, 'init')`,
			},
			want: "version,description,applied_at",
			why:  "the discriminating row: without dry-run this table is altered in place",
		},
		{
			name: "no revision table at all",
			seed: nil,
			want: "",
			why:  "the control, which cannot fail: an absent table is never located, so nothing initializes it",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			c.Cleanup(cancel)

			databaseURL := newAdoptPreflightDatabase(c, ctx, adminURL, test.seed)

			conn, err := dbschema.ConnectToDatabase(ctx, databaseURL)
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { _ = conn.Close() })

			report, err := adoptpreflight.Analyze(ctx, adoptpreflight.Options{
				Conn:           conn,
				DatabaseURL:    databaseURL,
				RevisionFormat: migrator.RevisionTableFormatPtah,
			})
			c.Assert(err, qt.IsNil)

			// The report is asserted too, because a preflight that failed to
			// find the table would also leave it untouched -- and would pass
			// the column check for the wrong reason.
			c.Assert(report.Format, qt.Equals, string(migrator.RevisionTableFormatPtah))

			c.Assert(adoptPreflightColumns(c, ctx, databaseURL), qt.Equals, test.want,
				qt.Commentf("%s", test.why))
		})
	}
}

// newAdoptPreflightDatabase creates a throwaway database, runs the seed in it,
// and returns its URL.
func newAdoptPreflightDatabase(c *qt.C, ctx context.Context, adminURL string, seed []string) string {
	c.Helper()

	adminDB, err := sql.Open("pgx", adminURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = adminDB.Close() })

	name := fmt.Sprintf("ptah_adopt_preflight_%d", time.Now().UnixNano())
	createE2EDatabase(c, ctx, adminDB, name)
	c.Cleanup(func() { dropE2EDatabase(c, context.Background(), adminDB, name) })

	databaseURL := replaceDatabaseName(c, adminURL, name)
	if len(seed) == 0 {
		return databaseURL
	}

	db, err := sql.Open("pgx", databaseURL)
	c.Assert(err, qt.IsNil)
	defer db.Close()
	for _, statement := range seed {
		_, err := db.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil, qt.Commentf("seed: %s", statement))
	}
	return databaseURL
}

// adoptPreflightColumns reads the revision table's columns back, in order, and
// answers "" for a table that is not there. Reading the catalog is the point:
// an assertion on the preflight's own report would be the preflight agreeing
// with itself about what it did.
func adoptPreflightColumns(c *qt.C, ctx context.Context, databaseURL string) string {
	c.Helper()

	db, err := sql.Open("pgx", databaseURL)
	c.Assert(err, qt.IsNil)
	defer db.Close()

	var columns sql.NullString
	err = db.QueryRowContext(ctx,
		`SELECT string_agg(column_name, ',' ORDER BY ordinal_position)
		 FROM information_schema.columns
		 WHERE table_schema = 'public' AND table_name = 'schema_migrations'`,
	).Scan(&columns)
	c.Assert(err, qt.IsNil)
	return columns.String
}
