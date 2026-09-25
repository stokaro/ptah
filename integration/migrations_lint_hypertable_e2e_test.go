//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the pgx driver for database/sql

	"ptah.run/internal/dbtarget"
)

// TestNativeMigrationsLintAdvisesTheHypertableRemedyE2E drives PG101 through
// the shipped binary against TimescaleDB (stokaro/ptah#3551).
//
// PG101's ordinary remedy is CREATE INDEX CONCURRENTLY, which TimescaleDB
// refuses on a hypertable. Nothing in the migration text says the table is
// one: the first version's create_hypertable call is replayed on the dev
// database, and only the extension's catalog, read before the second version,
// can say so. Each row moves one fact away from the reporting case:
//
//   - the same table left ordinary keeps the CONCURRENTLY advice;
//   - the per-chunk build TimescaleDB offers, in a file that runs outside a
//     transaction, replays and is not reported.
func TestNativeMigrationsLintAdvisesTheHypertableRemedyE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.TimescaleDB)

	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	repoRoot := e2eRepoRoot(t)
	nativeBinary := filepath.Join(t.TempDir(), "ptah")
	buildPtah(c, ctx, repoRoot, nativeBinary)

	adminDB, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()

	const events = "CREATE EXTENSION IF NOT EXISTS timescaledb;\n" +
		"CREATE TABLE events (id bigint NOT NULL, happened_at timestamptz NOT NULL, kind text NOT NULL);\n"
	tests := []struct {
		name    string
		base    string
		change  string
		want    []string
		wantNot []string
	}{
		{
			name:   "an index on a hypertable",
			base:   events + "SELECT create_hypertable('events', 'happened_at');\n",
			change: "CREATE INDEX events_kind_idx ON events (kind, happened_at DESC);\n",
			want: []string{
				"] PG101: ",
				"CREATE INDEX on the hypertable events blocks writes to it and to every chunk",
				"WITH (timescaledb.transaction_per_chunk) in a no_transaction migration",
			},
			wantNot: []string{"use CREATE INDEX CONCURRENTLY"},
		},
		{
			name:    "control: the same index on the table left ordinary",
			base:    events,
			change:  "CREATE INDEX events_kind_idx ON events (kind, happened_at DESC);\n",
			want:    []string{"] PG101: ", "use CREATE INDEX CONCURRENTLY outside a transaction"},
			wantNot: []string{"on the hypertable"},
		},
		{
			name: "the per-chunk build outside a transaction",
			base: events + "SELECT create_hypertable('events', 'happened_at');\n",
			change: "-- atlas:txmode none\n\n" +
				"CREATE INDEX events_kind_idx ON events (kind, happened_at DESC) WITH (timescaledb.transaction_per_chunk);\n",
			wantNot: []string{"] PG101: "},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			testDBName := fmt.Sprintf("ptah_lint_hypertable_e2e_%d", time.Now().UnixNano())
			createE2EDatabase(c, ctx, adminDB, testDBName)
			defer dropE2EDatabase(c, context.Background(), adminDB, testDBName)

			migrationsDir := c.TempDir()
			writeLintE2EFile(c, migrationsDir, "1.sql", test.base)
			writeLintE2EFile(c, migrationsDir, "2.sql", test.change)

			stdout, stderr, err := runLintE2EBinary(ctx, nativeBinary,
				"migrations", "lint",
				"--dir", migrationsDir,
				"--dir-format", "atlas",
				"--dev-url", replaceDatabaseName(c, dbURL, testDBName),
				"--latest", "1",
				"--fail-on", "none",
			)

			c.Assert(exitStatusOf(c, err), qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
			// The dev database was read, so no rule went without the state it asked for.
			c.Assert(stderr, qt.Not(qt.Contains), "ran without the")
			for _, want := range test.want {
				c.Assert(stdout, qt.Contains, want)
			}
			for _, wantNot := range test.wantNot {
				c.Assert(stdout, qt.Not(qt.Contains), wantNot)
			}
		})
	}
}
