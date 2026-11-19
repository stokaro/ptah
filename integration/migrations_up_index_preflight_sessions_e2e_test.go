//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// preflightTable is a migration body the up index preflight has nothing to
// inspect in.
const preflightTable = "CREATE TABLE notes (id int PRIMARY KEY, body text);\n"

// preflightNoTransaction runs a migration file outside a transaction, which a
// concurrent index build needs.
const preflightNoTransaction = "-- +ptah no_transaction\n"

// TestMigrationsUpOpensTheIndexPreflightSessionOnlyForAnIndexE2E applies one
// migration file to an empty PostgreSQL database and counts the sessions the
// run established there, against a file in the same transaction mode that
// creates the table alone. The preflight that looks for an index a failed
// build left behind opens a session of its own and discards it; on a server at
// its connection limit, the connection the run asks for next is refused
// (SQLSTATE 53300). A body with nothing for it to inspect opens no such
// session, so the bare table opens exactly one session less than a body that
// creates an index with IF NOT EXISTS or drops one.
func TestMigrationsUpOpensTheIndexPreflightSessionOnlyForAnIndexE2E(t *testing.T) {
	c := qt.New(t)
	table := map[string]int{
		"":                     sessionsForMigration(c, preflightTable),
		preflightNoTransaction: sessionsForMigration(c, preflightNoTransaction+preflightTable),
	}
	tests := []struct {
		name      string
		mode      string
		statement string
		// extra is how many sessions more than the bare table the run opens.
		extra int
	}{
		{name: "an index built without IF NOT EXISTS", statement: "CREATE INDEX notes_body ON notes (body);\n", extra: 0},
		{name: "an index created with IF NOT EXISTS", statement: "CREATE INDEX IF NOT EXISTS notes_body ON notes (body);\n", extra: 1},
		{name: "a dropped index", statement: "DROP INDEX IF EXISTS notes_body;\n", extra: 1},
		{
			name: "a concurrent build without IF NOT EXISTS", mode: preflightNoTransaction,
			statement: "CREATE INDEX CONCURRENTLY notes_body ON notes (body);\n", extra: 0,
		},
		{
			name: "a concurrent build with IF NOT EXISTS", mode: preflightNoTransaction,
			statement: "CREATE INDEX CONCURRENTLY IF NOT EXISTS notes_body ON notes (body);\n", extra: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			got := sessionsForMigration(c, tt.mode+preflightTable+tt.statement)

			c.Assert(got-table[tt.mode], qt.Equals, tt.extra)
		})
	}
}

// sessionsForMigration applies a migration directory holding one file with
// body to an empty database of its own, and returns how many sessions the run
// established there.
func sessionsForMigration(c *qt.C, body string) int {
	c.Helper()
	admin, err := sql.Open("pgx", dbtarget.URL(c, dbtarget.PostgreSQL))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(admin.Close(), qt.IsNil) })
	name := fmt.Sprintf("preflight_sessions_%d", time.Now().UnixNano())
	createE2EDatabase(c, c.Context(), admin, name)
	c.Cleanup(func() { dropE2EDatabase(c, context.Background(), admin, name) })
	dir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_notes.up.sql"), []byte(body), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_notes.down.sql"), []byte("DROP TABLE notes;\n"), 0o600), qt.IsNil)

	applied := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir},
		"migrations", "up", "--db-url", replaceDatabaseName(c, dbtarget.URL(c, dbtarget.PostgreSQL), name), "--migrations-dir", dir)

	c.Assert(applied.ExitCode, qt.Equals, 0, historyComment(applied))
	return sessionsEstablished(c, admin, name)
}

// sessionsEstablished returns pg_stat_database.sessions for database once no
// session is connected to it. A backend writes its statistics as it exits and
// before it leaves pg_stat_activity, so the count is final by then.
func sessionsEstablished(c *qt.C, admin *sql.DB, database string) int {
	c.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		var connected int
		err := admin.QueryRowContext(c.Context(), "SELECT count(*) FROM pg_stat_activity WHERE datname = $1", database).Scan(&connected)
		c.Assert(err, qt.IsNil)
		if connected == 0 {
			break
		}
		if time.Now().After(deadline) {
			c.Fatalf("%d sessions are still connected to %s", connected, database)
		}
		time.Sleep(50 * time.Millisecond)
	}
	var sessions int
	err := admin.QueryRowContext(c.Context(), "SELECT sessions FROM pg_stat_database WHERE datname = $1", database).Scan(&sessions)
	c.Assert(err, qt.IsNil)
	return sessions
}
