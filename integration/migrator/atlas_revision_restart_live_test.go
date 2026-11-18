//go:build integration

package migrator_test

import (
	"context"
	"net/url"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/migrator"
)

// atlasRepairs is one migration whose body fails until the table it writes
// exists, so the first run leaves a failed row for a retry to restart.
func atlasRepairs() fstest.MapFS {
	return fstest.MapFS{
		"0000000001_repairs.up.sql":   &fstest.MapFile{Data: []byte("INSERT INTO repairs (id) VALUES (1);\n")},
		"0000000001_repairs.down.sql": &fstest.MapFile{Data: []byte("DELETE FROM repairs;\n")},
	}
}

// atlasMigrator runs fsys against the Atlas revision table on a connection of
// its own, outside a transaction, so its restart of a dirty row records on
// the migrator's own connection.
func atlasMigrator(c *qt.C, r *repeatableEngineTarget, fsys fstest.MapFS) *migrator.Migrator {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), r.url)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	mig, err := migrator.NewFSMigrator(conn, fsys)
	c.Assert(err, qt.IsNil)
	return mig.WithRevisionTableFormat(migrator.RevisionTableFormatAtlas).
		WithTransactionMode(migrator.MigrationTxModeNone)
}

// clearAtlasError is what a restart does to the error of every row of the
// Atlas revision table, spelled for r. On PostgreSQL the table sits in a schema
// of its own name, as the community binary keeps it; on Oracle, where a schema
// is an account, it sits in the account's schema, and the migrator quotes the
// name, which keeps it lower case.
func clearAtlasError(r *repeatableEngineTarget) string {
	if r.engine.family == "oracle" {
		return `UPDATE "atlas_schema_revisions" SET error = ''`
	}
	return "UPDATE atlas_schema_revisions.atlas_schema_revisions SET error = ''"
}

// blockedSessionCount counts the sessions of r's scratch database, or its
// Oracle account, that wait for a row another transaction holds.
func blockedSessionCount(c *qt.C, r *repeatableEngineTarget) int {
	c.Helper()
	parsed, err := url.Parse(r.url)
	c.Assert(err, qt.IsNil)
	query := `SELECT COUNT(*) FROM pg_stat_activity WHERE datname = $1 AND wait_event_type = 'Lock'`
	scope := strings.TrimPrefix(parsed.Path, "/")
	admin := r.conn
	if r.engine.family == "oracle" {
		query = blockedSessions
		scope = strings.ToUpper(parsed.User.Username())
		admin = connectOracleLock(c, dbtarget.URL(c, dbtarget.OracleAdmin))
	}
	var count int
	c.Assert(admin.QueryRowContext(context.Background(), query, scope).Scan(&count), qt.IsNil)
	return count
}

// waitForBlockedSession reports whether, within half a minute, a session of
// r waits for a row another transaction holds.
func waitForBlockedSession(c *qt.C, r *repeatableEngineTarget) bool {
	c.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if blockedSessionCount(c, r) > 0 {
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return false
}

// The Atlas revision table has no state column: a row is dirty when it
// carries an error or counts fewer applied statements than its total, and a
// restart clears the error. So a retry of a failed migration rewrites the row
// only while it still carries an error. Here the stand-in for a first retry
// clears the error in a transaction it has not committed; the second retry,
// which runs without the lock and read the row with its error, waits on the
// row, and once the first commits it matches nothing, refuses, and runs no
// statement of the body. Without the guard it rewrites the row again and runs
// the body a second time.
func TestAtlasRevisionRestartLive_RefusesARowAnotherRetryRewrote(t *testing.T) {
	for _, engine := range []repeatableEngine{repeatablePostgres, repeatableOracle} {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			r := newRepeatableEngineTarget(c, t, engine)
			c.Assert(atlasMigrator(c, r, atlasRepairs()).MigrateUp(context.Background()), qt.IsNotNil)
			_, err := r.conn.ExecContext(context.Background(), "CREATE TABLE repairs (id INTEGER)")
			c.Assert(err, qt.IsNil)
			first, err := r.conn.BeginTx(context.Background(), nil)
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { _ = first.Rollback() })
			_, err = first.ExecContext(context.Background(), clearAtlasError(r))
			c.Assert(err, qt.IsNil)
			retry := atlasMigrator(c, r, atlasRepairs()).WithoutMigrationLock()
			var retryErr error
			var wg sync.WaitGroup
			wg.Go(func() {
				retryErr = retry.MigrateUpWithOptions(context.Background(), migrator.MigrateUpOptions{AllowDirty: true})
			})

			waited := waitForBlockedSession(c, r)
			c.Assert(first.Commit(), qt.IsNil)
			wg.Wait()

			c.Assert(retryErr, qt.ErrorIs, migrator.ErrRevisionClaimLost)
			c.Assert(retryErr, qt.ErrorMatches, `(?s).*the row of migration 1 no longer carries the error this run read.*`)
			c.Assert(countRows(c, r.conn, "repairs"), qt.Equals, 0)
			c.Assert(waited, qt.IsTrue)
		})
	}
}

// countRows counts the rows of a table conn reaches.
func countRows(c *qt.C, conn *dbschema.DatabaseConnection, table string) int {
	c.Helper()
	var count int
	row := conn.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM "+table)
	c.Assert(row.Scan(&count), qt.IsNil)
	return count
}
