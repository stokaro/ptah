//go:build integration

package devlock_test

import (
	"context"
	"net/url"
	"strconv"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/jackc/pgx/v5"

	"ptah.run/dbschema"
	"ptah.run/internal/dblock"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/devlock"
)

func TestSameRealm_PostgresDriverURLOverridesLive(t *testing.T) {
	c := qt.New(t)
	targetURL := dbtarget.URL(c, dbtarget.PostgreSQL)
	overrideURL := postgresDriverOverrideURL(c, targetURL)
	targetConn, err := dbschema.ConnectToDatabase(c.Context(), targetURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(targetConn)
	overrideConn, err := dbschema.ConnectToDatabase(c.Context(), overrideURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(overrideConn)

	same, err := devlock.SameRealm(c.Context(), targetConn, overrideConn)

	c.Assert(err, qt.IsNil)
	c.Assert(same, qt.IsTrue)
}

func TestSameRealm_NetworkDatabasesLive(t *testing.T) {
	tests := []struct {
		name   string
		engine dbtarget.Engine
	}{
		{name: "postgres", engine: dbtarget.PostgreSQL},
		{name: "cockroachdb", engine: dbtarget.CockroachDB},
		{name: "yugabytedb", engine: dbtarget.YugabyteDB},
		{name: "mysql", engine: dbtarget.MySQL},
		{name: "mariadb", engine: dbtarget.MariaDB},
		{name: "clickhouse", engine: dbtarget.ClickHouse},
		{name: "sqlserver", engine: dbtarget.SQLServer},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			databaseURL := dbtarget.URL(c, test.engine)
			firstConn, err := dbschema.ConnectToDatabase(c.Context(), databaseURL)
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() {
				dbschema.CloseAndWarn(firstConn)
			})
			secondConn, err := dbschema.ConnectToDatabase(c.Context(), databaseURL)
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() {
				dbschema.CloseAndWarn(secondConn)
			})

			same, err := devlock.SameRealm(c.Context(), firstConn, secondConn)

			c.Assert(err, qt.IsNil)
			c.Assert(same, qt.IsTrue)
		})
	}
}

func TestAcquire_CockroachDBSerializesSameRealmLive(t *testing.T) {
	c := qt.New(t)
	databaseURL := dbtarget.URL(c, dbtarget.CockroachDB)
	firstConn, err := dbschema.ConnectToDatabase(c.Context(), databaseURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(firstConn)
	secondConn, err := dbschema.ConnectToDatabase(c.Context(), databaseURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(secondConn)

	firstLock, err := devlock.Acquire(c.Context(), firstConn, 0)
	c.Assert(err, qt.IsNil)

	waitCtx, cancel := context.WithTimeout(c.Context(), 50*time.Millisecond)
	defer cancel()
	blockedLock, err := devlock.Acquire(waitCtx, secondConn, 0)
	c.Assert(err, qt.ErrorIs, context.DeadlineExceeded)
	c.Assert(blockedLock, qt.IsNil)

	c.Assert(firstLock.Release(), qt.IsNil)
	secondLock, err := devlock.Acquire(c.Context(), secondConn, 0)
	c.Assert(err, qt.IsNil)
	c.Assert(secondLock.Release(), qt.IsNil)
}

func postgresDriverOverrideURL(c *qt.C, rawURL string) string {
	c.Helper()
	config, err := pgx.ParseConfig(rawURL)
	c.Assert(err, qt.IsNil)
	parsed, err := url.Parse(rawURL)
	c.Assert(err, qt.IsNil)
	if parsed.Scheme == "" {
		// dbtarget accepts a scheme-less driver DSN, because that is a legitimate
		// shape for several engines. This test rewrites the address as a URL, so
		// it still has to refuse one that is not.
		c.Skip("the configured PostgreSQL address is a driver DSN, not a URL")
	}
	parsed.Host = "guard.invalid:1"
	parsed.Path = "/ignored"
	query := parsed.Query()
	query.Set("host", config.Host)
	query.Set("port", strconv.Itoa(int(config.Port)))
	query.Set("database", config.Database)
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

// A dev database lock lives on a session of its own while the replay runs on
// other connections. When the server ends that session, the context Guard
// returned ends, and settle reports the loss as the work's error, whatever the
// work saw: here the work only waits for the context, and its cancellation is
// not reported a second time. The release afterwards gives nothing back,
// since the server released the lock with the session.
func TestGuard_ReportsTheEndOfTheLockSessionLive(t *testing.T) {
	c := qt.New(t)
	databaseURL := dbtarget.URL(c, dbtarget.PostgreSQL)
	conn, err := dbschema.ConnectToDatabase(c.Context(), databaseURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	witness, err := dbschema.ConnectToDatabase(c.Context(), databaseURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(witness) })
	var database string
	c.Assert(witness.QueryRowContext(c.Context(), "SELECT current_database()").Scan(&database), qt.IsNil)
	key := dblock.PostgresKey("ptah-dev-replay:postgres:" + database)

	lock, err := devlock.Acquire(c.Context(), conn, 0)
	c.Assert(err, qt.IsNil)
	guarded, settle := lock.Guard(context.Background())
	_, err = witness.ExecContext(c.Context(),
		"SELECT pg_terminate_backend(pid) FROM pg_locks WHERE locktype = 'advisory' AND granted AND objid::bigint = $1", key)
	c.Assert(err, qt.IsNil)

	c.Assert(endedWithin(guarded, 10*time.Second), qt.IsTrue)
	err = settle(guarded.Err())
	c.Assert(dblock.IsLost(err), qt.IsTrue, qt.Commentf("error: %v", err))
	c.Assert(err, qt.ErrorMatches, `dev database lock: advisory lock "ptah-dev-replay:postgres:`+database+`" on postgres `+
		`was lost while it was held: .*; the work on the dev database stopped there`)
	c.Assert(lock.Release(), qt.IsNil)
}

// endedWithin reports whether ctx ends within limit.
func endedWithin(ctx context.Context, limit time.Duration) bool {
	timer := time.NewTimer(limit)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return true
	case <-timer.C:
		return false
	}
}
