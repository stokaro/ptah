//go:build integration

package devlock_test

import (
	"context"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/jackc/pgx/v5"

	"ptah.run/dbschema"
	"ptah.run/internal/dblock"
	pglock "ptah.run/internal/dblockext/postgres"
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
	key := pglock.Key("ptah-dev-replay:postgres:" + database)

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

// MySQL takes a GET_LOCK name of at most 64 characters, and a database name
// may be 64 characters itself. A dev database with a 60-character name locks,
// a second replay of it is refused while the first holds it, and another one
// whose name shares the first 59 characters is another realm and locks too.
func TestAcquire_LocksADevDatabaseWithALongNameLive(t *testing.T) {
	for _, engine := range []dbtarget.Engine{dbtarget.MySQLAdmin, dbtarget.MariaDBAdmin} {
		t.Run(engine.String(), func(t *testing.T) {
			c := qt.New(t)
			stem := "ptah_devlock_" + strconv.FormatInt(time.Now().UnixNano(), 10) + "_"
			stem += strings.Repeat("x", 59-len(stem))
			first := mysqlFamilyDevDatabase(c, engine, stem+"a")
			second := mysqlFamilyDevDatabase(c, engine, stem+"b")

			held, err := devlock.Acquire(c.Context(), connectDev(c, first), dblock.NoWait)
			c.Assert(err, qt.IsNil)
			refused, err := devlock.Acquire(c.Context(), connectDev(c, first), dblock.NoWait)
			c.Assert(dblock.IsTimeout(err), qt.IsTrue, qt.Commentf("error: %v", err))
			c.Assert(refused, qt.IsNil)
			other, err := devlock.Acquire(c.Context(), connectDev(c, second), dblock.NoWait)
			c.Assert(err, qt.IsNil)

			c.Assert(other.Release(), qt.IsNil)
			c.Assert(held.Release(), qt.IsNil)
		})
	}
}

// mysqlFamilyDevDatabase creates the database name on engine's server, drops
// it when the test ends, and returns the URL ptah connects to it with.
func mysqlFamilyDevDatabase(c *qt.C, engine dbtarget.Engine, name string) string {
	c.Helper()
	c.Assert(name, qt.HasLen, 60)
	adminURL := dbtarget.URL(c, engine)
	admin := connectDev(c, adminURL)
	_, err := admin.ExecContext(c.Context(), "CREATE DATABASE `"+name+"`")
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		_, err := admin.ExecContext(ctx, "DROP DATABASE IF EXISTS `"+name+"`")
		c.Check(err, qt.IsNil)
	})
	slash := strings.LastIndex(adminURL, "/")
	c.Assert(slash, qt.Not(qt.Equals), -1)
	return adminURL[:slash+1] + name
}

// connectDev opens a connection to databaseURL, closed when the test ends.
func connectDev(c *qt.C, databaseURL string) *dbschema.DatabaseConnection {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), databaseURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	return conn
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
