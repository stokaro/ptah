package dblock_test

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/dbschema"
	"ptah.run/internal/dblock"
	"ptah.run/internal/dbschema/dbtest"
)

func TestSupported(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		want    bool
	}{
		{name: "postgres", dialect: "postgres", want: true},
		{name: "postgresql alias", dialect: "postgresql", want: true},
		{name: "mysql", dialect: "mysql", want: true},
		{name: "mariadb", dialect: "mariadb", want: true},
		{name: "sqlserver", dialect: "sqlserver", want: true},
		{name: "sqlite", dialect: "sqlite", want: false},
		{name: "clickhouse", dialect: "clickhouse", want: false},
		{name: "cockroachdb", dialect: "cockroachdb", want: false},
		{name: "yugabytedb", dialect: "yugabytedb", want: true},
		{name: "spanner", dialect: "spanner", want: false},
		{name: "ydb", dialect: "ydb", want: true},
		{name: "ydbs alias", dialect: "ydbs", want: true},
		{name: "unknown", dialect: "not-a-dialect", want: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(dblock.Supported(test.dialect), qt.Equals, test.want)
		})
	}
}

// TestSupportedDialects_HappyPath drives the two readers of one declaration
// against each other. The list names the alternatives a refusal offers, and
// Supported answers whether this target is one of them; a second copy of the
// set would agree on the day it was written and stop agreeing when an engine
// is added to the first.
func TestSupportedDialects_HappyPath(t *testing.T) {
	c := qt.New(t)

	dialects := dblock.SupportedDialects()

	// The order is the dialect registry's, which a refusal prints.
	c.Assert(dialects, qt.DeepEquals, []string{"postgres", "mysql", "mariadb", "sqlserver", "yugabytedb", "ydb"})
	for _, dialect := range dialects {
		c.Assert(dblock.Supported(dialect), qt.IsTrue, qt.Commentf("dialect %q", dialect))
	}
}

// TestSupportedDialects_ReturnsACopy pins the contract a caller sorting or
// filtering the list depends on.
func TestSupportedDialects_ReturnsACopy(t *testing.T) {
	c := qt.New(t)
	first := dblock.SupportedDialects()

	first[0] = "not-a-dialect"

	c.Assert(dblock.SupportedDialects(), qt.Not(qt.DeepEquals), first)
	c.Assert(dblock.Supported(dblock.SupportedDialects()[0]), qt.IsTrue)
}

func TestAcquire_HappyPath(t *testing.T) {
	t.Run("sqlite acquires a no-op lock", func(t *testing.T) {
		c := qt.New(t)
		conn := openSQLite(c)

		lock, err := dblock.Acquire(c.Context(), conn, "ptah_test_lock", time.Second)
		c.Assert(err, qt.IsNil)
		c.Assert(lock.Supported(), qt.IsFalse)
		c.Assert(lock.Release(c.Context()), qt.IsNil)
	})

	t.Run("no-op lock release is idempotent", func(t *testing.T) {
		c := qt.New(t)
		conn := openSQLite(c)

		lock, err := dblock.Acquire(c.Context(), conn, "ptah_test_lock", 0)
		c.Assert(err, qt.IsNil)
		c.Assert(lock.Release(c.Context()), qt.IsNil)
		c.Assert(lock.Release(c.Context()), qt.IsNil)
	})

	t.Run("nil and zero locks release without error", func(t *testing.T) {
		c := qt.New(t)
		var lock *dblock.Lock
		c.Assert(lock.Release(c.Context()), qt.IsNil)
		c.Assert(lock.Supported(), qt.IsFalse)
		c.Assert(new(dblock.Lock).Release(c.Context()), qt.IsNil)
	})
}

func TestWithLockSession_NoOpDialectStillPinsCallback(t *testing.T) {
	c := qt.New(t)
	conn := openSQLite(c)
	var callbackSession *dbschema.DatabaseConnection

	runErr, releaseErr := dblock.WithLockSession(
		c.Context(),
		conn,
		"ptah_test_lock",
		time.Second,
		func(session *dbschema.DatabaseConnection, lock *dblock.Lock) error {
			callbackSession = session
			c.Assert(lock.Supported(), qt.IsFalse)
			_, err := session.ExecContext(c.Context(),
				"CREATE TEMPORARY TABLE lock_session_marker (id INTEGER)")
			return err
		},
	)

	c.Assert(runErr, qt.IsNil)
	c.Assert(releaseErr, qt.IsNil)
	c.Assert(callbackSession, qt.Not(qt.Equals), conn)
	var count int
	c.Assert(
		conn.QueryRowContext(c.Context(), "SELECT COUNT(*) FROM lock_session_marker").Scan(&count),
		qt.ErrorMatches,
		".*no such table: lock_session_marker.*",
	)
}

func TestAcquire_FailurePath(t *testing.T) {
	t.Run("no connection", func(t *testing.T) {
		c := qt.New(t)

		lock, err := dblock.Acquire(c.Context(), nil, "ptah_test_lock", time.Second)
		c.Assert(err, qt.ErrorMatches, `advisory locking requires database connection`)
		c.Assert(lock, qt.IsNil)
	})

	t.Run("a nil database connection", func(t *testing.T) {
		c := qt.New(t)
		var conn *dbschema.DatabaseConnection

		lock, err := dblock.Acquire(c.Context(), conn, "ptah_test_lock", time.Second)
		c.Assert(err, qt.ErrorMatches, `advisory locking requires database connection`)
		c.Assert(lock, qt.IsNil)
	})

	t.Run("empty name rejected", func(t *testing.T) {
		c := qt.New(t)
		conn := openSQLite(c)

		lock, err := dblock.Acquire(c.Context(), conn, "  ", time.Second)
		c.Assert(err, qt.ErrorMatches, `advisory lock name must not be empty`)
		c.Assert(lock, qt.IsNil)
	})
}

func TestTimeoutError(t *testing.T) {
	c := qt.New(t)

	err := &dblock.TimeoutError{Dialect: "postgres", Name: "ptah_schema_apply", Timeout: 250 * time.Millisecond}
	c.Assert(err.Error(), qt.Equals, `timed out acquiring advisory lock "ptah_schema_apply" on postgres after 250ms`)
}

// A negative timeout is a refusal to wait, so the error names the holder
// rather than a wait that never happened.
func TestTimeoutErrorUnderNoWait(t *testing.T) {
	c := qt.New(t)

	err := &dblock.TimeoutError{Dialect: "postgres", Name: "ptah_schema_apply", Timeout: dblock.NoWait}
	c.Assert(err.Error(), qt.Equals, `advisory lock "ptah_schema_apply" on postgres is held by another session`)
	c.Assert(dblock.IsTimeout(err), qt.IsTrue)
}

func TestIsTimeout(t *testing.T) {
	c := qt.New(t)

	wrapped := fmt.Errorf("acquire schema apply lock: %w", &dblock.TimeoutError{
		Dialect: "postgres",
		Name:    "ptah_schema_apply",
		Timeout: time.Second,
	})
	c.Assert(dblock.IsTimeout(wrapped), qt.IsTrue)
	c.Assert(dblock.IsTimeout(fmt.Errorf("other error")), qt.IsFalse)
	c.Assert(dblock.IsTimeout(nil), qt.IsFalse)
}

func openSQLite(c *qt.C) *dbschema.DatabaseConnection {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+filepath.Join(c.TB.TempDir(), "dblock.db"))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	return conn
}

// connection is a pool that names its dialect: the part of a
// *dbschema.DatabaseConnection that Acquire reads.
type connection struct {
	*sql.DB
	dialect string
}

func (c connection) Info() catalog.ServerInfo { return catalog.ServerInfo{Dialect: c.dialect} }

// advisoryServer answers pg_try_advisory_lock with the answers in turn, the
// last one from then on, and pg_advisory_unlock with true. It counts both.
type advisoryServer struct {
	answers  []bool
	attempts atomic.Int64
	unlocks  atomic.Int64
}

func (s *advisoryServer) answer(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
	if strings.Contains(query, "pg_try_advisory_lock") {
		index := min(int(s.attempts.Add(1)-1), len(s.answers)-1)
		return dbtest.QueryResult{Columns: []string{"pg_try_advisory_lock"}, Rows: [][]driver.Value{{s.answers[index]}}}, nil
	}
	s.unlocks.Add(1)
	return dbtest.QueryResult{Columns: []string{"pg_advisory_unlock"}, Rows: [][]driver.Value{{true}}}, nil
}

// openAdvisory returns a pool of dialect over a server that answers as
// server does.
func openAdvisory(c *qt.C, dialect string, server *advisoryServer) connection {
	c.Helper()
	return connection{DB: dbtest.Open(c, server.answer).SQL, dialect: dialect}
}

// A lock through a pool that is not a *dbschema.DatabaseConnection is taken
// the same way, and a direct server, whose second session is refused the lock,
// keeps it.
func TestAcquire_ThroughAPool_HappyPath(t *testing.T) {
	t.Run("a direct server refuses the witness, so the lock stands", func(t *testing.T) {
		c := qt.New(t)
		server := &advisoryServer{answers: []bool{true, false}}
		conn := openAdvisory(c, "postgres", server)

		lock, err := dblock.Acquire(c.Context(), conn, " ptah_test ", time.Second)

		c.Assert(err, qt.IsNil)
		c.Assert(lock.Supported(), qt.IsTrue)
		c.Assert(lock.Name(), qt.Equals, "ptah_test")
		c.Assert(server.attempts.Load(), qt.Equals, int64(2))
		c.Assert(server.unlocks.Load(), qt.Equals, int64(0))
		c.Assert(lock.Release(c.Context()), qt.IsNil)
		c.Assert(server.unlocks.Load(), qt.Equals, int64(1))
	})

	// The opt-out skips the proof and not the lock: the lock is taken, and
	// the witness is never asked.
	t.Run("the opt-out keeps a lock that excludes nobody", func(t *testing.T) {
		c := qt.New(t)
		t.Setenv(dblock.AllowUnverifiedLock.Name(), "1")
		server := &advisoryServer{answers: []bool{true}}
		conn := openAdvisory(c, "postgres", server)

		lock, err := dblock.Acquire(c.Context(), conn, "ptah_test", time.Second)

		c.Assert(err, qt.IsNil)
		c.Assert(lock.Supported(), qt.IsTrue)
		c.Assert(server.attempts.Load(), qt.Equals, int64(1))
		c.Assert(server.unlocks.Load(), qt.Equals, int64(0))
		c.Assert(lock.Release(c.Context()), qt.IsNil)
	})
}

func TestAcquire_ThroughAPool_FailurePath(t *testing.T) {
	t.Run("a lock another session holds times out under the dialect's name", func(t *testing.T) {
		c := qt.New(t)
		server := &advisoryServer{answers: []bool{false}}
		conn := openAdvisory(c, "yugabytedb", server)

		lock, err := dblock.Acquire(c.Context(), conn, "ptah_test", time.Millisecond)

		c.Assert(err, qt.ErrorMatches, `timed out acquiring advisory lock "ptah_test" on yugabytedb after 1ms`)
		c.Assert(dblock.IsTimeout(err), qt.IsTrue)
		c.Assert(lock, qt.IsNil)
	})

	// A transaction pooler hands the witness the backend the lock was taken
	// on, where the lock is reentrant, so the lock excludes nobody and the run
	// is refused, with the witness's claim given back (stokaro/ptah#1029).
	t.Run("a lock that excludes nobody is refused", func(t *testing.T) {
		c := qt.New(t)
		server := &advisoryServer{answers: []bool{true}}
		conn := openAdvisory(c, "postgres", server)

		lock, err := dblock.Acquire(c.Context(), conn, "ptah_test", time.Second)

		c.Assert(err, qt.ErrorMatches, `postgres advisory lock "ptah_test" excludes nothing on this connection: `+
			`a second connection took the same lock, which is what a transaction-pooling proxy such as PgBouncer `+
			`produces -- it hands both clients one backend session, where the lock is reentrant\. Two runs would `+
			`both believe they held it\. Connect to the database directly for schema-mutating commands, or set `+
			`PTAH_ALLOW_UNVERIFIED_MIGRATION_LOCK=1 to accept the risk`)
		c.Assert(dblock.IsTimeout(err), qt.IsFalse)
		c.Assert(lock, qt.IsNil)
		c.Assert(server.unlocks.Load(), qt.Equals, int64(1))
	})

	t.Run("a malformed opt-out is refused before the witness is asked", func(t *testing.T) {
		c := qt.New(t)
		t.Setenv(dblock.AllowUnverifiedLock.Name(), "maybe")
		server := &advisoryServer{answers: []bool{true}}
		conn := openAdvisory(c, "postgres", server)

		lock, err := dblock.Acquire(c.Context(), conn, "ptah_test", time.Second)

		c.Assert(err, qt.ErrorMatches, `invalid boolean value "maybe" for PTAH_ALLOW_UNVERIFIED_MIGRATION_LOCK`)
		c.Assert(lock, qt.IsNil)
		c.Assert(server.attempts.Load(), qt.Equals, int64(1))
	})
}
