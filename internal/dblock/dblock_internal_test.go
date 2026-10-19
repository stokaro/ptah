package dblock

// White-box testing required: which session a lock and its callback run on,
// and what happens to a physical session after a failed acquisition, are only
// observable through database/sql internals. Their ordering and pool-reuse
// edge cases cannot be asserted through the public API without coupling these
// unit tests to live servers.

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/dbschema"
)

func TestWithLockSession_KeepsLockAndCallbackOnPinnedSession(t *testing.T) {
	c := qt.New(t)
	conn := openSessionLockSQLite(c)
	runFailure := errors.New("target operation failed")
	releaseFailure := errors.New("lock release failed")
	var events []string
	var acquiredSession, callbackSession, witnessConnection *dbschema.DatabaseConnection

	acquire := func(
		ctx context.Context,
		session,
		witness *dbschema.DatabaseConnection,
		_, name string,
		_ time.Duration,
	) (*Lock, error) {
		events = append(events, "acquire")
		acquiredSession = session
		witnessConnection = witness
		_, err := session.ExecContext(ctx, "CREATE TEMPORARY TABLE lock_session_marker (id INTEGER)")
		c.Assert(err, qt.IsNil)
		return &Lock{
			name: name,
			release: func(releaseCtx context.Context) error {
				events = append(events, "release")
				var count int
				queryErr := session.QueryRowContext(
					releaseCtx,
					"SELECT COUNT(*) FROM lock_session_marker",
				).Scan(&count)
				c.Assert(queryErr, qt.IsNil)
				c.Assert(count, qt.Equals, 0)
				return releaseFailure
			},
		}, nil
	}

	runErr, releaseErr := withLockSession(
		c.Context(),
		conn,
		"ptah_test",
		time.Second,
		acquire,
		func(session *dbschema.DatabaseConnection, lock *Lock) error {
			events = append(events, "callback")
			callbackSession = session
			var count int
			queryErr := session.QueryRowContext(
				c.Context(),
				"SELECT COUNT(*) FROM lock_session_marker",
			).Scan(&count)
			c.Assert(queryErr, qt.IsNil)
			c.Assert(count, qt.Equals, 0)
			c.Assert(lock.Supported(), qt.IsTrue)
			return runFailure
		},
	)

	c.Assert(runErr, qt.ErrorIs, runFailure)
	c.Assert(releaseErr, qt.ErrorIs, releaseFailure)
	c.Assert(acquiredSession, qt.Equals, callbackSession)
	c.Assert(witnessConnection, qt.Equals, conn)
	c.Assert(witnessConnection, qt.Not(qt.Equals), acquiredSession)
	c.Assert(events, qt.DeepEquals, []string{"acquire", "callback", "release"})
	var count int
	c.Assert(
		conn.QueryRowContext(c.Context(), "SELECT COUNT(*) FROM lock_session_marker").Scan(&count),
		qt.ErrorMatches,
		".*no such table: lock_session_marker.*",
	)
}

func TestWithLockSession_AcquisitionFailureSkipsCallbackAndRelease(t *testing.T) {
	c := qt.New(t)
	conn := openSessionLockSQLite(c)
	acquireFailure := errors.New("lock acquisition failed")
	var callbackCount atomic.Int64

	runErr, releaseErr := withLockSession(
		c.Context(),
		conn,
		"ptah_test",
		time.Second,
		func(
			context.Context,
			*dbschema.DatabaseConnection,
			*dbschema.DatabaseConnection,
			string,
			string,
			time.Duration,
		) (*Lock, error) {
			return nil, acquireFailure
		},
		func(*dbschema.DatabaseConnection, *Lock) error {
			callbackCount.Add(1)
			return nil
		},
	)

	c.Assert(runErr, qt.ErrorIs, acquireFailure)
	c.Assert(releaseErr, qt.IsNil)
	c.Assert(callbackCount.Load(), qt.Equals, int64(0))
}

func TestWithLockSession_PromotesReleaseFailureAfterSuccessfulCallback(t *testing.T) {
	c := qt.New(t)
	conn := openSessionLockSQLite(c)
	releaseFailure := errors.New("lock release failed")

	runErr, releaseErr := withLockSession(
		c.Context(),
		conn,
		"ptah_test",
		time.Second,
		func(
			context.Context,
			*dbschema.DatabaseConnection,
			*dbschema.DatabaseConnection,
			string,
			string,
			time.Duration,
		) (*Lock, error) {
			return &Lock{release: func(context.Context) error { return releaseFailure }}, nil
		},
		func(*dbschema.DatabaseConnection, *Lock) error { return nil },
	)

	c.Assert(runErr, qt.ErrorMatches, "advisory lock session failed during release: lock release failed")
	c.Assert(runErr, qt.ErrorIs, releaseFailure)
	c.Assert(releaseErr, qt.IsNil)
}

func openSessionLockSQLite(c *qt.C) *dbschema.DatabaseConnection {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(
		c.Context(),
		"sqlite://"+filepath.Join(c.TB.TempDir(), "session-lock.db"),
	)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	return conn
}

// A session lock whose acquiring statement lost its answer may be held by the
// session, so the session is discarded rather than returned to the pool, on
// every engine whose lock a session holds.
func TestCloseAfterFailedAcquisition_DiscardsAmbiguousSession(t *testing.T) {
	tests := []struct {
		dialect string
		lost    string
	}{
		{dialect: platform.Postgres, lost: "lock response lost"},
		{dialect: platform.MySQL, lost: "GET_LOCK response lost"},
		{dialect: platform.SQLServer, lost: "sp_getapplock response lost"},
	}
	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)
			queryErr := errors.New(test.lost)
			db, tracker := openTrackingDB(c, queryErr)
			session, err := db.Conn(c.Context())
			c.Assert(err, qt.IsNil)

			lock, acquireErr := acquireOnSession(c.Context(), session, db, test.dialect, "ptah_test", time.Second, false)
			c.Assert(lock, qt.IsNil)
			c.Assert(acquireErr, qt.ErrorIs, queryErr)
			c.Assert(closeAfterFailedAcquisition(session, acquireErr), qt.ErrorIs, queryErr)
			c.Assert(tracker.closeCount.Load(), qt.Equals, int64(1))

			replacement, err := db.Conn(c.Context())
			c.Assert(err, qt.IsNil)
			c.Assert(tracker.openCount.Load(), qt.Equals, int64(2))
			c.Assert(replacement.Close(), qt.IsNil)
		})
	}
}

func TestCloseAfterFailedAcquisition_ReturnsDefiniteFailureSessionToPool(t *testing.T) {
	c := qt.New(t)
	db, tracker := openTrackingDB(c, nil)
	session, err := db.Conn(c.Context())
	c.Assert(err, qt.IsNil)
	timeoutErr := &TimeoutError{
		Dialect: platform.Postgres,
		Name:    "ptah_test",
		Timeout: time.Second,
	}

	c.Assert(closeAfterFailedAcquisition(session, timeoutErr), qt.ErrorIs, timeoutErr)
	c.Assert(tracker.closeCount.Load(), qt.Equals, int64(0))

	replacement, err := db.Conn(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(tracker.openCount.Load(), qt.Equals, int64(1))
	c.Assert(replacement.Close(), qt.IsNil)
}

func TestLockRelease_DiscardsSessionAfterTimeout(t *testing.T) {
	c := qt.New(t)
	db, tracker := openTrackingDB(c, nil)
	session, err := db.Conn(c.Context())
	c.Assert(err, qt.IsNil)
	lock := &Lock{
		conn: session,
		release: func(context.Context) error {
			return context.DeadlineExceeded
		},
	}

	c.Assert(lock.Release(c.Context()), qt.ErrorIs, context.DeadlineExceeded)
	c.Assert(tracker.closeCount.Load(), qt.Equals, int64(1))
	c.Assert(lock.Release(c.Context()), qt.IsNil)

	replacement, err := db.Conn(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(tracker.openCount.Load(), qt.Equals, int64(2))
	c.Assert(replacement.Close(), qt.IsNil)
}

type trackingDB struct {
	openCount  atomic.Int64
	closeCount atomic.Int64
	queryErr   error
}

type trackingDriver struct {
	tracker *trackingDB
}

type trackingConn struct {
	tracker *trackingDB
}

var trackingDriverID atomic.Int64

func openTrackingDB(c *qt.C, queryErr error) (*sql.DB, *trackingDB) {
	c.Helper()
	tracker := &trackingDB{queryErr: queryErr}
	driverName := "ptah_dblock_tracking_" + strconv.FormatInt(trackingDriverID.Add(1), 10)
	sql.Register(driverName, &trackingDriver{tracker: tracker})
	db, err := sql.Open(driverName, "")
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		c.Check(db.Close(), qt.IsNil)
	})
	return db, tracker
}

func (d *trackingDriver) Open(string) (driver.Conn, error) {
	d.tracker.openCount.Add(1)
	return &trackingConn{tracker: d.tracker}, nil
}

func (c *trackingConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepare is not supported")
}

func (c *trackingConn) Close() error {
	c.tracker.closeCount.Add(1)
	return nil
}

func (c *trackingConn) Begin() (driver.Tx, error) {
	return nil, errors.New("transactions are not supported")
}

func (c *trackingConn) QueryContext(
	context.Context,
	string,
	[]driver.NamedValue,
) (driver.Rows, error) {
	return nil, c.tracker.queryErr
}

var (
	_ driver.Conn           = (*trackingConn)(nil)
	_ driver.QueryerContext = (*trackingConn)(nil)
)
