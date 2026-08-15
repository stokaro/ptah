package dblock

// White-box testing required: advisory-lock polling and timeout conversions
// and physical-session disposal are only observable through SQL sent to live
// servers and database/sql internals. Their retry, numeric, and pool-reuse
// edge cases cannot be asserted through the public Acquire API without
// coupling these unit tests to live PostgreSQL-family, MySQL, MariaDB, and
// SQL Server instances.

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"math"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/internal/dbschema/dbtest"
)

func TestAcquirePostgresLock_RetriesUntilAcquired(t *testing.T) {
	c := qt.New(t)
	responses := []bool{false, true}
	var attempt atomic.Int64
	db := dbtest.Open(t, func(
		query string,
		args []driver.NamedValue,
	) (dbtest.QueryResult, error) {
		c.Check(query, qt.Equals, "SELECT pg_try_advisory_lock($1)")
		c.Check(args, qt.DeepEquals, []driver.NamedValue{{
			Ordinal: 1,
			Value:   PostgresKey("ptah_test"),
		}})
		index := min(int(attempt.Add(1)-1), len(responses)-1)
		return dbtest.QueryResult{
			Columns: []string{"pg_try_advisory_lock"},
			Rows:    [][]driver.Value{{responses[index]}},
		}, nil
	})
	conn, err := db.SQL.Conn(t.Context())
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() {
		c.Check(conn.Close(), qt.IsNil)
	})

	err = acquirePostgresLock(
		t.Context(),
		conn,
		platform.YugabyteDB,
		"ptah_test",
		time.Second,
	)

	c.Assert(err, qt.IsNil)
	c.Assert(db.QueryCount(), qt.Equals, 2)
}

func TestAcquirePostgresLock_ReportsDialectOnTimeout(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, func(
		string,
		[]driver.NamedValue,
	) (dbtest.QueryResult, error) {
		return dbtest.QueryResult{
			Columns: []string{"pg_try_advisory_lock"},
			Rows:    [][]driver.Value{{false}},
		}, nil
	})
	conn, err := db.SQL.Conn(t.Context())
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() {
		c.Check(conn.Close(), qt.IsNil)
	})

	err = acquirePostgresLock(
		context.Background(),
		conn,
		platform.YugabyteDB,
		"ptah_test",
		time.Millisecond,
	)

	c.Assert(
		err,
		qt.ErrorMatches,
		`timed out acquiring advisory lock "ptah_test" on yugabytedb after 1ms`,
	)
	c.Assert(IsTimeout(err), qt.IsTrue)
}

func TestPostgresLockWaitError_MapsQueryDeadlineToTimeout(t *testing.T) {
	c := qt.New(t)
	lockCtx, cancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	defer cancel()

	err := postgresLockWaitError(
		t.Context(),
		lockCtx,
		platform.Postgres,
		"ptah_test",
		25*time.Millisecond,
		context.DeadlineExceeded,
	)

	c.Assert(
		err,
		qt.ErrorMatches,
		`timed out acquiring advisory lock "ptah_test" on postgres after 25ms`,
	)
	c.Assert(IsTimeout(err), qt.IsTrue)
}

func TestCloseAfterFailedAcquisition_DiscardsAmbiguousSession(t *testing.T) {
	c := qt.New(t)
	queryErr := errors.New("lock response lost")
	db, tracker := openTrackingDB(c, queryErr)
	session, err := db.Conn(c.Context())
	c.Assert(err, qt.IsNil)

	acquireErr := acquirePostgresLock(
		c.Context(),
		session,
		platform.Postgres,
		"ptah_test",
		time.Second,
	)
	c.Assert(acquireErr, qt.ErrorIs, queryErr)
	c.Assert(closeAfterFailedAcquisition(session, acquireErr), qt.ErrorIs, queryErr)
	c.Assert(tracker.closeCount.Load(), qt.Equals, int64(1))

	replacement, err := db.Conn(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(tracker.openCount.Load(), qt.Equals, int64(2))
	c.Assert(replacement.Close(), qt.IsNil)
}

func TestAcquireMySQLLock_DiscardsSessionAfterAmbiguousFailure(t *testing.T) {
	c := qt.New(t)
	queryErr := errors.New("GET_LOCK response lost")
	db, tracker := openTrackingDB(c, queryErr)
	session, err := db.Conn(c.Context())
	c.Assert(err, qt.IsNil)

	acquireErr := acquireMySQLLock(
		c.Context(),
		session,
		platform.MySQL,
		"ptah_test",
		time.Second,
	)
	c.Assert(acquireErr, qt.ErrorIs, queryErr)
	c.Assert(closeAfterFailedAcquisition(session, acquireErr), qt.ErrorIs, queryErr)
	c.Assert(tracker.closeCount.Load(), qt.Equals, int64(1))

	replacement, err := db.Conn(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(tracker.openCount.Load(), qt.Equals, int64(2))
	c.Assert(replacement.Close(), qt.IsNil)
}

func TestAcquireSQLServerLock_DiscardsSessionAfterAmbiguousFailure(t *testing.T) {
	c := qt.New(t)
	queryErr := errors.New("sp_getapplock response lost")
	db, tracker := openTrackingDB(c, queryErr)
	session, err := db.Conn(c.Context())
	c.Assert(err, qt.IsNil)

	acquireErr := acquireSQLServerLock(
		c.Context(),
		session,
		"ptah_test",
		time.Second,
	)
	c.Assert(acquireErr, qt.ErrorIs, queryErr)
	c.Assert(closeAfterFailedAcquisition(session, acquireErr), qt.ErrorIs, queryErr)
	c.Assert(tracker.closeCount.Load(), qt.Equals, int64(1))

	replacement, err := db.Conn(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(tracker.openCount.Load(), qt.Equals, int64(2))
	c.Assert(replacement.Close(), qt.IsNil)
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

func TestMySQLLockTimeoutSeconds(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		timeout time.Duration
		want    float64
	}{
		{name: "mysql default uses native infinite timeout", dialect: "mysql", want: -1},
		{name: "mariadb default avoids unsupported negative timeout", dialect: "mariadb", want: mariaDBDefaultTimeoutSeconds},
		{name: "mysql explicit subsecond rounds up", dialect: "mysql", timeout: 500 * time.Millisecond, want: 1},
		{name: "mariadb explicit duration", dialect: "mariadb", timeout: 2 * time.Second, want: 2},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(mySQLLockTimeoutSeconds(test.dialect, test.timeout), qt.Equals, test.want)
		})
	}
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

func TestSQLServerLockTimeoutMilliseconds(t *testing.T) {
	tests := []struct {
		name    string
		timeout time.Duration
		want    int
	}{
		{name: "default waits indefinitely", want: -1},
		{name: "submillisecond rounds up", timeout: time.Nanosecond, want: 1},
		{name: "explicit duration", timeout: 1500 * time.Millisecond, want: 1500},
		{name: "caps at SQL Server int maximum", timeout: time.Duration(math.MaxInt32+1) * time.Millisecond, want: math.MaxInt32},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(sqlServerLockTimeoutMilliseconds(test.timeout), qt.Equals, test.want)
		})
	}
}
