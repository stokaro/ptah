package dblock

// White-box testing required: advisory-lock polling and timeout conversions
// are only observable through SQL sent to live servers. Their retry and
// numeric edge cases cannot be asserted through the public Acquire API without
// coupling these unit tests to live PostgreSQL-family, MySQL, MariaDB, and SQL
// Server instances.

import (
	"context"
	"database/sql/driver"
	"math"
	"sync/atomic"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/internal/dbschema/dbtest"
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

func TestMySQLLockTimeoutSeconds(t *testing.T) {
	c := qt.New(t)

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
		c.Run(test.name, func(c *qt.C) {
			c.Assert(mySQLLockTimeoutSeconds(test.dialect, test.timeout), qt.Equals, test.want)
		})
	}
}

func TestSQLServerLockTimeoutMilliseconds(t *testing.T) {
	c := qt.New(t)

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
		c.Run(test.name, func(c *qt.C) {
			c.Assert(sqlServerLockTimeoutMilliseconds(test.timeout), qt.Equals, test.want)
		})
	}
}
