package dblock

// White-box testing required: the verification runs inside Acquire, between
// taking the lock and returning it, and the state it distinguishes -- a lock
// that excludes nobody -- has no exported surface. Its whole observable effect
// is which error Acquire returns.

import (
	"database/sql/driver"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/dbschema/dbtest"
)

// pooledPostgres answers pg_try_advisory_lock the way a transaction pooler
// makes a server answer it: true every time, because both clients are on one
// backend and a session lock is reentrant there.
//
// Measured through PgBouncer 1.25.2 in transaction mode, two independent client
// connections and the same key -- both acquired, and both reported
// pg_backend_pid 113.
func pooledPostgres(answers, unlocks *atomic.Int64) func(string, []driver.NamedValue) (dbtest.QueryResult, error) {
	return func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
		if strings.Contains(query, "pg_try_advisory_lock") {
			answers.Add(1)
			return dbtest.QueryResult{
				Columns: []string{"pg_try_advisory_lock"},
				Rows:    [][]driver.Value{{true}},
			}, nil
		}
		unlocks.Add(1)
		return dbtest.QueryResult{
			Columns: []string{"pg_advisory_unlock"},
			Rows:    [][]driver.Value{{true}},
		}, nil
	}
}

// directPostgres answers the way a server with one session per client does: the
// first attempt takes the lock and the second is refused.
func directPostgres(answers, unlocks *atomic.Int64) func(string, []driver.NamedValue) (dbtest.QueryResult, error) {
	return func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
		if strings.Contains(query, "pg_try_advisory_lock") {
			return dbtest.QueryResult{
				Columns: []string{"pg_try_advisory_lock"},
				Rows:    [][]driver.Value{{answers.Add(1) == 1}},
			}, nil
		}
		unlocks.Add(1)
		return dbtest.QueryResult{
			Columns: []string{"pg_advisory_unlock"},
			Rows:    [][]driver.Value{{true}},
		}, nil
	}
}

// TestAcquire_RefusesALockThatExcludesNobody pins both answers of the check
// that stands between taking a lock and trusting it.
//
// A PostgreSQL advisory lock is session-scoped, and a transaction pooler hands
// a client whichever backend is free between transactions -- so a second client
// can be handed the SAME backend the first locked on, where the lock is
// reentrant and answers true again. It does not weaken the lock; it removes it,
// and it fails OPEN: two migration runs would both believe they held it
// (stokaro/ptah#1029).
//
// The check asks the property rather than identifying the proxy, which is what
// makes it a test with two rows rather than a probe for a product name.
func TestAcquire_RefusesALockThatExcludesNobody(t *testing.T) {
	tests := []struct {
		name string
		// server decides what the second attempt answers, which is the whole
		// difference between the two topologies.
		server func(*atomic.Int64, *atomic.Int64) func(string, []driver.NamedValue) (dbtest.QueryResult, error)
		// wantRefused carries the outcome as a value, because a Contains
		// assertion against an empty substring passes on any string and would
		// let a check that refuses everything through.
		wantRefused bool
		// wantErr is the substring the refusal carries, empty where the lock
		// stands.
		wantErr string
		// wantUnlocks is how many times the witness gave its own claim back. A
		// refusal that kept it would leave the backend holding a lock nobody
		// will release.
		wantUnlocks int64
	}{
		{
			name:   "a direct server refuses the second attempt, so the lock stands",
			server: directPostgres,
		},
		{
			name:        "a pooled server takes the same lock twice, so the run is refused",
			server:      pooledPostgres,
			wantRefused: true,
			wantErr:     "excludes nothing on this connection",
			wantUnlocks: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var answers, unlocks atomic.Int64
			db := dbtest.Open(t, test.server(&answers, &unlocks))
			session, err := db.SQL.Conn(c.Context())
			c.Assert(err, qt.IsNil)
			defer func() { _ = session.Close() }()
			c.Assert(acquirePostgresLock(c.Context(), session, platform.Postgres, "ptah_test", time.Second), qt.IsNil)

			verifyErr := verifyPostgresLockExcludes(c.Context(), db.SQL, platform.Postgres, "ptah_test")

			c.Assert(verifyErr != nil, qt.Equals, test.wantRefused)
			c.Assert(acquireErrorText(verifyErr), qt.Contains, test.wantErr)
			c.Assert(unlocks.Load(), qt.Equals, test.wantUnlocks)
		})
	}
}

// TestAcquire_UnverifiedLockOptOutIsHonored covers the escape hatch, because an
// opt-out nothing exercises is an opt-out that stops working quietly.
//
// It opts out of the REFUSAL, not of the lock: the lock is still taken, and
// what is skipped is the proof that it excludes anybody. That distinction is
// what the variable's name and its documentation have to keep, so the test
// asserts the lock is still acquired rather than only that no error came back.
func TestAcquire_UnverifiedLockOptOutIsHonored(t *testing.T) {
	c := qt.New(t)
	t.Setenv(AllowUnverifiedLock.Name(), "1")

	var answers, unlocks atomic.Int64
	db := dbtest.Open(t, pooledPostgres(&answers, &unlocks))
	session, err := db.SQL.Conn(c.Context())
	c.Assert(err, qt.IsNil)
	defer func() { _ = session.Close() }()
	c.Assert(acquirePostgresLock(c.Context(), session, platform.Postgres, "ptah_test", time.Second), qt.IsNil)

	c.Assert(verifyPostgresLockExcludes(c.Context(), db.SQL, platform.Postgres, "ptah_test"), qt.IsNil)

	// The lock was taken and the witness was never asked, so nothing had to be
	// given back either.
	c.Assert(answers.Load(), qt.Equals, int64(1))
	c.Assert(unlocks.Load(), qt.Equals, int64(0))
}

// acquireErrorText renders the error so a row states the absent case as a
// value rather than as a branch.
func acquireErrorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
