package postgres_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dblockext"
	"ptah.run/internal/dblockext/postgres"
	"ptah.run/internal/dbschema/dbtest"
)

// errHeld stands in for the timeout internal/dblock puts in a request.
var errHeld = errors.New("held by another session")

func request(timeout time.Duration) dblockext.Request {
	return dblockext.Request{Dialect: "yugabytedb", Name: "ptah_test", Timeout: timeout, Held: errHeld}
}

// server answers pg_try_advisory_lock with the answers in turn, the last one
// from then on, and pg_advisory_unlock with unlocked. It records the queries.
type server struct {
	answers  []bool
	unlocked bool
	delay    time.Duration
	err      error

	attempts atomic.Int64
	unlocks  atomic.Int64
	keys     []any
}

func (s *server) answer(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
	time.Sleep(s.delay)
	if s.err != nil {
		return dbtest.QueryResult{}, s.err
	}
	s.keys = append(s.keys, args[0].Value)
	if strings.HasPrefix(query, "SELECT pg_try_advisory_lock($1)") {
		index := min(int(s.attempts.Add(1)-1), len(s.answers)-1)
		return dbtest.QueryResult{Columns: []string{"pg_try_advisory_lock"}, Rows: [][]driver.Value{{s.answers[index]}}}, nil
	}
	s.unlocks.Add(1)
	return dbtest.QueryResult{Columns: []string{"pg_advisory_unlock"}, Rows: [][]driver.Value{{s.unlocked}}}, nil
}

func open(c *qt.C, s *server) (*sql.DB, dblockext.SQLSession) {
	c.Helper()
	db := dbtest.Open(c, s.answer)
	session, err := db.SQL.Conn(c.Context())
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = session.Close() })
	return db.SQL, dblockext.SQLSession{Conn: session}
}

// The key must stay stable across releases: concurrent runners of different
// Ptah versions must contend on the same pg_advisory_lock key.
func TestKey(t *testing.T) {
	c := qt.New(t)

	c.Assert(postgres.Key("ptah_migrate"), qt.Equals, int64(2705505214))
	c.Assert(postgres.Key("ptah_schema_apply"), qt.Not(qt.Equals), postgres.Key("ptah_migrate"))
	c.Assert(postgres.Key("custom-lock"), qt.Equals, postgres.Key("custom-lock"))
}

func TestServices(t *testing.T) {
	c := qt.New(t)

	services := postgres.Services()

	c.Assert(services.Dialects, qt.DeepEquals, []string{"postgres", "yugabytedb"})
	c.Assert(services.Session, qt.IsNotNil)
	c.Assert(services.Exclusion, qt.IsNotNil)
	c.Assert(services.Separate, qt.IsNil)
}

func TestAcquire_HappyPath(t *testing.T) {
	tests := []struct {
		name         string
		answers      []bool
		timeout      time.Duration
		wantAttempts int64
	}{
		{name: "granted at once", answers: []bool{true}, timeout: time.Second, wantAttempts: 1},
		{name: "asked again until granted", answers: []bool{false, true}, timeout: time.Second, wantAttempts: 2},
		{name: "a zero timeout waits until granted", answers: []bool{false, false, true}, wantAttempts: 3},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			s := &server{answers: test.answers}
			_, session := open(c, s)

			err := postgres.Services().Session.Acquire(c.Context(), session, request(test.timeout))

			c.Assert(err, qt.IsNil)
			c.Assert(s.attempts.Load(), qt.Equals, test.wantAttempts)
			c.Assert(s.keys[0], qt.Equals, postgres.Key("ptah_test"))
		})
	}
}

func TestAcquire_FailurePath(t *testing.T) {
	t.Run("held past the timeout", func(t *testing.T) {
		c := qt.New(t)
		_, session := open(c, &server{answers: []bool{false}})

		err := postgres.Services().Session.Acquire(c.Context(), session, request(30*time.Millisecond))

		c.Assert(err, qt.Equals, errHeld)
	})

	t.Run("a negative timeout asks once", func(t *testing.T) {
		c := qt.New(t)
		s := &server{answers: []bool{false, true}}
		_, session := open(c, s)

		err := postgres.Services().Session.Acquire(c.Context(), session, request(-1))

		c.Assert(err, qt.Equals, errHeld)
		c.Assert(s.attempts.Load(), qt.Equals, int64(1))
	})

	// The session may hold the lock when the answer was lost, so the failure
	// is ambiguous.
	t.Run("a lost answer is ambiguous", func(t *testing.T) {
		c := qt.New(t)
		lost := errors.New("lock response lost")
		_, session := open(c, &server{err: lost})

		err := postgres.Services().Session.Acquire(c.Context(), session, request(time.Second))

		c.Assert(err, qt.ErrorIs, lost)
		c.Assert(err, qt.ErrorAs, new(*dblockext.AmbiguousError))
	})

	// An answer that arrives after the timeout passed is the timeout, and
	// still ambiguous.
	t.Run("an answer lost to the timeout is an ambiguous timeout", func(t *testing.T) {
		c := qt.New(t)
		_, session := open(c, &server{delay: 50 * time.Millisecond, err: errors.New("canceled")})

		err := postgres.Services().Session.Acquire(c.Context(), session, request(time.Millisecond))

		c.Assert(err, qt.ErrorIs, errHeld)
		c.Assert(err, qt.ErrorAs, new(*dblockext.AmbiguousError))
	})

	// A wait the caller ends is the caller's error, not a timeout.
	t.Run("the caller's context ends the wait", func(t *testing.T) {
		c := qt.New(t)
		_, session := open(c, &server{answers: []bool{false}})
		ctx, cancel := context.WithTimeout(c.Context(), 30*time.Millisecond)
		c.Cleanup(cancel)

		err := postgres.Services().Session.Acquire(ctx, session, request(time.Minute))

		c.Assert(err, qt.ErrorIs, context.DeadlineExceeded)
		c.Assert(err, qt.Not(qt.ErrorIs), errHeld)
	})
}

func TestRelease_HappyPath(t *testing.T) {
	c := qt.New(t)
	s := &server{unlocked: true}
	_, session := open(c, s)

	err := postgres.Services().Session.Release(c.Context(), session, request(time.Second))

	c.Assert(err, qt.IsNil)
	c.Assert(s.unlocks.Load(), qt.Equals, int64(1))
	c.Assert(s.keys, qt.DeepEquals, []any{postgres.Key("ptah_test")})
}

func TestRelease_FailurePath(t *testing.T) {
	t.Run("the session did not hold the lock", func(t *testing.T) {
		c := qt.New(t)
		_, session := open(c, &server{unlocked: false})

		err := postgres.Services().Session.Release(c.Context(), session, request(time.Second))

		c.Assert(err, qt.ErrorMatches, `yugabytedb advisory lock was not held`)
	})

	t.Run("the statement failed", func(t *testing.T) {
		c := qt.New(t)
		failed := errors.New("connection reset")
		_, session := open(c, &server{err: failed})

		err := postgres.Services().Session.Release(c.Context(), session, request(time.Second))

		c.Assert(err, qt.ErrorIs, failed)
	})
}

// A direct server refuses the witness the lock, and a transaction pooler hands
// the witness the backend the lock was taken on, where the lock is reentrant.
// Measured through PgBouncer 1.25.2 in transaction mode: both handles acquired
// and both reported pg_backend_pid 113 (stokaro/ptah#1029).
func TestExcludes_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		server *server
	}{
		{name: "a direct server refuses the witness", server: &server{answers: []bool{false}}},
		{name: "a witness that cannot ask is no evidence", server: &server{err: errors.New("no route")}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			pool, _ := open(c, test.server)

			err := postgres.Services().Exclusion.Excludes(c.Context(), pool, request(time.Second))

			c.Assert(err, qt.IsNil)
			c.Assert(test.server.unlocks.Load(), qt.Equals, int64(0))
		})
	}
}

func TestExcludes_FailurePath(t *testing.T) {
	c := qt.New(t)
	s := &server{answers: []bool{true}, unlocked: true}
	pool, _ := open(c, s)

	err := postgres.Services().Exclusion.Excludes(c.Context(), pool, request(time.Second))

	c.Assert(err, qt.ErrorMatches, `yugabytedb advisory lock "ptah_test" excludes nothing on this connection: `+
		`a second connection took the same lock, which is what a transaction-pooling proxy such as PgBouncer `+
		`produces -- it hands both clients one backend session, where the lock is reentrant\. Two runs would `+
		`both believe they held it`)
	// The witness gave its own claim back, or the backend would keep a lock
	// nobody releases.
	c.Assert(s.unlocks.Load(), qt.Equals, int64(1))
}
