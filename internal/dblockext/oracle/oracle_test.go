//go:build !js

package oracle_test

import (
	"context"
	"database/sql/driver"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/sijms/go-ora/v3/network"

	"ptah.run/internal/dblockext"
	"ptah.run/internal/dblockext/oracle"
	"ptah.run/internal/dbschema/dbtest"
)

// errHeld stands in for the timeout internal/dblock puts in a request.
var errHeld = errors.New("held by another session")

func request(timeout time.Duration) dblockext.Request {
	return dblockext.Request{Dialect: "oracle", Name: "ptah_test", Timeout: timeout, Held: errHeld}
}

// The answers the server gives the statement that locks the row.
const (
	locked       = "locked"
	busy         = "busy"
	noTable      = "no table"
	noRow        = "no row"
	statementErr = "statement failed"
)

var errStatement = errors.New("ORA-03113: end-of-file on communication channel")

// server answers each lock statement with the next of its answers, the last
// one again once they run out, and records each statement it was sent: the
// first word of an Exec, and "lock" with its argument for a lock statement.
type server struct {
	mu      sync.Mutex
	answers []string
	execErr error
	sent    []string
}

func (s *server) query(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sent = append(s.sent, "lock "+args[0].Value.(string))
	answer := s.answers[0]
	if len(s.answers) > 1 {
		s.answers = s.answers[1:]
	}
	switch answer {
	case locked:
		return dbtest.QueryResult{Columns: []string{"NAME"}, Rows: [][]driver.Value{{args[0].Value}}}, nil
	case busy:
		return dbtest.QueryResult{}, &network.OracleError{ErrCode: 54, ErrMsg: "ORA-00054: resource busy"}
	case noTable:
		return dbtest.QueryResult{}, &network.OracleError{ErrCode: 942, ErrMsg: "ORA-00942: table or view does not exist"}
	case noRow:
		return dbtest.QueryResult{Columns: []string{"NAME"}}, nil
	default:
		return dbtest.QueryResult{}, errStatement
	}
}

func (s *server) exec(query string, args []driver.NamedValue) (driver.Result, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	statement := strings.Fields(query)[0]
	if len(args) > 0 {
		statement += " " + args[0].Value.(string)
	}
	s.sent = append(s.sent, statement)
	if s.execErr != nil {
		return nil, s.execErr
	}
	return driver.RowsAffected(0), nil
}

func (s *server) statements() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.sent...)
}

func open(c *qt.C, s *server) dblockext.SQLSession {
	c.Helper()
	session, err := dbtest.OpenWithExec(c, s.query, s.exec).SQL.Conn(c.Context())
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = session.Close() })
	return dblockext.SQLSession{Conn: session}
}

func TestServices(t *testing.T) {
	c := qt.New(t)
	services := oracle.Services()
	c.Assert(services.Dialects, qt.DeepEquals, []string{"oracle"})
	c.Assert(services.Session, qt.IsNotNil)
	c.Assert(services.ReleasedByCommit, qt.IsTrue)
	c.Assert(services.Table, qt.Equals, "ptah_locks")
	c.Assert(services.Exclusion, qt.IsNil)
	c.Assert(services.Separate, qt.IsNil)
}

// A run that writes creates the row before it locks it, and asks again while
// another session holds the row.
func TestAcquire_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		answers  []string
		timeout  time.Duration
		wantSent []string
	}{
		{
			name:     "a free row",
			answers:  []string{locked},
			wantSent: []string{"DECLARE ptah_test", "lock ptah_test"},
		},
		{
			name:     "a row another session gives back",
			answers:  []string{busy, busy, locked},
			timeout:  time.Minute,
			wantSent: []string{"DECLARE ptah_test", "lock ptah_test", "lock ptah_test", "lock ptah_test"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			s := &server{answers: test.answers}

			err := oracle.Services().Session.Acquire(c.Context(), open(c, s), request(test.timeout))

			c.Assert(err, qt.IsNil)
			c.Assert(s.statements(), qt.DeepEquals, test.wantSent)
		})
	}
}

// A preview creates nothing. A name that has no row, or a schema with no lock
// table, has never been locked, so it goes on holding nothing.
func TestAcquire_PreviewWritesNothing(t *testing.T) {
	for _, answer := range []string{locked, noTable, noRow} {
		t.Run(answer, func(t *testing.T) {
			c := qt.New(t)
			s := &server{answers: []string{answer}}
			preview := request(time.Second)
			preview.Preview = true

			err := oracle.Services().Session.Acquire(c.Context(), open(c, s), preview)

			c.Assert(err, qt.IsNil)
			c.Assert(s.statements(), qt.DeepEquals, []string{"lock ptah_test"})
		})
	}
}

func TestAcquire_FailurePath(t *testing.T) {
	t.Run("held past the timeout", func(t *testing.T) {
		c := qt.New(t)
		s := &server{answers: []string{busy}}

		err := oracle.Services().Session.Acquire(c.Context(), open(c, s), request(100*time.Millisecond))

		c.Assert(err, qt.Equals, errHeld)
	})

	t.Run("held under no wait is asked once", func(t *testing.T) {
		c := qt.New(t)
		s := &server{answers: []string{busy}}

		err := oracle.Services().Session.Acquire(c.Context(), open(c, s), request(-1))

		c.Assert(err, qt.Equals, errHeld)
		c.Assert(s.statements(), qt.DeepEquals, []string{"DECLARE ptah_test", "lock ptah_test"})
	})

	t.Run("a row that went missing is a definite failure", func(t *testing.T) {
		c := qt.New(t)
		s := &server{answers: []string{noRow}}

		err := oracle.Services().Session.Acquire(c.Context(), open(c, s), request(time.Second))

		c.Assert(err, qt.ErrorMatches, `the row of lock "ptah_test" is missing from ptah_locks`)
		c.Assert(err, qt.Not(qt.ErrorAs), new(*dblockext.AmbiguousError))
	})

	t.Run("a failed lock statement is ambiguous", func(t *testing.T) {
		c := qt.New(t)
		s := &server{answers: []string{statementErr}}

		err := oracle.Services().Session.Acquire(c.Context(), open(c, s), request(time.Second))

		c.Assert(err, qt.ErrorIs, errStatement)
		c.Assert(err, qt.ErrorAs, new(*dblockext.AmbiguousError))
	})

	t.Run("a row that cannot be created stops before the lock statement", func(t *testing.T) {
		c := qt.New(t)
		refused := errors.New("ORA-01031: insufficient privileges")
		s := &server{answers: []string{locked}, execErr: refused}

		err := oracle.Services().Session.Acquire(c.Context(), open(c, s), request(time.Second))

		c.Assert(err, qt.ErrorIs, refused)
		c.Assert(err, qt.ErrorMatches, `create the row of lock "ptah_test" in ptah_locks: ORA-01031: insufficient privileges`)
		c.Assert(s.statements(), qt.DeepEquals, []string{"DECLARE ptah_test"})
	})

	t.Run("a canceled wait ends with the context", func(t *testing.T) {
		c := qt.New(t)
		s := &server{answers: []string{busy}}
		ctx, cancel := context.WithTimeout(c.Context(), 100*time.Millisecond)
		defer cancel()

		err := oracle.Services().Session.Acquire(ctx, open(c, s), request(0))

		c.Assert(err, qt.ErrorIs, context.DeadlineExceeded)
		c.Assert(err, qt.Not(qt.Equals), errHeld)
	})
}

// The session runs nothing but the lock, so ending its transaction gives the
// row back.
func TestRelease_HappyPath(t *testing.T) {
	c := qt.New(t)
	s := &server{answers: []string{locked}}

	err := oracle.Services().Session.Release(c.Context(), open(c, s), request(time.Second))

	c.Assert(err, qt.IsNil)
	c.Assert(s.statements(), qt.DeepEquals, []string{"ROLLBACK"})
}

func TestRelease_FailurePath(t *testing.T) {
	c := qt.New(t)
	s := &server{answers: []string{locked}, execErr: errStatement}

	err := oracle.Services().Session.Release(c.Context(), open(c, s), request(time.Second))

	c.Assert(err, qt.ErrorIs, errStatement)
}

// The timeout bounds the creation of the row as well as the wait for its
// lock: a creation that outlasts it is the lock another session holds, not a
// wait without end.
func TestAcquire_RowCreationRunsUnderTheTimeout(t *testing.T) {
	c := qt.New(t)
	db := dbtest.OpenWithExec(c,
		func(string, []driver.NamedValue) (dbtest.QueryResult, error) {
			return dbtest.QueryResult{}, errStatement
		},
		func(string, []driver.NamedValue) (driver.Result, error) {
			time.Sleep(200 * time.Millisecond)
			return nil, context.DeadlineExceeded
		},
	)
	session, err := db.SQL.Conn(c.Context())
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = session.Close() })

	err = oracle.Services().Session.Acquire(c.Context(), dblockext.SQLSession{Conn: session}, request(50*time.Millisecond))

	c.Assert(err, qt.ErrorIs, errHeld)
	c.Assert(err, qt.ErrorAs, new(*dblockext.AmbiguousError))
}

// The lock table lives in the schema the request names, which the migrator
// sets to the schema its revision table is configured into, so that every
// account recording there locks one row. The schema is quoted as the revision
// table's is; with none, the table is the session's own.
func TestAcquire_LocksTheTableOfTheRequestSchema(t *testing.T) {
	tests := []struct {
		name        string
		schema      string
		wantCreated string
		wantLocked  string
	}{
		{
			name:        "the session's current schema",
			wantCreated: "ptah_locks",
			wantLocked:  "SELECT name FROM ptah_locks WHERE name = :1 FOR UPDATE NOWAIT",
		},
		{
			name:        "a configured schema",
			schema:      "APP",
			wantCreated: `"APP".ptah_locks`,
			wantLocked:  `SELECT name FROM "APP".ptah_locks WHERE name = :1 FOR UPDATE NOWAIT`,
		},
		{
			name:        "a quote in the schema name",
			schema:      `A"B`,
			wantCreated: `"A""B".ptah_locks`,
			wantLocked:  `SELECT name FROM "A""B".ptah_locks WHERE name = :1 FOR UPDATE NOWAIT`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var mu sync.Mutex
			var created, lockedBy string
			db := dbtest.OpenWithExec(c,
				func(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
					mu.Lock()
					defer mu.Unlock()
					lockedBy = query
					return dbtest.QueryResult{Columns: []string{"NAME"}, Rows: [][]driver.Value{{args[0].Value}}}, nil
				},
				func(_ string, args []driver.NamedValue) (driver.Result, error) {
					mu.Lock()
					defer mu.Unlock()
					created = args[1].Value.(string)
					return driver.RowsAffected(0), nil
				},
			)
			session, err := db.SQL.Conn(c.Context())
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { _ = session.Close() })
			req := request(time.Second)
			req.Schema = test.schema

			err = oracle.Services().Session.Acquire(c.Context(), dblockext.SQLSession{Conn: session}, req)

			c.Assert(err, qt.IsNil)
			mu.Lock()
			defer mu.Unlock()
			c.Assert(created, qt.Equals, test.wantCreated)
			c.Assert(lockedBy, qt.Equals, test.wantLocked)
		})
	}
}
