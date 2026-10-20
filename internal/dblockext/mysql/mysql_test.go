package mysql_test

import (
	"database/sql/driver"
	"errors"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dblockext"
	"ptah.run/internal/dblockext/mysql"
	"ptah.run/internal/dbschema/dbtest"
)

// errHeld stands in for the timeout internal/dblock puts in a request.
var errHeld = errors.New("held by another session")

func request(timeout time.Duration) dblockext.Request {
	return dblockext.Request{Dialect: "mysql", Name: "ptah_test", Timeout: timeout, Held: errHeld}
}

// server answers GET_LOCK and RELEASE_LOCK with value, or fails with err, and
// records the arguments each statement sent.
type server struct {
	value driver.Value
	err   error
	sent  [][]any
}

func (s *server) answer(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
	values := make([]any, 0, len(args))
	for _, arg := range args {
		values = append(values, arg.Value)
	}
	s.sent = append(s.sent, append([]any{strings.TrimSpace(query)}, values...))
	if s.err != nil {
		return dbtest.QueryResult{}, s.err
	}
	return dbtest.QueryResult{Columns: []string{"result"}, Rows: [][]driver.Value{{s.value}}}, nil
}

func open(c *qt.C, s *server) dblockext.SQLSession {
	c.Helper()
	session, err := dbtest.Open(c, s.answer).SQL.Conn(c.Context())
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = session.Close() })
	return dblockext.SQLSession{Conn: session}
}

func TestServices(t *testing.T) {
	c := qt.New(t)
	c.Assert(mysql.MySQL().Dialects, qt.DeepEquals, []string{"mysql"})
	c.Assert(mysql.MariaDB().Dialects, qt.DeepEquals, []string{"mariadb"})
	for _, services := range []dblockext.Services{mysql.MySQL(), mysql.MariaDB()} {
		c.Assert(services.Session, qt.IsNotNil)
		c.Assert(services.Exclusion, qt.IsNil)
		c.Assert(services.Separate, qt.IsNil)
	}
}

// The wait is sent in whole seconds: a positive timeout rounded up, zero for
// one that does not wait, and for one without limit -1 on MySQL and one year
// on MariaDB, which refuses a negative GET_LOCK timeout.
func TestAcquire_HappyPath(t *testing.T) {
	tests := []struct {
		name        string
		services    dblockext.Services
		timeout     time.Duration
		wantSeconds float64
	}{
		{name: "mysql without limit", services: mysql.MySQL(), wantSeconds: -1},
		{name: "mariadb without limit", services: mysql.MariaDB(), wantSeconds: 31_536_000},
		{name: "a fraction of a second rounds up", services: mysql.MySQL(), timeout: 500 * time.Millisecond, wantSeconds: 1},
		{name: "whole seconds", services: mysql.MariaDB(), timeout: 2 * time.Second, wantSeconds: 2},
		{name: "no wait", services: mysql.MariaDB(), timeout: -1, wantSeconds: 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			s := &server{value: int64(1)}
			session := open(c, s)

			err := test.services.Session.Acquire(c.Context(), session, request(test.timeout))

			c.Assert(err, qt.IsNil)
			c.Assert(s.sent, qt.DeepEquals, [][]any{{"SELECT GET_LOCK(?, ?)", "ptah_test", test.wantSeconds}})
		})
	}
}

func TestAcquire_FailurePath(t *testing.T) {
	t.Run("held past the timeout", func(t *testing.T) {
		c := qt.New(t)
		session := open(c, &server{value: int64(0)})

		err := mysql.MySQL().Session.Acquire(c.Context(), session, request(time.Second))

		c.Assert(err, qt.Equals, errHeld)
	})

	t.Run("a NULL answer is a definite failure", func(t *testing.T) {
		c := qt.New(t)
		session := open(c, &server{value: nil})

		err := mysql.MySQL().Session.Acquire(c.Context(), session, request(time.Second))

		c.Assert(err, qt.ErrorMatches, `GET_LOCK\("ptah_test"\) returned NULL`)
		c.Assert(err, qt.Not(qt.ErrorAs), new(*dblockext.AmbiguousError))
	})

	t.Run("a lost answer is ambiguous", func(t *testing.T) {
		c := qt.New(t)
		lost := errors.New("GET_LOCK response lost")
		session := open(c, &server{err: lost})

		err := mysql.MariaDB().Session.Acquire(c.Context(), session, request(time.Second))

		c.Assert(err, qt.ErrorIs, lost)
		c.Assert(err, qt.ErrorAs, new(*dblockext.AmbiguousError))
	})
}

func TestRelease_HappyPath(t *testing.T) {
	c := qt.New(t)
	s := &server{value: int64(1)}
	session := open(c, s)

	err := mysql.MySQL().Session.Release(c.Context(), session, request(time.Second))

	c.Assert(err, qt.IsNil)
	c.Assert(s.sent, qt.DeepEquals, [][]any{{"SELECT RELEASE_LOCK(?)", "ptah_test"}})
}

func TestRelease_FailurePath(t *testing.T) {
	failed := errors.New("connection reset")
	tests := []struct {
		name    string
		server  *server
		wantErr string
	}{
		{name: "the session did not hold the lock", server: &server{value: nil}, wantErr: `mysql advisory lock was not held`},
		{name: "another session holds the lock", server: &server{value: int64(0)}, wantErr: `mysql advisory lock was not released`},
		{name: "the statement failed", server: &server{err: failed}, wantErr: `connection reset`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			session := open(c, test.server)

			err := mysql.MariaDB().Session.Release(c.Context(), session, request(time.Second))

			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}
