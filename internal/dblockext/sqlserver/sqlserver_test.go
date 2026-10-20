package sqlserver_test

import (
	"database/sql/driver"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dblockext"
	"ptah.run/internal/dblockext/sqlserver"
	"ptah.run/internal/dbschema/dbtest"
)

// errHeld stands in for the timeout internal/dblock puts in a request.
var errHeld = errors.New("held by another session")

func request(timeout time.Duration) dblockext.Request {
	return dblockext.Request{Dialect: "sqlserver", Name: "ptah_test", Timeout: timeout, Held: errHeld}
}

// server answers sp_getapplock and sp_releaseapplock with result, or fails
// with err, and records the procedure each statement ran and its arguments.
type server struct {
	result int64
	err    error
	sent   [][]any
}

func (s *server) answer(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
	procedure := "sp_getapplock"
	if strings.Contains(query, "sp_releaseapplock") {
		procedure = "sp_releaseapplock"
	}
	sent := []any{procedure}
	for _, arg := range args {
		sent = append(sent, arg.Value)
	}
	s.sent = append(s.sent, sent)
	if s.err != nil {
		return dbtest.QueryResult{}, s.err
	}
	return dbtest.QueryResult{Columns: []string{""}, Rows: [][]driver.Value{{s.result}}}, nil
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
	services := sqlserver.Services()
	c.Assert(services.Dialects, qt.DeepEquals, []string{"sqlserver"})
	c.Assert(services.Session, qt.IsNotNil)
	c.Assert(services.Exclusion, qt.IsNil)
	c.Assert(services.Separate, qt.IsNil)
}

// The wait is sent in milliseconds: zero for one that does not wait, -1 for
// one without limit, and a positive timeout rounded up, at most the largest
// INT. 0 and 1 both answer that the lock was granted.
func TestAcquire_HappyPath(t *testing.T) {
	tests := []struct {
		name       string
		timeout    time.Duration
		result     int64
		wantMillis int64
	}{
		{name: "without limit", wantMillis: -1},
		{name: "no wait", timeout: -1, wantMillis: 0},
		{name: "a fraction of a millisecond rounds up", timeout: time.Nanosecond, wantMillis: 1},
		{name: "milliseconds", timeout: 1500 * time.Millisecond, result: 1, wantMillis: 1500},
		{name: "capped at the largest INT", timeout: time.Duration(math.MaxInt32+1) * time.Millisecond, wantMillis: math.MaxInt32},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			s := &server{result: test.result}
			session := open(c, s)

			err := sqlserver.Services().Session.Acquire(c.Context(), session, request(test.timeout))

			c.Assert(err, qt.IsNil)
			c.Assert(s.sent, qt.DeepEquals, [][]any{{"sp_getapplock", "ptah_test", test.wantMillis}})
		})
	}
}

func TestAcquire_FailurePath(t *testing.T) {
	t.Run("held past the timeout", func(t *testing.T) {
		c := qt.New(t)
		session := open(c, &server{result: -1})

		err := sqlserver.Services().Session.Acquire(c.Context(), session, request(time.Second))

		c.Assert(err, qt.Equals, errHeld)
	})

	t.Run("another return code is a definite failure", func(t *testing.T) {
		c := qt.New(t)
		session := open(c, &server{result: -3})

		err := sqlserver.Services().Session.Acquire(c.Context(), session, request(time.Second))

		c.Assert(err, qt.ErrorMatches, `sqlserver sp_getapplock\("ptah_test"\) failed with return code -3`)
		c.Assert(err, qt.Not(qt.ErrorAs), new(*dblockext.AmbiguousError))
	})

	t.Run("a lost answer is ambiguous", func(t *testing.T) {
		c := qt.New(t)
		lost := errors.New("sp_getapplock response lost")
		session := open(c, &server{err: lost})

		err := sqlserver.Services().Session.Acquire(c.Context(), session, request(time.Second))

		c.Assert(err, qt.ErrorIs, lost)
		c.Assert(err, qt.ErrorAs, new(*dblockext.AmbiguousError))
	})
}

func TestRelease_HappyPath(t *testing.T) {
	c := qt.New(t)
	s := &server{result: 0}
	session := open(c, s)

	err := sqlserver.Services().Session.Release(c.Context(), session, request(time.Second))

	c.Assert(err, qt.IsNil)
	c.Assert(s.sent, qt.DeepEquals, [][]any{{"sp_releaseapplock", "ptah_test"}})
}

func TestRelease_FailurePath(t *testing.T) {
	failed := errors.New("connection reset")
	tests := []struct {
		name    string
		server  *server
		wantErr string
	}{
		{name: "the server refused", server: &server{result: -999}, wantErr: `sqlserver sp_releaseapplock\("ptah_test"\) failed with return code -999`},
		{name: "the statement failed", server: &server{err: failed}, wantErr: `connection reset`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			session := open(c, test.server)

			err := sqlserver.Services().Session.Release(c.Context(), session, request(time.Second))

			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}
