package mysqlreplay_test

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/dataext"
	"ptah.run/internal/devclean/replay"
)

// reportedCharset is a dev session that answers every query with one value,
// its client character set, and counts the queries it is asked.
type reportedCharset struct {
	charset string
	err     error
	asked   *int
}

func (s reportedCharset) QueryRowContext(context.Context, string, ...any) dataext.Row {
	*s.asked++
	return charsetRow(s)
}

type charsetRow reportedCharset

func (r charsetRow) Scan(dest ...any) error {
	*dest[0].(*string) = r.charset
	return r.err
}

func (r charsetRow) Err() error { return r.err }

// sessionGuardFor is the guard of dialect as a guard that reads the session.
func sessionGuardFor(c *qt.C, dialect string) replay.SessionGuard {
	c.Helper()
	reader, ok := guardFor(c, dialect).(replay.SessionGuard)
	c.Assert(ok, qt.IsTrue)
	return reader
}

// A session whose client character set is one of the UTF-8 family reads a
// statement's bytes as the guard does, in any realm.
func TestGuard_ValidateSession_HappyPath(t *testing.T) {
	for _, dialect := range []string{"mysql", "mariadb"} {
		for _, charset := range []string{"utf8mb4", "utf8mb3", "utf8", "UTF8MB4"} {
			t.Run(dialect+"/"+charset, func(t *testing.T) {
				c := qt.New(t)
				asked := new(int)
				session := reportedCharset{charset: charset, asked: asked}

				err := sessionGuardFor(c, dialect).ValidateSession(c.Context(), session, replay.Scope{Database: "ptah_dev", Server: true})

				c.Assert(err, qt.IsNil)
				c.Assert(*asked, qt.Equals, 1)
			})
		}
	}
}

// A session whose client character set is outside the UTF-8 family is
// refused in every realm, owned server included: the guard would misread
// what it still refuses there.
func TestGuard_ValidateSession_FailurePath(t *testing.T) {
	scopes := []struct {
		name  string
		scope replay.Scope
	}{
		{name: "database", scope: replay.Scope{Database: "ptah_dev"}},
		{name: "server databases", scope: replay.Scope{Databases: true}},
		{name: "provisioned server", scope: replay.Scope{Database: "ptah_dev", Server: true, Provisioned: true}},
	}
	for _, dialect := range []string{"mysql", "mariadb"} {
		for _, charset := range []string{"latin1", "gbk", "binary"} {
			for _, scope := range scopes {
				t.Run(dialect+"/"+charset+"/"+scope.name, func(t *testing.T) {
					c := qt.New(t)
					session := reportedCharset{charset: charset, asked: new(int)}

					err := sessionGuardFor(c, dialect).ValidateSession(c.Context(), session, scope.scope)

					c.Assert(err, qt.ErrorMatches, `the dev session's client character set is "`+charset+`", and the `+dialect+
						` replay guard reads each statement as UTF-8: the server reads the same bytes as `+charset+
						` and can run what the guard does not see; connect with a utf8mb4 client character set, `+
						`such as charset=utf8mb4 in the dev URL`)
				})
			}
		}
	}
}

var errSessionGone = errors.New("session gone")

// A session the guard cannot read is an error, not a session it accepts.
func TestGuard_ValidateSession_FailurePathUnreadableSession(t *testing.T) {
	c := qt.New(t)
	session := reportedCharset{err: errSessionGone, asked: new(int)}

	err := sessionGuardFor(c, "mysql").ValidateSession(c.Context(), session, replay.Scope{Database: "ptah_dev"})

	c.Assert(err, qt.ErrorIs, errSessionGone)
	c.Assert(err, qt.ErrorMatches, `read the dev session's client character set: session gone`)
}

// A run that trusts its SQL is not refused, and its session is not read.
func TestGuard_ValidateSession_TrustedSQLReadsNoSession(t *testing.T) {
	for _, dialect := range []string{"mysql", "mariadb"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			asked := new(int)
			session := reportedCharset{charset: "latin1", err: errSessionGone, asked: asked}

			err := sessionGuardFor(c, dialect).ValidateSession(c.Context(), session, replay.Scope{Database: "ptah_dev", TrustedSQL: true})

			c.Assert(err, qt.IsNil)
			c.Assert(*asked, qt.Equals, 0)
		})
	}
}
