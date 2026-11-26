package mysqlreplay_test

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/dataext"
	"ptah.run/internal/devclean/replay"
)

// reportedSession is a dev session that answers every query with one row: its
// client character set, the account CURRENT_USER() names and the JSON array of
// the roles that account holds. It counts the queries it is asked.
type reportedSession struct {
	charset, account, roles string
	err                     error
	asked                   *int
}

func (s reportedSession) QueryRowContext(context.Context, string, ...any) dataext.Row {
	*s.asked++
	return sessionRow(s)
}

type sessionRow reportedSession

func (r sessionRow) Scan(dest ...any) error {
	for index, value := range []string{r.charset, r.account, r.roles} {
		*dest[index].(*string) = value
	}
	return r.err
}

func (r sessionRow) Err() error { return r.err }

// utf8Session is a session that reads statements as the guard does, run by
// account and holding roles.
func utf8Session(account, roles string) reportedSession {
	return reportedSession{charset: "utf8mb4", account: account, roles: roles, asked: new(int)}
}

// sessionGuardFor is the guard of dialect as a guard that reads the session.
func sessionGuardFor(c *qt.C, dialect string) replay.SessionGuard {
	c.Helper()
	reader, ok := guardFor(c, dialect).(replay.SessionGuard)
	c.Assert(ok, qt.IsTrue)
	return reader
}

// boundGuard is the guard dialect returns for session, which it reads under
// a scope that accepts any character set.
func boundGuard(c *qt.C, dialect string, session reportedSession) replay.Guard {
	c.Helper()
	guard, err := sessionGuardFor(c, dialect).ValidateSession(c.Context(), session, replay.Scope{TrustedSQL: true})
	c.Assert(err, qt.IsNil)
	return guard
}

// A session whose client character set is one of the UTF-8 family reads a
// statement's bytes as the guard does, in any realm, and is read once.
func TestGuard_ValidateSession_HappyPath(t *testing.T) {
	for _, dialect := range []string{"mysql", "mariadb"} {
		for _, charset := range []string{"utf8mb4", "utf8mb3", "utf8", "UTF8MB4"} {
			t.Run(dialect+"/"+charset, func(t *testing.T) {
				c := qt.New(t)
				asked := new(int)
				session := reportedSession{charset: charset, account: "dev@%", roles: "[]", asked: asked}

				guard, err := sessionGuardFor(c, dialect).ValidateSession(c.Context(), session, replay.Scope{Database: "ptah_dev", Server: true})

				c.Assert(err, qt.IsNil)
				c.Assert(guard, qt.IsNotNil)
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
					session := reportedSession{charset: charset, account: "dev@%", roles: "[]", asked: new(int)}

					guard, err := sessionGuardFor(c, dialect).ValidateSession(c.Context(), session, scope.scope)

					var refusal *replay.SessionRefusal
					c.Assert(err, qt.ErrorAs, &refusal)
					c.Assert(err, qt.ErrorMatches, `the dev session's client character set is "`+charset+`", and the `+dialect+
						` replay guard reads each statement as UTF-8: the server reads the same bytes as `+charset+
						` and can run what the guard does not see; connect with a utf8mb4 client character set, `+
						`such as charset=utf8mb4 in the dev URL`)
					c.Assert(guard, qt.IsNil)
				})
			}
		}
	}
}

var errSessionGone = errors.New("session gone")

// A session the guard cannot read is an error, not a session it accepts, in
// every scope: under trust too, since the account it runs as decides refusals
// trust keeps.
func TestGuard_ValidateSession_FailurePathUnreadableSession(t *testing.T) {
	scopes := []struct {
		name  string
		scope replay.Scope
	}{
		{name: "database", scope: replay.Scope{Database: "ptah_dev"}},
		{name: "database, trusted", scope: replay.Scope{Database: "ptah_dev", TrustedSQL: true}},
	}
	for _, scope := range scopes {
		t.Run(scope.name, func(t *testing.T) {
			c := qt.New(t)
			session := reportedSession{err: errSessionGone, asked: new(int)}

			guard, err := sessionGuardFor(c, "mysql").ValidateSession(c.Context(), session, scope.scope)

			c.Assert(err, qt.ErrorIs, errSessionGone)
			c.Assert(err, qt.ErrorMatches, `read the dev session's client character set and account: session gone`)
			var refusal *replay.SessionRefusal
			c.Assert(err, qt.Not(qt.ErrorAs), &refusal)
			c.Assert(guard, qt.IsNil)
		})
	}
}

// An answer that names no account the guard can read is an error too.
func TestGuard_ValidateSession_FailurePathUnreadableAccount(t *testing.T) {
	tests := []struct {
		name, account, roles, wantErr string
	}{
		{name: "account without a host", account: "dev", roles: "[]",
			wantErr: `read the dev session's account: CURRENT_USER\(\) answered "dev", which names no host`},
		{name: "roles that are not a JSON array of names", account: "dev@%", roles: `{"r": 1}`,
			wantErr: `read the dev session's account: read the roles the account holds from "{\\"r\\": 1}": .*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			session := reportedSession{charset: "utf8mb4", account: test.account, roles: test.roles, asked: new(int)}

			guard, err := sessionGuardFor(c, "mariadb").ValidateSession(c.Context(), session, replay.Scope{Database: "ptah_dev"})

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(guard, qt.IsNil)
		})
	}
}

// A run that trusts its SQL is not refused for its character set. Its session
// is still read once, for the account it runs as.
func TestGuard_ValidateSession_TrustedSQLAcceptsAnyCharacterSet(t *testing.T) {
	for _, dialect := range []string{"mysql", "mariadb"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			asked := new(int)
			session := reportedSession{charset: "latin1", account: "dev@%", roles: "[]", asked: asked}

			guard, err := sessionGuardFor(c, dialect).ValidateSession(c.Context(), session, replay.Scope{Database: "ptah_dev", TrustedSQL: true})

			c.Assert(err, qt.IsNil)
			c.Assert(guard, qt.IsNotNil)
			c.Assert(*asked, qt.Equals, 1)
		})
	}
}
