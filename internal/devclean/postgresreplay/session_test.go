package postgresreplay_test

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/dataext"
	"ptah.run/internal/devclean/replay"
)

// reportedSession is a dev session that answers every query with its client
// encoding, its standard_conforming_strings, the names of its catalog, the
// schemas of its search path, its rules and its database, and counts the
// queries it is asked. A zero catalog names nothing, a zero path is a search
// path that names no schema, zero rules are none, and a zero database is
// ptah_dev.
type reportedSession struct {
	encoding, conforming, catalog, rules, database string
	path                                           []string
	err                                            error
	asked                                          *int
}

func (s reportedSession) QueryRowContext(context.Context, string, ...any) dataext.Row {
	*s.asked++
	return sessionRow(s)
}

type sessionRow reportedSession

func (r sessionRow) Scan(dest ...any) error {
	*dest[0].(*string) = r.encoding
	*dest[1].(*string) = r.conforming
	*dest[2].(*string) = cmp.Or(r.catalog, "[]")
	path, err := json.Marshal(r.path)
	*dest[3].(*string) = string(path)
	*dest[4].(*string) = cmp.Or(r.rules, "[]")
	*dest[5].(*string) = cmp.Or(r.database, "ptah_dev")
	return cmp.Or(r.err, err)
}

func (r sessionRow) Err() error { return r.err }

// sessionGuardFor is the guard of dialect as a guard that reads the session.
func sessionGuardFor(c *qt.C, dialect string) replay.SessionGuard {
	c.Helper()
	reader, ok := guardFor(c, dialect).(replay.SessionGuard)
	c.Assert(ok, qt.IsTrue)
	return reader
}

var postgresFamily = []string{"postgres", "cockroachdb", "yugabytedb"}

// A session that reads statements as UTF-8 with standard-conforming strings
// reads them as the guard does, in any realm, and is asked once. The server
// reports the encoding as UTF8; the spellings it accepts for it are read the
// same.
func TestGuard_ValidateSession_HappyPath(t *testing.T) {
	for _, dialect := range postgresFamily {
		for _, encoding := range []string{"UTF8", "utf8", "UTF-8", "UNICODE"} {
			t.Run(dialect+"/"+encoding, func(t *testing.T) {
				c := qt.New(t)
				asked := new(int)
				session := reportedSession{encoding: encoding, conforming: "on", asked: asked}

				got, err := sessionGuardFor(c, dialect).ValidateSession(c.Context(), session, replay.Scope{Database: "ptah_dev"})

				c.Assert(err, qt.IsNil)
				c.Assert(got, qt.IsNotNil)
				c.Assert(*asked, qt.Equals, 1)
			})
		}
	}
}

// A run that trusts its SQL is not refused for its encoding or its strings:
// trust lifts a DO block, which can change either setting. The session is read
// all the same, for the catalog's names, and the guard it returns reads them:
// a mutation of pg_catalog stays refused under trust.
func TestGuard_ValidateSession_HappyPathTrusted(t *testing.T) {
	c := qt.New(t)
	asked := new(int)
	session := reportedSession{encoding: "GBK", conforming: "off", catalog: `[["f", "pg_catalog", ["now"]]]`, asked: asked}
	trusted := replay.Scope{Database: "ptah_dev", TrustedSQL: true}

	got, err := sessionGuardFor(c, "postgres").ValidateSession(c.Context(), session, trusted)

	c.Assert(err, qt.IsNil)
	c.Assert(*asked, qt.Equals, 1)
	c.Assert(got.Validate(`COMMENT ON FUNCTION now() IS 'x'`, trusted), qt.ErrorMatches,
		`postgres migration replay rejects protected namespace "pg_catalog" mutation through the unqualified name "now" because .*`)
}

// A session that reads statements in another encoding, or with backslash
// escapes in plain string literals, is refused in every realm, owned and
// provisioned servers included: the guard would misread what it still refuses
// there.
func TestGuard_ValidateSession_FailurePath(t *testing.T) {
	sessions := []struct {
		name                 string
		encoding, conforming string
	}{
		{name: "GBK", encoding: "GBK", conforming: "on"},
		{name: "SQL_ASCII", encoding: "SQL_ASCII", conforming: "on"},
		{name: "backslash escapes", encoding: "UTF8", conforming: "off"},
	}
	scopes := []struct {
		name  string
		scope replay.Scope
	}{
		{name: "database", scope: replay.Scope{Database: "ptah_dev"}},
		{name: "server", scope: replay.Scope{Database: "ptah_dev", Server: true}},
		{name: "provisioned server", scope: replay.Scope{Database: "ptah_dev", Server: true, Provisioned: true}},
	}
	for _, dialect := range postgresFamily {
		for _, reported := range sessions {
			for _, scope := range scopes {
				t.Run(dialect+"/"+reported.name+"/"+scope.name, func(t *testing.T) {
					c := qt.New(t)
					session := reportedSession{encoding: reported.encoding, conforming: reported.conforming, asked: new(int)}

					got, err := sessionGuardFor(c, dialect).ValidateSession(c.Context(), session, scope.scope)

					c.Assert(got, qt.IsNil)
					var refusal *replay.SessionRefusal
					c.Assert(err, qt.ErrorAs, &refusal)
					c.Assert(err, qt.ErrorIs, replay.ErrRefused)
					c.Assert(err, qt.ErrorMatches, `the dev session reads statements with client_encoding "`+reported.encoding+
						`" and standard_conforming_strings "`+reported.conforming+`", and the `+dialect+` replay guard reads `+
						`each statement as UTF-8 with standard-conforming strings: the server reads the same bytes by other `+
						`rules and can run what the guard does not see; connect with client_encoding UTF8 and `+
						`standard_conforming_strings on`)
				})
			}
		}
	}
}

var errSessionGone = errors.New("session gone")

// A session the guard cannot read is an error, not a session it accepts.
func TestGuard_ValidateSession_FailurePathUnreadableSession(t *testing.T) {
	c := qt.New(t)
	session := reportedSession{err: errSessionGone, asked: new(int)}

	got, err := sessionGuardFor(c, "postgres").ValidateSession(c.Context(), session, replay.Scope{Database: "ptah_dev"})

	c.Assert(got, qt.IsNil)
	c.Assert(err, qt.ErrorIs, errSessionGone)
	c.Assert(err, qt.ErrorMatches, `read the dev session's client_encoding, standard_conforming_strings, catalog names, search path, rules and database: session gone`)
	var refusal *replay.SessionRefusal
	c.Assert(err, qt.Not(qt.ErrorAs), &refusal)
}

// A catalog answer the guard cannot read is an error, not a catalog that names
// nothing: a guard that took it for one would fall back to the rule it reads
// without a session.
func TestGuard_ValidateSession_FailurePathUnreadableCatalog(t *testing.T) {
	c := qt.New(t)
	session := reportedSession{encoding: "UTF8", conforming: "on", catalog: `{"r": []}`, asked: new(int)}

	got, err := sessionGuardFor(c, "postgres").ValidateSession(c.Context(), session, replay.Scope{Database: "ptah_dev"})

	c.Assert(got, qt.IsNil)
	c.Assert(err, qt.ErrorMatches, `read the dev session's catalog names: json: cannot unmarshal object .*`)
}
