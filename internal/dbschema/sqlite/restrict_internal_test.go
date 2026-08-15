package sqlite

// White-box testing required: verifyAttachRefused is the fail-closed half of
// the restriction — the part that turns "we called Limit" into "the engine
// really refuses ATTACH". Its unhappy branches are unreachable through the
// exported API, because RestrictSession applies the limit first, so they are
// exercised here directly.

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"reflect"
	"testing"

	qt "github.com/frankban/quicktest"
	sqlitedriver "modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

// sqliteSession opens a probe database and hands back a session on it. Both
// are closed for the caller, in the right order.
//
// The session has to be closed and not only the *sql.DB: sql.DB.Close() closes
// idle connections and marks the pool closed, but a Conn already checked out
// keeps its driver connection -- and therefore the database file -- open until
// its own Close. Every caller here closed only the DB, so the file outlived the
// test.
//
// On Unix that is invisible: an open file can be unlinked, and t.TempDir's
// cleanup succeeded. Windows refuses to delete an open file, so it reported
// "TempDir RemoveAll cleanup: unlinkat ...", which is how the leak was found.
// A double Close on the session is harmless -- it returns sql.ErrConnDone --
// so the caller that closes it deliberately keeps working.
func sqliteSession(c *qt.C) *sql.Conn {
	c.Helper()
	db, err := sql.Open("sqlite", filepath.Join(c.TB.TempDir(), "probe.db"))
	c.Assert(err, qt.IsNil)
	db.SetMaxOpenConns(1)
	session, err := db.Conn(context.Background())
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_ = session.Close()
		c.Check(db.Close(), qt.IsNil)
	})
	return session
}

func TestVerifyAttachRefusedAcceptsTheLimitError(t *testing.T) {
	c := qt.New(t)
	session := sqliteSession(c)
	_, err := sqlitedriver.Limit(session, sqlite3.SQLITE_LIMIT_ATTACHED, 0)
	c.Assert(err, qt.IsNil)

	c.Assert(verifyAttachRefused(context.Background(), session), qt.IsNil)
}

func TestVerifyAttachRefusedRejectsAnUnrestrictedSession(t *testing.T) {
	c := qt.New(t)
	session := sqliteSession(c)

	// No limit applied: the ATTACH succeeds, which means the restriction is
	// not in force and the caller must not proceed.
	err := verifyAttachRefused(context.Background(), session)

	c.Assert(err, qt.ErrorMatches,
		`sqlite session restriction did not take effect: ATTACH still succeeds on the restricted session`)
}

func TestVerifyAttachRefusedRejectsAnUnrelatedFailure(t *testing.T) {
	c := qt.New(t)
	session := sqliteSession(c)
	c.Assert(session.Close(), qt.IsNil)

	// The ATTACH fails, but not because of the attached-database limit. A
	// failure for any other reason proves nothing about the restriction, so it
	// must not be read as success.
	err := verifyAttachRefused(context.Background(), session)

	c.Assert(err, qt.ErrorMatches,
		`sqlite session restriction could not be confirmed: the verification ATTACH failed without the expected "too many attached databases" error.*`)
}

func TestRestrictSessionRequiresASession(t *testing.T) {
	c := qt.New(t)

	c.Assert(RestrictSession(context.Background(), nil), qt.ErrorMatches,
		`sqlite session restriction requires a pinned session`)
}

// TestVerifyRestrictionDefaultsToTheAttachProbe pins what the seam is bound
// to. Without this, rebinding verifyRestriction to a no-op would satisfy every
// other test: they prove RestrictSession consults *a* hook, not that the hook
// is the ATTACH probe that makes the restriction real.
func TestVerifyRestrictionDefaultsToTheAttachProbe(t *testing.T) {
	c := qt.New(t)

	c.Assert(reflect.ValueOf(verifyRestriction).Pointer(), qt.Equals,
		reflect.ValueOf(verifyAttachRefused).Pointer())
}

// TestRestrictSessionConsultsTheVerification pins that applying the limit is
// not the whole job: RestrictSession must confirm the restriction took effect
// and propagate a failed confirmation. On a session where the limit did work,
// verifying and skipping verification are indistinguishable, so the check is
// made observable here.
func TestRestrictSessionConsultsTheVerification(t *testing.T) {
	c := qt.New(t)
	session := sqliteSession(c)
	sentinel := errors.New("verification refused this session")
	original := verifyRestriction
	verifyRestriction = func(context.Context, *sql.Conn) error { return sentinel }
	c.Cleanup(func() { verifyRestriction = original })

	err := RestrictSession(context.Background(), session)

	c.Assert(err, qt.ErrorIs, sentinel)
}
