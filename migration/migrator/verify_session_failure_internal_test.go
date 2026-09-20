package migrator

// White-box testing required: sessionFailure decides whether a verification
// result may be reported at all, and the state it decides on -- a rollback or a
// discard that failed after the assertion already ran -- cannot be produced
// through the exported API. VerifyChecks takes a concrete
// *dbschema.DatabaseConnection, so no caller can hand it a session whose
// cleanup fails, and the nearest a live server gets is not close enough:
// terminating the backend mid-assertion (pg_terminate_backend on PostgreSQL)
// reports the assertion error alone, because database/sql marks the connection
// bad and both the rollback and the discard then return nothing.
//
// What holds the production path to this decision is that verifyOneCheck is
// its only caller and records the assertion error for it; removing the call
// leaves that record unused and the package does not build.

import (
	"errors"
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"
)

var (
	errAssertionRefused = errors.New("assertion refused by the server")
	errRollbackFailed   = errors.New("roll back transaction: connection reset")
)

// The assertion error is the callback's own signal, raised so the session ends
// rather than commits; it is already in the result and is not the run failing.
// Everything else the session reports is the isolation failing, and a report
// that says verified over one of those is a report about a database nobody
// proved was left alone.
func TestSessionFailure_HappyPath(t *testing.T) {
	t.Run("nothing failed", func(t *testing.T) {
		c := qt.New(t)
		c.Assert(sessionFailure(nil, nil), qt.IsNil)
	})

	t.Run("the assertion error alone is the result, not a failure", func(t *testing.T) {
		c := qt.New(t)
		wrapped := fmt.Errorf("isolated session: %w", errAssertionRefused)
		c.Assert(sessionFailure(wrapped, errAssertionRefused), qt.IsNil)
	})

	t.Run("the assertion error joined with nothing else", func(t *testing.T) {
		c := qt.New(t)
		joined := errors.Join(errAssertionRefused)
		c.Assert(sessionFailure(joined, errAssertionRefused), qt.IsNil)
	})
}

func TestSessionFailure_FailurePath(t *testing.T) {
	// The shape the wrapper actually produces: it joins the callback's error
	// with the rollback that failed after it. Asking whether the assertion
	// error is in there answers yes, which is how the second error goes
	// missing.
	t.Run("a cleanup failure beside the assertion error", func(t *testing.T) {
		c := qt.New(t)
		joined := errors.Join(errAssertionRefused, errRollbackFailed)

		failure := sessionFailure(joined, errAssertionRefused)

		c.Assert(failure, qt.ErrorIs, errRollbackFailed)
		c.Assert(failure, qt.Not(qt.ErrorIs), errAssertionRefused)
	})

	t.Run("a cleanup failure after an assertion that held", func(t *testing.T) {
		c := qt.New(t)

		failure := sessionFailure(errors.Join(errRollbackFailed), nil)

		c.Assert(failure, qt.ErrorIs, errRollbackFailed)
	})

	t.Run("a session that never opened", func(t *testing.T) {
		c := qt.New(t)
		opening := errors.New("pin isolated query session: pool is closed")

		failure := sessionFailure(opening, nil)

		c.Assert(failure, qt.Equals, opening)
	})
}
