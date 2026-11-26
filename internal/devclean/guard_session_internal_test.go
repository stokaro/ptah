package devclean

// White-box testing required: validateSession runs only inside
// NewDevReplayGuardFor, which needs a live connection, and the cases under
// test are a session read that fails, which no live server can be made to do
// on demand, and the guard the session read returns, which only the guard
// field shows apart from the statements a server would refuse. These give the
// guard a session guard that answers as the test says.

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/devclean/replay"
)

// answeringSessionGuard is a replay guard that reads the session and answers
// with err, except under trust, where it accepts. It returns bound for a
// session it accepts.
type answeringSessionGuard struct {
	err   error
	bound replay.Guard
}

func (answeringSessionGuard) Validate(string, replay.Scope) error { return nil }

func (g answeringSessionGuard) ValidateSession(_ context.Context, _ replay.Session, scope replay.Scope) (replay.Guard, error) {
	if scope.TrustedSQL {
		return g.bound, nil
	}
	return nil, g.err
}

// boundSessionGuard is the guard a session read returns, named so a test can
// tell it from the guard that read the session.
type boundSessionGuard struct {
	name string
}

func (boundSessionGuard) Validate(string, replay.Scope) error { return nil }

// The guard the session read returns validates the run's statements.
func TestReplayGuard_ValidateSession_HappyPathKeepsTheBoundGuard(t *testing.T) {
	c := qt.New(t)
	bound := boundSessionGuard{name: "bound to the session"}
	guard := &ReplayGuard{guard: answeringSessionGuard{bound: bound}, trustSQL: true}

	err := guard.validateSession(c.Context(), nil)

	c.Assert(err, qt.IsNil)
	c.Assert(guard.guard, qt.Equals, replay.Guard(bound))
}

var errSessionUnreadable = errors.New("connection reset")

// A session the guard refuses ends with the trust remedy, because trusting
// the SQL lifts that refusal.
func TestReplayGuard_ValidateSession_FailurePathRefusalNamesTrust(t *testing.T) {
	c := qt.New(t)
	reader := answeringSessionGuard{
		err:   &replay.SessionRefusal{Reason: "the session reads latin1"},
		bound: boundSessionGuard{name: "bound under trust"},
	}
	guard := &ReplayGuard{guard: reader, trustRemedy: "trust remedy"}

	err := guard.validateSession(c.Context(), nil)

	c.Assert(err, qt.ErrorMatches, `the session reads latin1; trust remedy`)
	c.Assert(guard.guard, qt.Equals, replay.Guard(reader))
}

// A session the guard cannot read is reported as it failed, with no trust
// remedy: trusting the SQL does not mend a connection.
func TestReplayGuard_ValidateSession_FailurePathUnreadableSessionNamesNoTrust(t *testing.T) {
	c := qt.New(t)
	guard := &ReplayGuard{
		guard:       answeringSessionGuard{err: errSessionUnreadable},
		trustRemedy: "trust remedy",
	}

	err := guard.validateSession(c.Context(), nil)

	c.Assert(err, qt.ErrorIs, errSessionUnreadable)
	c.Assert(err, qt.ErrorMatches, `connection reset`)
}
